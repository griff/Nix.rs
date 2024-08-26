use std::collections::BTreeMap;
use std::fmt;

use async_trait::async_trait;
use futures::TryFutureExt;
use tokio::io::{copy, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tracing::{debug, instrument};

use super::process_stderr::ProcessStderr;
use crate::archive::copy_nar;
use crate::io::FramedSink;
use crate::io::{AsyncSink, AsyncSource};
use crate::path_info::ValidPathInfo;
use crate::store::activity::ActivityLogger;
use crate::store::daemon::{
    get_protocol_major, get_protocol_minor, DaemonStore, QueryMissingResult, TrustedFlag,
    WorkerProtoOp, PROTOCOL_VERSION, WORKER_MAGIC_1, WORKER_MAGIC_2,
};
use crate::store::error::Verbosity;
use crate::store::misc::add_multiple_to_store_old;
use crate::store::settings::get_settings;
use crate::store::{
    BasicDerivation, BuildMode, BuildResult, BuildStatus, CheckSignaturesFlag, DerivedPath, Error,
    RepairFlag, SPWOParseResult, Store, SubstituteFlag, EXPORT_MAGIC,
};
use crate::store_path::{StoreDir, StoreDirProvider, StorePath, StorePathSet};

macro_rules! with_framed_sink {
    ($store:expr, |$sink:ident| $handle:block) => {
        let daemon_version = $store.daemon_version.unwrap();
        let (cancel, cancelled) = tokio::sync::oneshot::channel();
        let process_fut = async {
            let ret = ProcessStderr::new($store.logger.clone(), daemon_version, &mut $store.source)
                .run()
                .await;
            if let Err(_) = ret {
                let _ = cancel.send(());
            }
            ret
        };
        let mut sink_ = FramedSink::new(&mut $store.sink);
        let copy_fut = async move {
            let $sink = &mut sink_;
            let copy = $handle;
            tokio::select! {
                res = copy => {
                    sink_.shutdown().await?;
                    res
                },
                _ = cancelled => {
                    sink_.shutdown().await?;
                    Ok(())
                }
            }
        };

        tokio::try_join!(process_fut, copy_fut)?;
    };
}

#[derive(Debug)]
pub struct DaemonStoreClient<R, W> {
    host: String,
    store_dir: StoreDir,
    source: R,
    sink: W,
    daemon_version: Option<u64>,
    daemon_nix_version: Option<String>,
    remote_trusts_us: Option<TrustedFlag>,
    logger: ActivityLogger,
}

impl<R, W> DaemonStoreClient<R, W>
where
    R: AsyncRead + fmt::Debug + Unpin + Send + 'static,
    W: AsyncWrite + fmt::Debug + Unpin + Send + 'static,
{
    pub fn new(store_dir: StoreDir, host: String, reader: R, writer: W) -> Self {
        let sink = writer;
        Self {
            store_dir,
            source: reader,
            sink,
            daemon_version: None,
            daemon_nix_version: None,
            remote_trusts_us: None,
            host,
            logger: ActivityLogger::new(),
        }
    }

    #[instrument(skip(store_dir, reader, writer))]
    pub async fn connect(
        store_dir: StoreDir,
        host: String,
        reader: R,
        writer: W,
    ) -> Result<Self, Error> {
        let mut store = Self::new(store_dir, host, reader, writer);
        store.init_connection().await?;
        Ok(store)
    }
    pub async fn daemon_version(&mut self) -> Result<u64, Error> {
        if self.daemon_version.is_none() {
            self.init_connection().await?;
        }
        Ok(*self.daemon_version.as_ref().unwrap())
    }
    pub async fn init_connection(&mut self) -> Result<(), Error> {
        if self.daemon_version.is_some() {
            return Ok(());
        }
        if let Err(err) = self.handshake().await {
            return Err(Error::OpenConnectionFailed(
                self.host.clone(),
                Box::new(err),
            ));
        }
        self.set_options().await?;
        Ok(())
    }

    #[instrument(skip(self))]
    async fn handshake(&mut self) -> Result<(), Error> {
        // Send the magic greeting, check for the reply.
        self.sink.write_u64_le(WORKER_MAGIC_1).await?;
        self.sink.flush().await?;

        let magic = self.source.read_u64_le().await?;
        if magic != WORKER_MAGIC_2 {
            return Err(Error::DaemonProtocolMismatch);
        }

        let daemon_version = self.source.read_u64_le().await?;
        self.daemon_version = Some(daemon_version);
        if get_protocol_major!(daemon_version) != get_protocol_major!(PROTOCOL_VERSION) {
            return Err(Error::UnsupportedDaemonProtocol);
        }
        if get_protocol_minor!(daemon_version) < 10 {
            return Err(Error::DaemonVersionTooOld);
        }
        self.sink.write_u64_le(PROTOCOL_VERSION).await?;

        if get_protocol_minor!(daemon_version) >= 14 {
            // Obsolete CPU affinity.
            self.sink.write_u64_le(0).await?;
        }

        if get_protocol_minor!(daemon_version) >= 11 {
            // obsolete reserveSpace
            self.sink.write_bool(false).await?;
        }

        if get_protocol_minor!(daemon_version) >= 33 {
            self.sink.flush().await?;
            let daemon_nix_version = self.source.read_string().await?;
            self.daemon_nix_version = Some(daemon_nix_version);
        }

        if get_protocol_minor!(daemon_version) >= 35 {
            let temp = self.source.read_u64_le().await?;
            self.remote_trusts_us = match temp {
                0 => None,
                1 => Some(TrustedFlag::Trusted),
                2 => Some(TrustedFlag::NotTrusted),
                _ => return Err(Error::InvalidTrustedStatus),
            };
        }

        self.process_stderr().await?;

        Ok(())
    }

    pub async fn close(&mut self) -> Result<(), Error> {
        self.sink.shutdown().await?;
        Ok(())
    }

    async fn process_stderr(&mut self) -> Result<(), Error> {
        self.sink.flush().await?;
        ProcessStderr::new(
            self.logger.clone(),
            self.daemon_version.unwrap(),
            &mut self.source,
        )
        .run()
        .await
    }

    async fn process_stderr_source<SR>(&mut self, source: SR) -> Result<(), Error>
    where
        SR: AsyncRead + Unpin,
    {
        self.sink.flush().await?;
        ProcessStderr::new(
            self.logger.clone(),
            self.daemon_version.unwrap(),
            &mut self.source,
        )
        .with_source(&mut self.sink, source)
        .run()
        .await
    }

    async fn write_derived_paths(&mut self, reqs: &[DerivedPath]) -> Result<(), Error> {
        let store_dir = self.store_dir();
        let daemon_version = self.daemon_version.unwrap();
        if get_protocol_minor!(daemon_version) >= 30 {
            self.sink.write_printed_coll(&store_dir, reqs).await?;
        } else {
            self.sink.write_usize(reqs.len()).await?;
            for p in reqs {
                match p.into() {
                    SPWOParseResult::StorePathWithOutputs(sp) => {
                        self.sink.write_printed(&store_dir, &sp).await?
                    }
                    SPWOParseResult::StorePath(path) => Err(Error::ProtocolTooOld(
                        store_dir.print_path(&path),
                        get_protocol_major!(daemon_version),
                        get_protocol_minor!(daemon_version),
                    ))?,
                    SPWOParseResult::Unsupported => Err(Error::DerivationIsBuildProduct)?,
                }
            }
        }
        Ok(())
    }
}

impl<R, W> StoreDirProvider for DaemonStoreClient<R, W> {
    fn store_dir(&self) -> StoreDir {
        self.store_dir.clone()
    }
}

#[async_trait]
impl<R, W> DaemonStore for DaemonStoreClient<R, W>
where
    R: AsyncRead + fmt::Debug + Unpin + Send + 'static,
    W: AsyncWrite + fmt::Debug + Unpin + Send + 'static,
{
    fn is_trusted_client(&self) -> Option<TrustedFlag> {
        self.remote_trusts_us
    }

    #[instrument(skip(self))]
    async fn set_options(&mut self) -> Result<(), Error> {
        let daemon_version = self.daemon_version().await?;

        let (
            keep_failed,
            keep_going,
            try_fallback,
            verbosity,
            max_build_jobs,
            max_silent_time,
            verbose_build,
            build_cores,
            use_substitutes,
        ) = get_settings(|s| {
            (
                s.keep_failed,
                s.keep_going,
                s.try_fallback,
                s.verbosity,
                s.max_build_jobs,
                s.max_silent_time,
                s.verbose_build,
                s.build_cores,
                s.use_substitutes,
            )
        });

        self.sink.write_enum(WorkerProtoOp::SetOptions).await?;
        self.sink.write_bool(keep_failed).await?;
        self.sink.write_bool(keep_going).await?;
        self.sink.write_bool(try_fallback).await?;
        self.sink.write_enum(verbosity).await?;
        self.sink.write_u64_le(max_build_jobs).await?;
        self.sink.write_seconds(max_silent_time).await?;
        self.sink.write_bool(true).await?;
        if verbose_build {
            self.sink.write_enum(Verbosity::Error).await?;
        } else {
            self.sink.write_enum(Verbosity::Vomit).await?;
        }
        self.sink.write_u64_le(0).await?; // obsolete log type
        self.sink.write_u64_le(0).await?; // obsolete print build trace
        self.sink.write_u64_le(build_cores).await?;
        self.sink.write_bool(use_substitutes).await?;

        if get_protocol_minor!(daemon_version) >= 12 {
            let mut overrides = BTreeMap::new();
            get_settings(|settings| {
                settings.get_all(&mut overrides);
            });
            overrides.remove("keep-failed");
            overrides.remove("keep-going");
            overrides.remove("fallback"); // try_fallback
            overrides.remove("max-jobs"); // max_build_jobs
            overrides.remove("max-silent-time");
            overrides.remove("cores"); // build_cores
            overrides.remove("substitute"); // use_substitutes
                                            /*
                                            overrides.erase(loggerSettings.showTrace.name);
                                            overrides.erase(experimentalFeatureSettings.experimentalFeatures.name);
                                            overrides.erase(settings.pluginFiles.name);
                                             */
            self.sink.write_usize(overrides.len()).await?;
            for (k, v) in overrides.iter() {
                self.sink.write_str(k).await?;
                self.sink.write_str(v).await?;
            }
        }
        self.process_stderr().await?;
        Ok(())
    }

    #[instrument(skip_all, fields(%path))]
    async fn is_valid_path(&mut self, path: &StorePath) -> Result<bool, Error> {
        let store_dir = self.store_dir.clone();
        self.init_connection().await?;
        self.sink.write_enum(WorkerProtoOp::IsValidPath).await?;
        self.sink.write_printed(&store_dir, path).await?;
        self.process_stderr().await?;
        Ok(self.source.read_bool().await?)
    }

    #[instrument(skip(self, source))]
    async fn add_multiple_to_store<SR: AsyncRead + fmt::Debug + Send + Unpin>(
        &mut self,
        mut source: SR,
        repair: RepairFlag,
        check_sigs: CheckSignaturesFlag,
    ) -> Result<(), Error> {
        let daemon_version = self.daemon_version().await?;
        debug!(
            daemon_version,
            daemon.major = get_protocol_major!(daemon_version),
            daemon.minor = get_protocol_minor!(daemon_version),
            "Daemon version {}.{}",
            get_protocol_major!(daemon_version),
            get_protocol_minor!(daemon_version)
        );
        if get_protocol_minor!(daemon_version) >= 32 {
            self.sink
                .write_enum(WorkerProtoOp::AddMultipleToStore)
                .await?;
            self.sink.write_flag(repair).await?;
            self.sink.write_flag(!check_sigs).await?;
            with_framed_sink!(self, |sink| {
                copy(&mut source, sink).map_ok(|_| ()).map_err(Error::from)
            });
            Ok(())
        } else {
            add_multiple_to_store_old(&mut self, source, repair, check_sigs).await
        }
    }

    #[instrument(skip_all)]
    async fn query_missing(
        &mut self,
        targets: &[DerivedPath],
    ) -> Result<QueryMissingResult, Error> {
        let daemon_version = self.daemon_version().await?;
        if get_protocol_minor!(daemon_version) < 19 {
            // TODO: Implement fallback
            return Err(Error::DaemonVersionTooOld);
        }
        let store_dir = self.store_dir.clone();
        self.sink.write_enum(WorkerProtoOp::QueryMissing).await?;
        self.write_derived_paths(targets).await?;
        self.process_stderr().await?;
        let will_build = self.source.read_parsed_coll(&store_dir).await?;
        let will_substitute = self.source.read_parsed_coll(&store_dir).await?;
        let unknown = self.source.read_parsed_coll(&store_dir).await?;
        let download_size = self.source.read_u64_le().await?;
        let nar_size = self.source.read_u64_le().await?;
        Ok(QueryMissingResult {
            will_build,
            will_substitute,
            unknown,
            download_size,
            nar_size,
        })
    }
}

#[async_trait]
impl<R, W> Store for DaemonStoreClient<R, W>
where
    R: AsyncRead + fmt::Debug + Unpin + Send + 'static,
    W: AsyncWrite + fmt::Debug + Unpin + Send + 'static,
{
    #[instrument(skip_all)]
    async fn query_valid_paths(
        &mut self,
        paths: &StorePathSet,
        _maybe_substitute: SubstituteFlag,
    ) -> Result<StorePathSet, Error> {
        let daemon_version = self.daemon_version().await?;
        debug!(
            daemon_version,
            daemon.major = get_protocol_major!(daemon_version),
            daemon.minor = get_protocol_minor!(daemon_version),
            "Daemon version {}.{}",
            get_protocol_major!(daemon_version),
            get_protocol_minor!(daemon_version)
        );
        if get_protocol_minor!(daemon_version) < 12 {
            let mut res = StorePathSet::new();
            for i in paths.iter() {
                if self.is_valid_path(i).await? {
                    res.insert(i.clone());
                }
            }
            Ok(res)
        } else {
            let store_dir = self.store_dir.clone();
            self.sink.write_enum(WorkerProtoOp::QueryValidPaths).await?;
            self.sink.write_printed_coll(&store_dir, paths).await?;
            if get_protocol_minor!(daemon_version) >= 27 {
                // conn->to << (settings.buildersUseSubstitutes ? 1 : 0);
                self.sink.write_bool(false).await?;
            }
            self.process_stderr().await?;
            let res = self.source.read_parsed_coll(&store_dir).await?;
            Ok(res)
        }
    }

    #[instrument(skip_all, fields(%path))]
    async fn query_path_info(&mut self, path: &StorePath) -> Result<Option<ValidPathInfo>, Error> {
        let store_dir = self.store_dir.clone();
        let daemon_version = self.daemon_version().await?;
        debug!(
            daemon_version,
            daemon.major = get_protocol_major!(daemon_version),
            daemon.minor = get_protocol_minor!(daemon_version),
            "Daemon version {}.{}",
            get_protocol_major!(daemon_version),
            get_protocol_minor!(daemon_version)
        );
        self.sink.write_enum(WorkerProtoOp::QueryPathInfo).await?;
        self.sink.write_printed(&store_dir, path).await?;
        if let Err(err) = self.process_stderr().await {
            // Ugly backwards compatibility hack.
            if err.to_string().contains("is not valid") {
                return Ok(None);
            } else {
                return Err(err);
            }
        }

        if get_protocol_minor!(daemon_version) >= 17 {
            let valid = self.source.read_bool().await?;
            if !valid {
                return Ok(None);
            }
        }

        let info = ValidPathInfo::read_path(
            &mut self.source,
            &store_dir,
            get_protocol_minor!(daemon_version),
            path.clone(),
        )
        .await?;
        Ok(Some(info))
    }

    #[instrument(skip_all, fields(%path))]
    async fn nar_from_path<SW>(&mut self, path: &StorePath, writer: SW) -> Result<(), Error>
    where
        SW: AsyncWrite + fmt::Debug + Send + Unpin,
    {
        let daemon_version = self.daemon_version().await?;
        debug!(
            daemon_version,
            daemon.major = get_protocol_major!(daemon_version),
            daemon.minor = get_protocol_minor!(daemon_version),
            "Daemon version {}.{}",
            get_protocol_major!(daemon_version),
            get_protocol_minor!(daemon_version)
        );
        debug!("Sending NAR for path {}", path);
        let store_dir = self.store_dir.clone();
        self.sink.write_enum(WorkerProtoOp::NarFromPath).await?;
        self.sink.write_printed(&store_dir, path).await?;
        self.process_stderr().await?;
        copy_nar(&mut self.source, writer).await?;
        debug!("Completed NAR for path {}", path);

        Ok(())
    }

    #[instrument(skip_all)]
    async fn add_to_store<SR: AsyncRead + fmt::Debug + Send + Unpin>(
        &mut self,
        info: &ValidPathInfo,
        source: SR,
        repair: RepairFlag,
        check_sigs: CheckSignaturesFlag,
    ) -> Result<(), Error> {
        let store_dir = self.store_dir.clone();
        debug!(
            "adding path '{}' to remote host '{}'",
            store_dir.print_path(&info.path),
            self.host
        );
        let daemon_version = self.daemon_version().await?;
        debug!(
            daemon_version,
            daemon.major = get_protocol_major!(daemon_version),
            daemon.minor = get_protocol_minor!(daemon_version),
            "Daemon version {}.{}",
            get_protocol_major!(daemon_version),
            get_protocol_minor!(daemon_version)
        );
        if get_protocol_minor!(daemon_version) < 18 {
            self.sink.write_enum(WorkerProtoOp::ImportPaths).await?;

            let (source2, mut sink) = tokio::io::duplex(65_000);
            let sink_to_source_fut = async {
                sink.write_u64_le(1).await?; // == path follows
                copy_nar(source, &mut sink).await?;
                sink.write_u64_le(EXPORT_MAGIC).await?;
                sink.write_printed(&store_dir, &info.path).await?;
                sink.write_printed_coll(&store_dir, &info.references)
                    .await?;
                if let Some(deriver) = info.deriver.as_ref() {
                    sink.write_printed(&store_dir, deriver).await?;
                } else {
                    sink.write_str("").await?;
                }
                sink.write_u64_le(0).await?; // == no legacy signature
                sink.write_u64_le(0).await?; // == no path follows
                Ok(())
            };
            let process_fut = self.process_stderr_source(source2);
            tokio::try_join!(process_fut, sink_to_source_fut)?;
            let imported_paths: StorePathSet = self.source.read_parsed_coll(&store_dir).await?;
            assert!(imported_paths.len() <= 1);
        } else {
            self.sink.write_enum(WorkerProtoOp::AddToStoreNar).await?;
            self.sink.write_printed(&store_dir, &info.path).await?;
            if let Some(deriver) = info.deriver.as_ref() {
                self.sink.write_printed(&store_dir, deriver).await?;
            } else {
                self.sink.write_str("").await?;
            }
            self.sink
                .write_string(info.nar_hash.encode_base16())
                .await?;
            self.sink
                .write_printed_coll(&store_dir, &info.references)
                .await?;
            self.sink.write_time(info.registration_time).await?;
            self.sink.write_u64_le(info.nar_size).await?;
            self.sink.write_bool(info.ultimate).await?;
            let sigs: Vec<String> = info.sigs.iter().map(ToString::to_string).collect();
            self.sink.write_string_coll(&sigs).await?;
            if let Some(ca) = info.ca.as_ref() {
                self.sink.write_str(&ca.to_string()).await?;
            } else {
                self.sink.write_str("").await?;
            }
            self.sink.write_flag(repair).await?;
            self.sink.write_flag(!check_sigs).await?;

            if get_protocol_minor!(daemon_version) >= 23 {
                with_framed_sink!(self, |sink| { copy_nar(source, sink).map_err(Error::from) });
            } else if get_protocol_minor!(daemon_version) >= 21 {
                self.process_stderr_source(source).await?;
            } else {
                copy_nar(source, &mut self.sink).await?;
                self.process_stderr().await?;
            }
        }
        Ok(())
    }

    #[instrument(skip_all, fields(%drv_path, build_mode))]
    async fn build_derivation(
        &mut self,
        drv_path: &StorePath,
        drv: &BasicDerivation,
        build_mode: BuildMode,
    ) -> Result<BuildResult, Error> {
        debug!("Build derivation {} with path {}", drv.name, drv_path);
        let store_dir = self.store_dir.clone();
        let daemon_version = self.daemon_version().await?;
        debug!(
            daemon_version,
            daemon.major = get_protocol_major!(daemon_version),
            daemon.minor = get_protocol_minor!(daemon_version),
            "Daemon version {}.{}",
            get_protocol_major!(daemon_version),
            get_protocol_minor!(daemon_version)
        );
        self.sink.write_enum(WorkerProtoOp::BuildDerivation).await?;
        self.sink.write_printed(&store_dir, drv_path).await?;
        drv.write_drv(&mut self.sink, &store_dir).await?;
        self.sink.write_enum(build_mode).await?;
        self.process_stderr().await?;
        let status: BuildStatus = self.source.read_enum().await?;
        let error_msg = self.source.read_string().await?;
        let mut status = BuildResult::new(status, error_msg);
        if get_protocol_minor!(daemon_version) >= 29 {
            status.times_built = self.source.read_u64_le().await?;
            status.is_non_deterministic = self.source.read_bool().await?;
            status.start_time = self.source.read_time().await?;
            status.stop_time = self.source.read_time().await?;
        }
        if get_protocol_minor!(daemon_version) >= 28 {
            let count = self.source.read_usize().await?;
            for _i in 0..count {
                let id = self.source.read_string().await?.parse()?;
                let realisation = self.source.read_string().await?.parse()?;
                status.built_outputs.insert(id, realisation);
            }
        }
        Ok(status)
    }

    #[instrument(skip(self, drv_paths))]
    async fn build_paths(
        &mut self,
        drv_paths: &[DerivedPath],
        build_mode: BuildMode,
    ) -> Result<(), Error> {
        debug!("Build paths {:?}", drv_paths);
        // copyDrvsFromEvalStore(drvPaths, evalStore);
        let daemon_version = self.daemon_version().await?;
        debug!(
            daemon_version,
            daemon.major = get_protocol_major!(daemon_version),
            daemon.minor = get_protocol_minor!(daemon_version),
            "Daemon version {}.{}",
            get_protocol_major!(daemon_version),
            get_protocol_minor!(daemon_version)
        );
        self.sink.write_enum(WorkerProtoOp::BuildPaths).await?;
        assert!(get_protocol_minor!(daemon_version) >= 13);
        self.write_derived_paths(drv_paths).await?;
        if get_protocol_minor!(daemon_version) >= 15 {
            self.sink.write_enum(build_mode).await?;
        } else {
            // Old daemons did not take a 'buildMode' parameter, so we
            // need to validate it here on the client side.  */
            if build_mode != BuildMode::Normal {
                return Err(Error::RepairingOrCheckingNotSupported);
            }
        }
        self.process_stderr().await?;
        self.source.read_u64_le().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::BTreeSet;
    use std::io::Cursor;
    use std::path::Path;
    use std::time::Duration;
    use std::time::Instant;
    use std::time::SystemTime;

    use ::proptest::arbitrary::any;
    use ::proptest::proptest;
    use bytes::BytesMut;
    use futures::future::try_join;

    use crate::archive::proptest::arb_nar_contents;
    use crate::archive::test_data::dir_example;
    use crate::hash;
    use crate::path_info::proptest::arb_valid_info_and_content;
    use crate::pretty_prop_assert_eq;
    use crate::signature::SignatureSet;
    use crate::store::assert_store::AssertStore;
    use crate::store::settings::BuildSettings;
    use crate::store::DerivationOutput;
    use crate::store::DrvOutput;
    use crate::store::Realisation;
    use crate::store_path::proptest::arb_drv_store_path;

    macro_rules! store_cmd {
        (
            $trusted:expr,
            $assert:ident($ae:expr$(,$ae2:expr)*$(,)?),
            $cmd:ident($ce:expr$(,$ce2:expr)*$(,)?),
            $res:expr
        ) => {{
            let r = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap();
            let store_dir = StoreDir::default();
            let (client, server) = tokio::io::duplex(1_000_000);
            let (read, write) = tokio::io::split(client);

            let mut test_store = DaemonStoreClient::new(store_dir.clone(), "localhost".into(), read, write);

            r.block_on(async {
                let mut store = AssertStore::$assert($ae $(, $ae2)*);
                let (read, write) = tokio::io::split(server);
                let server = Box::pin(crate::store::daemon::run_server(read, write, &mut store, $trusted));

                let cmd = async {
                    let res = test_store.$cmd($ce $(, $ce2)*).await?;
                    test_store.close().await?;
                    Ok(res)
                };
                let (res, _) = try_join(cmd, server).await?;
                store.assert_eq();
                ::pretty_assertions::assert_eq!(res, $res);
                Ok(()) as Result<(), Error>
            }).unwrap();
        }}
    }

    #[test]
    fn test_add_to_store() {
        let events = dir_example();
        let mut buf = BytesMut::new();
        let mut ctx = hash::Context::new(hash::Algorithm::SHA256);
        let mut nar_size = 0;
        for event in events {
            let encoded = event.encoded_size();
            nar_size += encoded as u64;
            buf.reserve(encoded);
            let mut temp = buf.split_off(buf.len());
            event.encode_into(&mut temp);
            ctx.update(&temp);
            buf.unsplit(temp);
        }
        let nar_hash = ctx.finish();
        let source = buf.freeze();
        let info = ValidPathInfo {
            path: StorePath::new_from_base_name("00000000000000000000000000000000-test").unwrap(),
            deriver: None,
            nar_size,
            nar_hash,
            references: StorePathSet::new(),
            sigs: SignatureSet::new(),
            registration_time: SystemTime::UNIX_EPOCH + Duration::from_secs(1697253889),
            ultimate: false,
            ca: None,
        };

        store_cmd!(
            TrustedFlag::Trusted,
            assert_add_to_store(
                Some(TrustedFlag::Trusted),
                &info,
                source.clone(),
                RepairFlag::NoRepair,
                CheckSignaturesFlag::NoCheckSigs,
                Ok(())
            ),
            add_to_store(
                &info,
                Cursor::new(source),
                RepairFlag::NoRepair,
                CheckSignaturesFlag::NoCheckSigs
            ),
            ()
        );
    }

    #[test]
    fn test_build_derivation() {
        let drv_path =
            StorePath::new_from_base_name("00000000000000000000000000000000-0.drv").unwrap();
        let output_path =
            StorePath::new_from_base_name("00000000000000000000000000000000-_").unwrap();
        let mut outputs = BTreeMap::new();
        outputs.insert(
            "=".to_string(),
            DerivationOutput::InputAddressed(output_path),
        );

        let drv = BasicDerivation {
            outputs,
            input_srcs: BTreeSet::new(),
            platform: "".into(),
            builder: Path::new("+/A").to_owned(),
            arguments: Vec::new(),
            env: Vec::new(),
            name: "0".into(),
        };
        let build_mode = BuildMode::Unknown(13);
        let drv_hash = hash::Hash::parse_any_prefixed(
            "sha256:0mdqa9w1p6cmli6976v4wi0sw9r4p5prkj7lzfd1877wk11c9c73",
        )
        .unwrap();
        let drv_output = DrvOutput {
            drv_hash,
            output_name: "+".into(),
        };
        let out_path = StorePath::new_from_base_name("00000000000000000000000000000000-+").unwrap();
        let realisation = Realisation {
            id: drv_output.clone(),
            out_path,
            signatures: BTreeSet::new(),
            dependent_realisations: BTreeMap::new(),
        };
        let mut built_outputs = BTreeMap::new();
        built_outputs.insert(drv_output, realisation);

        let result = BuildResult {
            status: BuildStatus::Unsupported(13),
            error_msg: "".into(),
            times_built: 0,
            is_non_deterministic: false,
            built_outputs,
            start_time: SystemTime::UNIX_EPOCH,
            stop_time: SystemTime::UNIX_EPOCH,
        };
        store_cmd!(
            TrustedFlag::Trusted,
            assert_build_derivation(
                Some(TrustedFlag::Trusted),
                &drv_path,
                &drv,
                build_mode,
                &BuildSettings::default(),
                Ok(result.clone())
            ),
            build_derivation(&drv_path, &drv, build_mode,),
            result
        );
    }

    macro_rules! prop_store_cmd {
        (
            $trusted:expr,
            $assert:ident($ae:expr$(,$ae2:expr)*$(,)?),
            $cmd:ident($ce:expr$(,$ce2:expr)*$(,)?),
            $res:expr
        ) => {{
            let r = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap();
            let store_dir = StoreDir::default();
            let (client, server) = tokio::io::duplex(1_000_000);
            let (read, write) = tokio::io::split(client);

            let mut test_store = DaemonStoreClient::new(store_dir.clone(), "localhost".into(), read, write);

            r.block_on(async {
                let mut store = AssertStore::$assert($ae $(, $ae2)*);
                let (read, write) = tokio::io::split(server);
                let server = Box::pin(crate::store::daemon::run_server(read, write, &mut store, $trusted));

                let cmd = async {
                    let res = test_store.$cmd($ce $(, $ce2)*).await?;
                    test_store.close().await?;
                    Ok(res)
                };
                let (res, _) = try_join(cmd, server).await?;
                store.prop_assert_eq()?;
                ::proptest::prop_assert_eq!(res, $res);
                Ok(())
            })?;
        }}
    }

    proptest! {
        #[test]
        fn proptest_store_query_valid_paths(
            trusted_flag in ::proptest::bool::ANY,
            paths in any::<StorePathSet>(),
            //maybe_substitute in ::proptest::bool::ANY,
            result in any::<StorePathSet>(),
        )
        {
            let trusted_flag : TrustedFlag = trusted_flag.into();
            prop_store_cmd!(
                trusted_flag,
                assert_query_valid_paths(Some(trusted_flag.into()), &paths, SubstituteFlag::NoSubstitute, Ok(result.clone())),
                query_valid_paths(&paths, SubstituteFlag::NoSubstitute),
                result
            );
        }
    }

    proptest! {
       #[test]
       fn proptest_store_query_path_info(
           trusted_flag in ::proptest::bool::ANY,
            (info, _source) in arb_valid_info_and_content(8, 256, 10),
        )
        {
           let trusted_flag : TrustedFlag = trusted_flag.into();
           prop_store_cmd!(
               trusted_flag,
                assert_query_path_info(Some(trusted_flag), &info.path.clone(), Ok(Some(info.clone()))),
                query_path_info(&info.path),
                Some(info)
            );
        }
    }

    proptest! {
        #[test]
        fn proptest_store_nar_from_path(
            trusted_flag in ::proptest::bool::ANY,
            (nar_size, nar_hash, contents) in arb_nar_contents(8, 256, 10),
            path in any::<StorePath>(),
        )
        {
            let mut buf = Vec::new();
            prop_store_cmd!(
                trusted_flag.into(),
                assert_nar_from_path(Some(trusted_flag.into()), &path, Ok(contents.clone())),
                nar_from_path(&path, Cursor::new(&mut buf)),
                ()
            );
            let nar_hash = nar_hash.try_into().unwrap();
            pretty_prop_assert_eq!(buf.len(), contents.len());
            pretty_prop_assert_eq!(buf.len() as u64, nar_size);
            pretty_prop_assert_eq!(hash::digest(hash::Algorithm::SHA256, &buf), nar_hash);
            pretty_prop_assert_eq!(buf, contents);
        }
    }

    proptest! {
        #[test]
        fn proptest_store_build_derivation(
            drv_path in arb_drv_store_path(),
            mut drv in any::<BasicDerivation>(),
            build_mode in any::<BuildMode>(),
            result in any::<BuildResult>(),
        )
        {
            let now = Instant::now();
            eprintln!("Run test {}", drv_path);
            drv.name = drv_path.name_from_drv().to_string();
            prop_store_cmd!(
                TrustedFlag::Trusted,
                assert_build_derivation(Some(TrustedFlag::Trusted), &drv_path, &drv, build_mode, &BuildSettings::default(), Ok(result.clone())),
                build_derivation(&drv_path, &drv, build_mode),
                result
            );
            eprintln!("Completed test {} in {}", drv_path, now.elapsed().as_secs_f64());
        }
    }

    proptest! {
        #[test]
        fn proptest_store_build_paths(
            drv_paths in any::<Vec<DerivedPath>>(),
            build_mode in any::<BuildMode>(),
        )
        {
            prop_store_cmd!(
                TrustedFlag::Trusted,
                assert_build_paths(Some(TrustedFlag::Trusted), &drv_paths, build_mode, &BuildSettings::default(), Ok(())),
                build_paths(&drv_paths, build_mode),
                ()
            );
        }
    }

    proptest! {
       #[test]
       fn proptest_store_add_to_store(
            (info, source) in arb_valid_info_and_content(8, 256, 10),
        )
        {
            prop_store_cmd!(
                TrustedFlag::Trusted,
                assert_add_to_store(Some(TrustedFlag::Trusted), &info, source.clone(), RepairFlag::NoRepair, CheckSignaturesFlag::NoCheckSigs, Ok(())),
                add_to_store(&info, Cursor::new(source), RepairFlag::NoRepair, CheckSignaturesFlag::NoCheckSigs),
                ()
            );
        }
    }
}
