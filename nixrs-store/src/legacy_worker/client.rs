use std::convert::TryInto;
use std::ffi::OsStr;
use std::path::Path;
use std::process::Stdio;
use std::time::SystemTime;

use async_trait::async_trait;
use futures::TryFutureExt;
use log::{debug, trace};
use nixrs_util::archive::copy_nar;
use nixrs_util::cancelled_reader::CancelledReader;
use nixrs_util::hash::Hash;
use nixrs_util::io::{AsyncSink, AsyncSource};
use nixrs_util::taken_reader::{TakenReader, Taker};
use tokio::io::{copy, stderr, AsyncRead, AsyncWrite};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::{ChildStderr, ChildStdin, ChildStdout, Command};
use tokio::spawn;

use crate::crypto::{ParseSignatureError, SignatureSet};
use crate::legacy_worker::{
    LegacyStore, ServeCommand, SERVE_MAGIC_1, SERVE_MAGIC_2, SERVE_PROTOCOL_VERSION,
};
use crate::store_api::{Store, StoreDirProvider, EXPORT_MAGIC};
use crate::Error;
use crate::StorePathWithOutputs;
use crate::ValidPathInfo;
use crate::{BasicDerivation, BuildResult, BuildStatus, CheckSignaturesFlag, DerivedPath};
use crate::{BuildSettings, ParseStorePathError};
use crate::{RepairFlag, StoreDir, StorePath, StorePathSet, SubstituteFlag};

#[macro_export]
macro_rules! get_protocol_major {
    ($x:expr) => {
        ($x) & 0xff00
    };
}

#[macro_export]
macro_rules! get_protocol_minor {
    ($x:expr) => {
        ($x) & 0x00ff
    };
}

pub struct LegacyStoreBuilder {
    cmd: Command,
    store_dir: StoreDir,
    host: String,
}

impl LegacyStoreBuilder {
    pub fn new<P: AsRef<OsStr>>(program: P) -> LegacyStoreBuilder {
        LegacyStoreBuilder {
            cmd: Command::new(program),
            store_dir: StoreDir::new("/nix/store").unwrap(),
            host: "localhost".into(),
        }
    }

    pub fn host<H: Into<String>>(&mut self, host: H) -> &mut Self {
        self.host = host.into();
        self
    }

    pub fn command_mut(&mut self) -> &mut Command {
        &mut self.cmd
    }

    pub fn store_dir<P: AsRef<Path>>(
        &mut self,
        store_dir: P,
    ) -> Result<&mut Self, ParseStorePathError> {
        self.store_dir = StoreDir::new(store_dir.as_ref())?;
        Ok(self)
    }

    pub async fn connect_with_log<W>(
        self,
        mut build_log: W,
    ) -> Result<LegacyStoreClient<ChildStdout, ChildStdin, ChildStderr>, Error>
    where
        W: AsyncWrite + Unpin + Send + 'static,
    {
        let mut cmd = self.cmd;
        cmd.stdin(Stdio::piped());
        cmd.stdout(Stdio::piped());
        cmd.stderr(Stdio::piped());
        let mut child = cmd.spawn()?;
        let reader = child.stdout.take().unwrap();
        let writer = child.stdin.take().unwrap();
        let stderr = child.stderr.take().unwrap();
        let mut taken_reader = TakenReader::new(stderr);
        let stderr = taken_reader.taker();
        spawn(async move { copy(&mut taken_reader, &mut build_log).await });

        let mut store = LegacyStoreClient::new(self.store_dir, self.host, reader, writer, stderr);
        store.handshake().await?;
        Ok(store)
    }

    pub async fn connect(
        self,
    ) -> Result<LegacyStoreClient<ChildStdout, ChildStdin, ChildStderr>, Error> {
        self.connect_with_log(stderr()).await
    }
}

pub struct LegacyStoreClient<R, W, BR> {
    host: String,
    store_dir: StoreDir,
    source: R,
    sink: W,
    remote_version: Option<u64>,
    build_log: Taker<BR>,
}

impl LegacyStoreClient<ChildStdout, ChildStdin, ChildStderr> {
    pub async fn connect(
        write_allowed: bool,
    ) -> Result<LegacyStoreClient<ChildStdout, ChildStdin, ChildStderr>, Error> {
        let mut b = LegacyStoreBuilder::new("nix-store");
        b.command_mut().arg("--serve");
        if write_allowed {
            b.command_mut().arg("--write");
        }
        b.connect().await
    }
}

impl<R, W, BR> LegacyStoreClient<R, W, BR>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    pub fn new(
        store_dir: StoreDir,
        host: String,
        reader: R,
        writer: W,
        build_log: Taker<BR>,
    ) -> LegacyStoreClient<R, W, BR> {
        let sink = writer;
        let remote_version = None;
        LegacyStoreClient {
            store_dir,
            source: reader,
            sink,
            remote_version,
            host,
            build_log,
        }
    }
    async fn remote_version(&mut self) -> Result<u64, Error> {
        if self.remote_version.is_none() {
            self.handshake().await?;
        }
        Ok(*self.remote_version.as_ref().unwrap())
    }
    async fn handshake(&mut self) -> Result<(), Error> {
        self.sink.write_u64_le(SERVE_MAGIC_1).await?;
        self.sink.write_u64_le(SERVE_PROTOCOL_VERSION).await?;
        self.sink.flush().await?;

        let magic = self.source.read_u64_le().await?;
        if magic != SERVE_MAGIC_2 {
            return Err(Error::LegacyProtocolMismatch(self.host.clone()));
        }
        let remote_version = self.source.read_u64_le().await?;
        self.remote_version = Some(remote_version);
        if get_protocol_major!(remote_version) != 0x200 {
            return Err(Error::UnsupportedLegacyProtocol(self.host.clone()));
        }
        Ok(())
    }

    pub async fn close(&mut self) -> Result<(), Error> {
        self.sink.shutdown().await?;
        Ok(())
    }
}

impl<R, W, BR> StoreDirProvider for LegacyStoreClient<R, W, BR> {
    fn store_dir(&self) -> StoreDir {
        self.store_dir.clone()
    }
}

#[async_trait]
impl<R, W, BR> LegacyStore for LegacyStoreClient<R, W, BR>
where
    R: AsyncRead + Send + Unpin + 'static,
    W: AsyncWrite + Send + Unpin + 'static,
    BR: AsyncRead + Send + Unpin + 'static,
{
    /*
    async fn query_path_infos(
        &mut self,
        paths: &StorePathSet
    ) -> Result<BTreeSet<ValidPathInfo>, Error> {
        let remote_version = self.remote_version().await?;
        /* No longer support missing NAR hash */
        assert!(get_protocol_minor!(remote_version) >= 4);

        let mut ret = BTreeSet::new();
        for path in paths.iter() {
            let store_dir = self.store_dir.clone();
            debug!(
                "querying remote host '{}' for info on '{}'",
                self.host,
                store_dir.print_path(path)
            );

            self.sink
                .write_enum(ServeCommand::CmdQueryPathInfos)
                .await?;
            self.sink.write_u64_le(1).await?;
            self.sink.write_printed(&store_dir, path).await?;
            self.sink.flush().await?;

            let p = self.source.read_string().await?;
            if p != "" {
                let path2 = store_dir.parse_path(&p)?;
                assert_eq!(path, &path2);

                let deriver = self.source.read_string().await?;
                let deriver = if deriver != "" {
                    Some(store_dir.parse_path(&deriver)?)
                } else {
                    None
                };
                let references = self.source.read_parsed_coll(&store_dir).await?;
                self.source.read_u64_le().await?; // download size
                let nar_size = self.source.read_u64_le().await?;

                let s = self.source.read_string().await?;
                if s == "" {
                    return Err(Error::MandatoryNARHash);
                }
                let nar_hash: Hash = s.parse()?;
                let ca_s = self.source.read_string().await?;
                let ca = if ca_s != "" {
                    Some(ca_s.parse()?)
                } else {
                    None
                };
                let sigs : Vec<String> = self.source.read_string_coll().await?;
                let sigs = sigs.iter().map(|s| s.parse() ).collect::<Result<SignatureSet, ParseSignatureError>>()?;

                let s = self.source.read_string().await?;
                assert_eq!(s, "");
                ret.insert(ValidPathInfo {
                    path: path2,
                    deriver,
                    references,
                    nar_size,
                    nar_hash,
                    ca,
                    sigs,
                    ultimate: false,
                    registration_time: SystemTime::UNIX_EPOCH,
                });
            }
        }
        Ok(ret)
    }
    */

    async fn query_valid_paths_locked(
        &mut self,
        paths: &StorePathSet,
        lock: bool,
        maybe_substitute: SubstituteFlag,
    ) -> Result<StorePathSet, Error> {
        debug!(
            "Query for valid paths lock={}, substitute={:?}, {:?}",
            lock, maybe_substitute, paths
        );
        let _remote_version = self.remote_version().await?;
        self.sink
            .write_enum(ServeCommand::CmdQueryValidPaths)
            .await?;
        self.sink.write_bool(lock).await?;
        self.sink.write_flag(maybe_substitute).await?;
        let store_dir = self.store_dir.clone();
        self.sink.write_printed_coll(&store_dir, paths).await?;
        self.sink.flush().await?;
        let paths = self.source.read_parsed_coll(&store_dir).await?;
        Ok(paths)
    }

    async fn export_paths<SW: AsyncWrite + Send + Unpin>(
        &mut self,
        paths: &StorePathSet,
        mut sink: SW,
    ) -> Result<(), Error> {
        debug!("Exporting: {:?}", paths);
        let _remote_version = self.remote_version().await?;
        let store_dir = self.store_dir.clone();
        self.sink.write_enum(ServeCommand::CmdExportPaths).await?;
        self.sink.write_printed_coll(&store_dir, paths).await?;
        self.sink.flush().await?;

        let mut next = self.source.read_u64_le().await?;
        while next != 0 {
            sink.write_u64_le(1).await?;

            copy_nar(&mut self.source, &mut sink).await?;
            let magic = self.source.read_u64_le().await?;
            sink.write_u64_le(magic).await?;
            let path: StorePath = self.source.read_parsed(&store_dir).await?;
            sink.write_printed(&store_dir, &path).await?;
            let paths: StorePathSet = self.source.read_parsed_coll(&store_dir).await?;
            sink.write_printed_coll(&store_dir, &paths).await?;
            let deriver = self.source.read_string().await?;
            sink.write_str(&deriver).await?;
            let end = self.source.read_u64_le().await?;
            if end != 0 {
                break;
            }
            sink.write_u64_le(0).await?;

            next = self.source.read_u64_le().await?;
        }
        sink.write_u64_le(0).await?;

        Ok(())
    }

    async fn import_paths<SR: AsyncRead + Send + Unpin>(
        &mut self,
        mut source: SR,
    ) -> Result<(), Error> {
        debug!("Importing paths");
        let _remote_version = self.remote_version().await?;
        let store_dir = self.store_dir.clone();
        self.sink.write_enum(ServeCommand::CmdImportPaths).await?;
        self.sink.flush().await?;

        let mut next = source.read_u64_le().await?;
        while next != 0 {
            self.sink.write_u64_le(1).await?;

            copy_nar(&mut source, &mut self.sink).await?;
            let magic = source.read_u64_le().await?;
            self.sink.write_u64_le(magic).await?;
            let path: StorePath = source.read_parsed(&store_dir).await?;
            self.sink.write_printed(&store_dir, &path).await?;
            let paths: StorePathSet = source.read_parsed_coll(&store_dir).await?;
            self.sink.write_printed_coll(&&store_dir, &paths).await?;
            let deriver = source.read_string().await?;
            self.sink.write_str(&deriver).await?;
            let end = source.read_u64_le().await?;
            if end != 0 {
                break;
            }
            self.sink.write_u64_le(0).await?;

            next = source.read_u64_le().await?;
        }
        self.sink.write_u64_le(0).await?;

        Ok(())
    }

    async fn query_closure(
        &mut self,
        paths: &StorePathSet,
        include_outputs: bool,
    ) -> Result<StorePathSet, Error> {
        debug!(
            "Query the closure include_outputs={} of {:?}",
            include_outputs, paths
        );
        let _remote_version = self.remote_version().await?;
        let store_dir = self.store_dir.clone();
        self.sink.write_enum(ServeCommand::CmdQueryClosure).await?;
        self.sink.write_bool(include_outputs).await?;
        self.sink.write_printed_coll(&store_dir, paths).await?;
        self.sink.flush().await?;
        Ok(self.source.read_parsed_coll(&store_dir).await?)
    }
}

#[async_trait]
impl<R, W, BR> Store for LegacyStoreClient<R, W, BR>
where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
    BR: AsyncRead + Unpin + Send + 'static,
{
    async fn query_valid_paths(
        &mut self,
        paths: &crate::StorePathSet,
        maybe_substitute: crate::SubstituteFlag,
    ) -> Result<crate::StorePathSet, crate::Error> {
        self.query_valid_paths_locked(paths, false, maybe_substitute)
            .await
    }

    async fn query_path_info(&mut self, path: &StorePath) -> Result<Option<ValidPathInfo>, Error> {
        let remote_version = self.remote_version().await?;
        /* No longer support missing NAR hash */
        assert!(get_protocol_minor!(remote_version) >= 4);

        let store_dir = self.store_dir.clone();
        debug!(
            "querying remote host '{}' for info on '{}'",
            self.host,
            store_dir.print_path(path)
        );

        self.sink
            .write_enum(ServeCommand::CmdQueryPathInfos)
            .await?;
        self.sink.write_u64_le(1).await?;
        self.sink.write_printed(&store_dir, path).await?;
        self.sink.flush().await?;

        let p = self.source.read_string().await?;
        if p == "" {
            return Ok(None);
        }
        let path2 = store_dir.parse_path(&p)?;
        assert_eq!(path, &path2);

        let deriver = self.source.read_string().await?;
        let deriver = if deriver != "" {
            Some(store_dir.parse_path(&deriver)?)
        } else {
            None
        };
        let references = self.source.read_parsed_coll(&store_dir).await?;
        self.source.read_u64_le().await?; // download size
        let nar_size = self.source.read_u64_le().await?;

        let s = self.source.read_string().await?;
        if s == "" {
            return Err(Error::MandatoryNARHash);
        }
        let nar_hash: Hash = s.parse()?;
        let ca_s = self.source.read_string().await?;
        let ca = if ca_s != "" {
            Some(ca_s.parse()?)
        } else {
            None
        };
        let sigs: Vec<String> = self.source.read_string_coll().await?;
        let sigs = sigs
            .iter()
            .map(|s| s.parse())
            .collect::<Result<SignatureSet, ParseSignatureError>>()?;

        let s = self.source.read_string().await?;
        assert_eq!(s, "");
        Ok(Some(ValidPathInfo {
            path: path2,
            deriver,
            references,
            nar_size,
            nar_hash,
            ca,
            sigs,
            ultimate: false,
            registration_time: SystemTime::UNIX_EPOCH,
        }))
    }

    async fn nar_from_path<SW>(&mut self, path: &StorePath, writer: SW) -> Result<(), Error>
    where
        SW: AsyncWrite + Send + Unpin,
    {
        debug!("Sending NAR for path {}", path);
        let _remote_version = self.remote_version().await?;
        let store_dir = self.store_dir.clone();
        self.sink.write_enum(ServeCommand::CmdDumpStorePath).await?;
        self.sink.write_printed(&store_dir, path).await?;
        self.sink.flush().await?;

        debug!("Write Command for {}", path);

        copy_nar(&mut self.source, writer).await?;

        debug!("Completed NAR for path {}", path);

        Ok(())
    }

    async fn add_to_store<SR: AsyncRead + Send + Unpin>(
        &mut self,
        info: &ValidPathInfo,
        source: SR,
        _repair: RepairFlag,
        _check_sigs: CheckSignaturesFlag,
    ) -> Result<(), Error> {
        let remote_version = self.remote_version().await?;
        let store_dir = self.store_dir.clone();
        debug!(
            "adding path '{}' to remote host '{}'",
            store_dir.print_path(&info.path),
            self.host
        );
        if get_protocol_minor!(remote_version) >= 5 {
            self.sink.write_enum(ServeCommand::CmdAddToStoreNar).await?;
            self.sink.write_printed(&store_dir, &info.path).await?;
            if let Some(deriver) = info.deriver.as_ref() {
                self.sink.write_printed(&store_dir, deriver).await?;
            } else {
                self.sink.write_str("").await?;
            }
            self.sink
                .write_str(&format!("{:#x}", info.nar_hash))
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

            // TODO: Handle exceptions
            //try {
            copy_nar(source, &mut self.sink).await?;
            //} catch (...) {
            //    conn->good = false;
            //    throw;
            //}

            self.sink.flush().await?;
        } else {
            self.sink.write_enum(ServeCommand::CmdImportPaths).await?;
            self.sink.write_u64_le(1).await?;

            // TODO: Handle exceptions
            //try {
            copy_nar(source, &mut self.sink).await?;
            //} catch (...) {
            //    conn->good = false;
            //    throw;
            //}
            self.sink.write_u64_le(EXPORT_MAGIC).await?;
            self.sink.write_printed(&store_dir, &info.path).await?;
            self.sink
                .write_printed_coll(&store_dir, &info.references)
                .await?;
            if let Some(deriver) = info.deriver.as_ref() {
                self.sink.write_printed(&store_dir, deriver).await?;
            } else {
                self.sink.write_str("").await?;
            }
            self.sink.write_u64_le(0).await?;
            self.sink.write_u64_le(0).await?;
            self.sink.flush().await?;
        }

        let success = self.source.read_u64_le().await?;
        if success != 1 {
            return Err(Error::FailedToAddToStore(
                store_dir.print_path(&info.path),
                self.host.clone(),
            ));
        }

        Ok(())
    }

    async fn build_derivation<BW: AsyncWrite + Send + Unpin>(
        &mut self,
        drv_path: &StorePath,
        drv: &BasicDerivation,
        settings: &BuildSettings,
        mut build_log: BW,
    ) -> Result<BuildResult, Error> {
        debug!("Build derivation {} with path {}", drv.name, drv_path);
        let taker = self.build_log.clone();
        let reader = taker.take();

        let remote_version = self.remote_version().await?;
        let store_dir = self.store_dir.clone();
        self.sink
            .write_enum(ServeCommand::CmdBuildDerivation)
            .await?;

        trace!("Write drv_path {}", drv_path);
        self.sink.write_printed(&store_dir, drv_path).await?;
        drv.write_drv(&mut self.sink, &store_dir).await?;

        self.sink.write_seconds(settings.max_silent_time).await?;
        self.sink.write_seconds(settings.build_timeout).await?;
        if get_protocol_minor!(remote_version) >= 2 {
            self.sink.write_u64_le(settings.max_log_size).await?;
        }
        if get_protocol_minor!(remote_version) >= 3 {
            self.sink.write_u64_le(settings.build_repeat).await?;
            self.sink.write_bool(settings.enforce_determinism).await?;
        }

        self.sink.flush().await?;

        let (mut creader, build_log_token) = CancelledReader::new(reader);
        let copy_fut = copy(&mut creader, &mut build_log).map_err(Error::from);
        let status_fut = async {
            let status: BuildStatus = self.source.read_enum().await?;
            build_log_token.cancel();
            Ok(status)
        };
        let status = match tokio::try_join!(status_fut, copy_fut) {
            Ok((status, _)) => status,
            Err(err) => return Err(err),
        };

        let error_msg = self.source.read_string().await?;
        let mut status = BuildResult::new(status, error_msg);
        if get_protocol_minor!(remote_version) >= 3 {
            status.times_built = self.source.read_u64_le().await?;
            status.is_non_deterministic = self.source.read_bool().await?;
            status.start_time = self.source.read_time().await?;
            status.stop_time = self.source.read_time().await?;
        }
        if get_protocol_minor!(remote_version) >= 6 {
            let count = self.source.read_usize().await?;
            for _i in 0..count {
                let id = self.source.read_string().await?.parse()?;
                let realisation = self.source.read_string().await?.parse()?;
                status.built_outputs.insert(id, realisation);
            }
        }
        Ok(status)
    }

    async fn build_paths<BW: AsyncWrite + Send + Unpin>(
        &mut self,
        drv_paths: &[DerivedPath],
        settings: &BuildSettings,
        mut build_log: BW,
    ) -> Result<(), Error> {
        debug!("Build paths {:?}", drv_paths);

        let taker = self.build_log.clone();
        let reader = taker.take();

        let remote_version = self.remote_version().await?;
        let store_dir = self.store_dir.clone();
        self.sink.write_enum(ServeCommand::CmdBuildPaths).await?;
        self.sink.write_usize(drv_paths.len()).await?;
        for p in drv_paths {
            let res: Result<StorePathWithOutputs, StorePath> = p.try_into();
            match res {
                Ok(sp) => self.sink.write_printed(&store_dir, &sp).await?,
                Err(path) => Err(Error::WantedFetchInLegacy(store_dir.print_path(&path)))?,
            }
        }

        self.sink.write_seconds(settings.max_silent_time).await?;
        self.sink.write_seconds(settings.build_timeout).await?;
        if get_protocol_minor!(remote_version) >= 2 {
            self.sink.write_u64_le(settings.max_log_size).await?;
        }
        if get_protocol_minor!(remote_version) >= 3 {
            self.sink.write_u64_le(settings.build_repeat).await?;
            self.sink.write_bool(settings.enforce_determinism).await?;
        }

        self.sink.flush().await?;

        let (mut creader, build_log_token) = CancelledReader::new(reader);
        let copy_fut = copy(&mut creader, &mut build_log).map_err(Error::from);
        let result_fut = async {
            let status: BuildStatus = self.source.read_enum().await?;
            build_log_token.cancel();
            if status.success() {
                Ok(())
            } else {
                let error_msg = self.source.read_string().await?;
                Err(Error::Custom(status.into(), error_msg))
            }
        };
        match tokio::try_join!(result_fut, copy_fut) {
            Ok(_) => {
                eprintln!("Both completed");
                Ok(())
            }
            Err(err) => Err(err),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::io::Cursor;
    #[cfg(feature = "slowtests")]
    use std::time::Instant;

    use ::proptest::arbitrary::any;
    use ::proptest::proptest;
    use futures::future::try_join;
    use nixrs_util::archive::proptest::arb_nar_contents;
    use nixrs_util::hash;
    use nixrs_util::pretty_prop_assert_eq;

    use crate::assert_store::AssertStore;
    #[cfg(feature = "slowtests")]
    use crate::path::proptest::arb_drv_store_path;
    use crate::path_info::proptest::arb_valid_info_and_content;

    macro_rules! store_cmd {
        (
            $assert:ident($ae:expr$(,$ae2:expr)*$(,)?),
            $cmd:ident($ce:expr$(,$ce2:expr)*$(,)?),
            $res:expr
        ) => {{
            let r = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap();
            let store_dir = StoreDir::new("/nix/store").unwrap();
            let (client, server) = tokio::io::duplex(1_000_000);
            let (read, write) = tokio::io::split(client);
            let (build_log_client, build_log_server) = tokio::io::duplex(1_000_000);
            let mut taken_reader = TakenReader::new(build_log_client);
            let stdreader = taken_reader.taker();
            r.spawn(async move {
                copy(&mut taken_reader, &mut stderr()).await
            });

            let mut test_store = LegacyStoreClient::new(store_dir.clone(), "localhost".into(), read, write, stdreader);

            r.block_on(async {
                let store = AssertStore::$assert($ae $(, $ae2)*);
                let (read, write) = tokio::io::split(server);
                let server = crate::legacy_worker::server::run(read, write, store, build_log_server, true);

                let cmd = async {
                    let res = test_store.$cmd($ce $(, $ce2)*).await?;
                    test_store.close().await?;
                    Ok(res)
                };
                let (res, _) = try_join(cmd, server).await?;
                pretty_prop_assert_eq!(res, $res);
                Ok(())
            })?;
        }}
    }

    proptest! {
        #[test]
        fn test_store_query_valid_paths_locked(
            paths in any::<StorePathSet>(),
            lock in ::proptest::bool::ANY,
            maybe_substitute in ::proptest::bool::ANY,
            result in any::<StorePathSet>(),
        )
        {
            store_cmd!(
                assert_query_valid_paths_locked(&paths, lock, maybe_substitute.into(), Ok(result.clone())),
                query_valid_paths_locked(&paths, lock, maybe_substitute.into()),
                result
            );
        }
    }

    proptest! {
        #[test]
        fn test_store_nar_from_path(
            (nar_size, nar_hash, contents) in arb_nar_contents(8, 256, 10),
            path in any::<StorePath>(),
        )
        {
            let mut buf = Vec::new();
            store_cmd!(
                assert_nar_from_path(&path, Ok(contents.clone())),
                nar_from_path(&path, Cursor::new(&mut buf)),
                ()
            );
            pretty_prop_assert_eq!(buf.len(), contents.len());
            pretty_prop_assert_eq!(buf.len() as u64, nar_size);
            pretty_prop_assert_eq!(hash::digest(hash::Algorithm::SHA256, &buf), nar_hash);
            pretty_prop_assert_eq!(buf, contents);
        }
    }

    proptest! {
        #[test]
        #[cfg(feature="slowtests")]
        fn test_store_build_derivation(
            drv_path in arb_drv_store_path(),
            mut drv in any::<BasicDerivation>(),
            settings in any::<BuildSettings>(),
            result in any::<BuildResult>(),
            build_log in any::<Vec<u8>>()
        )
        {
            let now = Instant::now();
            let build_log = bytes::Bytes::from(build_log);
            let mut buf = Vec::new();
            eprintln!("Run test {}", drv_path);
            drv.name = drv_path.name_from_drv();
            store_cmd!(
                assert_build_derivation(&drv_path, &drv, &settings, Ok((result.clone(), build_log.clone()))),
                build_derivation(&drv_path, &drv, &settings, Cursor::new(&mut buf)),
                result
            );
            eprintln!("Completed test {} in {}", drv_path, now.elapsed().as_secs_f64());
        }
    }

    proptest! {
        #[test]
        fn test_store_build_paths(
            drv_paths in any::<Vec<DerivedPath>>(),
            settings in any::<BuildSettings>(),
            build_log in any::<Vec<u8>>()
        )
        {
            let build_log = bytes::Bytes::from(build_log);
            let mut buf = Vec::new();
            store_cmd!(
                assert_build_paths(&drv_paths, &settings, Ok(build_log.clone())),
                build_paths(&drv_paths, &settings, Cursor::new(&mut buf)),
                ()
            );
            pretty_prop_assert_eq!(buf.len(), build_log.len(), "Build log length");
            pretty_prop_assert_eq!(buf, build_log, "Build log");
        }
    }

    proptest! {
       #[test]
       fn test_store_add_to_store(
            (info, source) in arb_valid_info_and_content(8, 256, 10),
        )
        {
            store_cmd!(
                assert_add_to_store(&info, source.clone(), RepairFlag::NoRepair, CheckSignaturesFlag::NoCheckSigs, Ok(())),
                add_to_store(&info, Cursor::new(source), RepairFlag::Repair, CheckSignaturesFlag::CheckSigs),
                ()
            );
        }
    }

    proptest! {
        #[test]
        fn test_store_query_closure(
            paths in any::<StorePathSet>(),
            include_outputs in ::proptest::bool::ANY,
            result in any::<StorePathSet>(),
        )
        {
            store_cmd!(
                assert_query_closure(&paths, include_outputs, Ok(result.clone())),
                query_closure(&paths, include_outputs),
                result
            );
        }
    }
}
