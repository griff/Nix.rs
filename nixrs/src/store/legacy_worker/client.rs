use std::ffi::OsStr;
use std::path::Path;
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::time::SystemTime;
use std::{fmt, io};

use async_trait::async_trait;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, BufReader};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::{ChildStdin, ChildStdout, Command};
use tokio::spawn;
use tracing::dispatcher::{get_default, with_default};
use tracing::Instrument;
use tracing::{debug, instrument, trace};
use tracing::{error, Dispatch};

use super::{
    get_protocol_major, get_protocol_minor, LegacyStore, ServeCommand, SERVE_MAGIC_1,
    SERVE_MAGIC_2, SERVE_PROTOCOL_VERSION,
};
use crate::activity;
use crate::archive::copy_nar;
use crate::hash::Hash;
use crate::io::{AsyncSink, AsyncSource};
use crate::path_info::ValidPathInfo;
use crate::signature::{ParseSignatureError, SignatureSet};
use crate::store::activity::{ActivityType, ResultType, RESULT_TARGET};
use crate::store::error::Verbosity;
use crate::store::settings::get_settings;
use crate::store::{
    BasicDerivation, BuildMode, BuildResult, BuildStatus, CheckSignaturesFlag, DerivedPath, Error,
    RepairFlag, SPWOParseResult, Store, SubstituteFlag, EXPORT_MAGIC,
};
use crate::store_path::{ParseStorePathError, StoreDir, StoreDirProvider, StorePath, StorePathSet};

#[derive(Debug, Clone)]
struct DispatchInner {
    dispatch: Dispatch,
    span: Option<tracing::span::Id>,
}

#[derive(Debug, Clone)]
pub struct LogDispatch(Arc<Mutex<DispatchInner>>);

impl LogDispatch {
    pub fn new() -> LogDispatch {
        let (dispatch, current) = get_default(|d| (d.clone(), d.current_span()));
        let inner = DispatchInner {
            dispatch,
            span: current.id().cloned(),
        };
        LogDispatch(Arc::new(Mutex::new(inner)))
    }
    fn replace(&self) -> ReplaceDispatch {
        let (dispatch, current) = get_default(|d| (d.clone(), d.current_span()));
        let logger = self.clone();
        let mut inner = self.0.lock().unwrap();
        let old = inner.clone();
        inner.dispatch = dispatch;
        inner.span = current.id().cloned();
        ReplaceDispatch { old, logger }
    }
}

struct ReplaceDispatch {
    logger: LogDispatch,
    old: DispatchInner,
}

impl ReplaceDispatch {}

impl Drop for ReplaceDispatch {
    fn drop(&mut self) {
        let mut inner = self.logger.0.lock().unwrap();
        *inner = self.old.clone();
    }
}

async fn dump_log<R: AsyncRead + Unpin>(build_log: R, dispatcher: LogDispatch) {
    let mut reader = BufReader::new(build_log);
    let mut message = String::new();
    while let Ok(read) = reader.read_line(&mut message).await {
        if read == 0 {
            break;
        }
        let d = dispatcher.0.lock().unwrap();
        with_default(&d.dispatch, || {
            eprintln!("Build log: {}", message);
            let result_type: u64 = ResultType::BuildLogLine.into();
            error!(target: RESULT_TARGET, parent: d.span.clone(), result_type, message);
        });
        message.clear()
    }
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
            store_dir: StoreDir::default(),
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

    pub async fn connect(self) -> Result<LegacyStoreClient<ChildStdout, ChildStdin>, Error> {
        let mut cmd = self.cmd;
        cmd.stdin(Stdio::piped());
        cmd.stdout(Stdio::piped());
        cmd.stderr(Stdio::piped());
        let mut child = cmd.spawn()?;
        let reader = child.stdout.take().unwrap();
        let writer = child.stdin.take().unwrap();
        let stderr = child.stderr.take().unwrap();
        let mut store =
            LegacyStoreClient::new(self.store_dir, self.host, reader, writer, stderr).await;
        store.handshake().await?;
        Ok(store)
    }
}

#[derive(Debug)]
pub struct LegacyStoreClient<R, W> {
    host: String,
    store_dir: StoreDir,
    source: R,
    sink: W,
    remote_version: Option<u64>,
    log_dispatch: LogDispatch,
}

impl LegacyStoreClient<ChildStdout, ChildStdin> {
    pub async fn connect(
        write_allowed: bool,
    ) -> Result<LegacyStoreClient<ChildStdout, ChildStdin>, Error> {
        let mut b = LegacyStoreBuilder::new("nix-store");
        b.command_mut().arg("--serve");
        if write_allowed {
            b.command_mut().arg("--write");
        }
        b.connect().await
    }
}

impl<R, W> LegacyStoreClient<R, W>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    pub async fn new<BR>(
        store_dir: StoreDir,
        host: String,
        reader: R,
        writer: W,
        build_log: BR,
    ) -> LegacyStoreClient<R, W>
    where
        BR: AsyncRead + Send + Unpin + 'static,
    {
        let log_dispatch = LogDispatch::new();
        let dispatcher = log_dispatch.clone();
        spawn(async move { dump_log(build_log, dispatcher).await });

        let sink = writer;
        let remote_version = None;
        LegacyStoreClient {
            store_dir,
            source: reader,
            sink,
            remote_version,
            host,
            log_dispatch,
        }
    }
    pub async fn remote_version(&mut self) -> Result<u64, Error> {
        if self.remote_version.is_none() {
            self.handshake().await?;
        }
        Ok(*self.remote_version.as_ref().unwrap())
    }
    pub async fn handshake(&mut self) -> Result<(), Error> {
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

    async fn write_build_settings(&mut self) -> io::Result<()> {
        let remote_version = self.remote_version.unwrap();

        let (max_silent_time, build_timeout, max_log_size, keep_failed) = get_settings(|s| {
            (
                s.max_silent_time,
                s.build_timeout,
                s.max_log_size,
                s.keep_failed,
            )
        });

        self.sink.write_seconds(max_silent_time).await?;
        self.sink.write_seconds(build_timeout).await?;
        if get_protocol_minor!(remote_version) >= 2 {
            self.sink.write_u64_le(max_log_size).await?;
        }
        if get_protocol_minor!(remote_version) >= 3 {
            self.sink.write_u64_le(0).await?; // buildRepeat hasn't worked for ages anyway
            self.sink.write_bool(false).await?;
        }
        if get_protocol_minor!(remote_version) >= 7 {
            self.sink.write_bool(keep_failed).await?;
        }
        Ok(())
    }
}

impl<R, W> StoreDirProvider for LegacyStoreClient<R, W> {
    fn store_dir(&self) -> StoreDir {
        self.store_dir.clone()
    }
}

#[async_trait]
impl<R, W> LegacyStore for LegacyStoreClient<R, W>
where
    R: AsyncRead + fmt::Debug + Send + Unpin + 'static,
    W: AsyncWrite + fmt::Debug + Send + Unpin + 'static,
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

    #[instrument(skip_all)]
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

    #[instrument(skip_all)]
    async fn import_paths<SR: AsyncRead + fmt::Debug + Send + Unpin>(
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

    #[instrument(skip_all)]
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
impl<R, W> Store for LegacyStoreClient<R, W>
where
    R: AsyncRead + fmt::Debug + Unpin + Send + 'static,
    W: AsyncWrite + fmt::Debug + Unpin + Send + 'static,
{
    #[instrument(skip_all)]
    async fn query_valid_paths(
        &mut self,
        paths: &StorePathSet,
        maybe_substitute: SubstituteFlag,
    ) -> Result<StorePathSet, Error> {
        self.query_valid_paths_locked(paths, false, maybe_substitute)
            .await
    }

    #[instrument(skip(self), fields(%path))]
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
        if p.is_empty() {
            return Ok(None);
        }
        let path2 = store_dir.parse_path(&p)?;
        assert_eq!(path, &path2);

        let deriver = self.source.read_string().await?;
        let deriver = if !deriver.is_empty() {
            Some(store_dir.parse_path(&deriver)?)
        } else {
            None
        };
        let references = self.source.read_parsed_coll(&store_dir).await?;
        self.source.read_u64_le().await?; // download size
        let nar_size = self.source.read_u64_le().await?;

        let s = self.source.read_string().await?;
        if s.is_empty() {
            return Err(Error::MandatoryNARHash);
        }
        let nar_hash: Hash = s.parse()?;
        let ca_s = self.source.read_string().await?;
        let ca = if !ca_s.is_empty() {
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

    #[instrument(skip(self, writer), fields(%path))]
    async fn nar_from_path<SW>(&mut self, path: &StorePath, writer: SW) -> Result<(), Error>
    where
        SW: AsyncWrite + fmt::Debug + Send + Unpin,
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

    #[instrument(skip_all)]
    async fn add_to_store<SR: AsyncRead + fmt::Debug + Send + Unpin>(
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

    #[instrument(skip(self, drv), fields(%drv_path, drv.name))]
    async fn build_derivation(
        &mut self,
        drv_path: &StorePath,
        drv: &BasicDerivation,
        build_mode: BuildMode,
    ) -> Result<BuildResult, Error> {
        let store_dir = self.store_dir.clone();

        let full_drv_path = store_dir.print_path(drv_path);
        let msg = match build_mode {
            BuildMode::Repair => {
                format!("repairing outputs of '{}' on {}", full_drv_path, self.host)
            }
            BuildMode::Check => format!("checking outputs of '{}' on {}", full_drv_path, self.host),
            _ => format!("building '{}' on {}", full_drv_path, self.host),
        };
        let act = activity!(
            Verbosity::Info,
            ActivityType::Build,
            msg,
            field0 = full_drv_path,
            field1 = self.host,
            field2 = 1,
            field3 = 1
        );

        let fut = async {
            let _log = self.log_dispatch.replace();

            let remote_version = self.remote_version().await?;
            self.sink
                .write_enum(ServeCommand::CmdBuildDerivation)
                .await?;

            trace!("Write drv_path {}", drv_path);
            self.sink.write_printed(&store_dir, drv_path).await?;
            drv.write_drv(&mut self.sink, &store_dir).await?;
            self.write_build_settings().await?;
            self.sink.flush().await?;

            let status = self.source.read_enum().await?;
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
        };
        fut.instrument(act.span).await
    }

    #[instrument(skip(self, drv_paths))]
    async fn build_paths(
        &mut self,
        drv_paths: &[DerivedPath],
        _build_mode: BuildMode,
    ) -> Result<(), Error> {
        debug!("Build paths {:?}", drv_paths);

        let _log = self.log_dispatch.replace();

        let _remote_version = self.remote_version().await?;
        let store_dir = self.store_dir.clone();
        self.sink.write_enum(ServeCommand::CmdBuildPaths).await?;

        self.sink.write_usize(drv_paths.len()).await?;
        for p in drv_paths {
            match p.into() {
                SPWOParseResult::StorePathWithOutputs(sp) => {
                    self.sink.write_printed(&store_dir, &sp).await?
                }
                SPWOParseResult::StorePath(path) => {
                    Err(Error::WantedFetchInLegacy(store_dir.print_path(&path)))?
                }
                SPWOParseResult::Unsupported => Err(Error::DerivationIsBuildProduct)?,
            }
        }

        self.write_build_settings().await?;
        self.sink.flush().await?;

        let status: BuildStatus = self.source.read_enum().await?;
        if status.success() {
            Ok(())
        } else {
            let error_msg = self.source.read_string().await?;
            Err(Error::Custom(status.into(), error_msg))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::io::Cursor;
    use std::time::Instant;

    use ::proptest::arbitrary::any;
    use ::proptest::proptest;
    use futures::future::try_join;

    use crate::archive::proptest::arb_nar_contents;
    use crate::hash;
    use crate::path_info::proptest::arb_valid_info_and_content;
    use crate::pretty_prop_assert_eq;
    use crate::store::assert_store::AssertStore;
    use crate::store::error::Verbosity;
    use crate::store::settings::{BuildSettings, WithSettings};
    use crate::store::StorePathWithOutputs;
    use crate::store_path::proptest::arb_drv_store_path;

    macro_rules! store_cmd {
        (
            $assert:ident($ae:expr$(,$ae2:expr)*$(,)?),
            $cmd:ident($ce:expr$(,$ce2:expr)*$(,)?),
            $res:expr
        ) => {{
            store_cmd!(BuildSettings::default(),
                $assert($ae $(,$ae2)*),
                $cmd($ce $(, $ce2)*),
                $res)

        }};
        (
            $settings:expr,
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

            r.block_on(async {
                let mut test_store = LegacyStoreClient::new(store_dir.clone(), "localhost".into(), read, write, build_log_client).await;

                let mut store = AssertStore::$assert($ae $(, $ae2)*);
                let (read, write) = tokio::io::split(server);
                let server = crate::store::legacy_worker::run_server_with_log(read, write, &mut store, build_log_server, true);

                let cmd = async {
                    let res = test_store.$cmd($ce $(, $ce2)*)
                        .with_settings($settings)
                        .await?;
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
        fn proptest_store_query_valid_paths_locked(
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
        fn proptest_store_nar_from_path(
            (nar_size, nar_hash, contents) in arb_nar_contents(8, 256, 10),
            path in any::<StorePath>(),
        )
        {
            let mut buf = Vec::new();
            store_cmd!(
                assert_nar_from_path(None, &path, Ok(contents.clone())),
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
        fn proptest_store_build_derivation(
            drv_path in arb_drv_store_path(),
            mut drv in any::<BasicDerivation>(),
            mut settings in any::<BuildSettings>(),
            result in any::<BuildResult>(),
        )
        {
            settings.verbosity = Verbosity::Error;
            let now = Instant::now();
            eprintln!("Run test {}", drv_path);
            drv.name = drv_path.name_from_drv().to_string();
            store_cmd!(
                settings,
                assert_build_derivation(None, &drv_path, &drv, BuildMode::Normal, &settings, Ok(result.clone())),
                build_derivation(&drv_path, &drv, BuildMode::Normal),
                result
            );
            eprintln!("Completed test {} in {}", drv_path, now.elapsed().as_secs_f64());
        }
    }

    proptest! {
        #[test]
        fn proptest_store_build_paths(
            drv_paths in any::<Vec<StorePathWithOutputs>>(),
            mut settings in any::<BuildSettings>(),
        )
        {
            settings.verbosity = Verbosity::Error;
            let drv_paths : Vec<DerivedPath> = drv_paths.into_iter().map(|path| path.into()).collect();
            store_cmd!(
                settings,
                assert_build_paths(None, &drv_paths, BuildMode::Normal, &settings, Ok(())),
                build_paths(&drv_paths, BuildMode::Normal),
                ()
            );
        }
    }

    proptest! {
       #[test]
       fn proptest_store_query_path_info(
            (info, _source) in arb_valid_info_and_content(8, 256, 10),
        )
        {
            store_cmd!(
                assert_query_path_info(None, &info.path.clone(), Ok(Some(info.clone()))),
                query_path_info(&info.path),
                Some(info)
            );
        }
    }

    proptest! {
       #[test]
       fn proptest_store_add_to_store(
            (info, source) in arb_valid_info_and_content(8, 256, 10),
        )
        {
            store_cmd!(
                assert_add_to_store(None, &info, source.clone(), RepairFlag::NoRepair, CheckSignaturesFlag::NoCheckSigs, Ok(())),
                add_to_store(&info, Cursor::new(source), RepairFlag::Repair, CheckSignaturesFlag::CheckSigs),
                ()
            );
        }
    }

    proptest! {
        #[test]
        fn proptest_store_query_closure(
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
