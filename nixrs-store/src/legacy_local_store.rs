use std::convert::TryInto;
use std::ffi::OsStr;
use std::fmt;
use std::path::Path;
use std::process::Stdio;
use std::time::SystemTime;

use async_trait::async_trait;
use derive_more::{LowerHex, UpperHex};
use log::{debug, trace};
use nixrs_util::archive::copy_nar;
use nixrs_util::hash::Hash;
use nixrs_util::io::{AsyncSink, AsyncSource};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::{ChildStdin, ChildStdout, Command};

use crate::store_api::EXPORT_MAGIC;
use crate::Error;
use crate::StorePathWithOutputs;
use crate::ValidPathInfo;
use crate::{BasicDerivation, BuildResult, BuildStatus, CheckSignaturesFlag, DerivedPath};
use crate::{BuildSettings, ParseStorePathError};
use crate::{RepairFlag, Store, StoreDir, StorePath, StorePathSet, SubstituteFlag};
use nixrs_util::num_enum;

pub const SERVE_MAGIC_1: u64 = 0x390c9deb;
pub const SERVE_MAGIC_2: u64 = 0x5452eecb;

pub const SERVE_PROTOCOL_VERSION: u64 = 2 << 8 | 6;

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

num_enum! {
    #[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, UpperHex, LowerHex)]
    pub enum ServeCommand {
        Unknown(u64),
        CmdQueryValidPaths = 1,
        CmdQueryPathInfos = 2,
        CmdDumpStorePath = 3,
        CmdImportPaths = 4,
        CmdExportPaths = 5,
        CmdBuildPaths = 6,
        CmdQueryClosure = 7,
        CmdBuildDerivation = 8,
        CmdAddToStoreNar = 9
    }
}

impl fmt::Display for ServeCommand {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use ServeCommand::*;
        match self {
            Unknown(cmd) => write!(f, "unknown command {}", cmd),
            CmdQueryValidPaths => write!(f, "query valid paths"),
            CmdQueryPathInfos => write!(f, "query path infos"),
            CmdDumpStorePath => write!(f, "dump store path"),
            CmdImportPaths => write!(f, "import paths"),
            CmdExportPaths => write!(f, "exports paths"),
            CmdBuildPaths => write!(f, "build paths"),
            CmdQueryClosure => write!(f, "query closure"),
            CmdBuildDerivation => write!(f, "build derviation"),
            CmdAddToStoreNar => write!(f, "add to store"),
        }
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

    pub async fn connect(self) -> Result<LegacyLocalStore<ChildStdout, ChildStdin>, Error> {
        let mut cmd = self.cmd;
        cmd.stdin(Stdio::piped());
        cmd.stdout(Stdio::piped());
        let mut child = cmd.spawn()?;
        let reader = child.stdout.take().unwrap();
        let writer = child.stdin.take().unwrap();
        let mut store = LegacyLocalStore::new(self.store_dir, self.host, reader, writer);
        store.handshake().await?;
        Ok(store)
    }
}

pub struct LegacyLocalStore<R, W> {
    host: String,
    store_dir: StoreDir,
    source: R,
    sink: W,
    remote_version: Option<u64>,
}

impl LegacyLocalStore<ChildStdout, ChildStdin> {
    pub async fn connect(
        write_allowed: bool,
    ) -> Result<LegacyLocalStore<ChildStdout, ChildStdin>, Error> {
        let mut b = LegacyStoreBuilder::new("nix-store");
        b.command_mut().arg("--serve");
        if write_allowed {
            b.command_mut().arg("--write");
        }
        b.connect().await
    }
}

impl<R, W> LegacyLocalStore<R, W>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    pub fn new(store_dir: StoreDir, host: String, reader: R, writer: W) -> LegacyLocalStore<R, W> {
        let sink = writer;
        let remote_version = None;
        LegacyLocalStore {
            store_dir,
            source: reader,
            sink,
            remote_version,
            host,
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

#[async_trait(?Send)]
impl<R, W> Store for LegacyLocalStore<R, W>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    fn store_dir(&self) -> StoreDir {
        self.store_dir.clone()
    }

    async fn query_path_info(&mut self, path: &StorePath) -> Result<ValidPathInfo, Error> {
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
            return Err(Error::InvalidPath(store_dir.print_path(path)));
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
        let sigs = self.source.read_string_coll().await?;

        let s = self.source.read_string().await?;
        assert_eq!(s, "");
        Ok(ValidPathInfo {
            path: path2,
            deriver,
            references,
            nar_size,
            nar_hash,
            ca,
            sigs,
            ultimate: false,
            registration_time: SystemTime::UNIX_EPOCH,
        })
    }

    async fn query_valid_paths(
        &mut self,
        paths: &StorePathSet,
        maybe_substitute: SubstituteFlag,
    ) -> Result<StorePathSet, Error> {
        self.legacy_query_valid_paths(paths, false, maybe_substitute)
            .await
    }

    async fn add_temp_root(&self, _path: &StorePath) {
        unimplemented!()
    }

    async fn legacy_query_valid_paths(
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

    async fn nar_from_path<SW>(&mut self, path: &StorePath, writer: SW) -> Result<(), Error>
    where
        SW: AsyncWrite + Unpin,
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

    async fn export_paths<SW: AsyncWrite + Unpin>(
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

    async fn import_paths<SR: AsyncRead + Unpin>(&mut self, mut source: SR) -> Result<(), Error> {
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

    async fn build_derivation(
        &mut self,
        drv_path: &StorePath,
        drv: &BasicDerivation,
        settings: &BuildSettings,
    ) -> Result<BuildResult, Error> {
        debug!("Build derivation {} with path {}", drv.name, drv_path);
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

        let status: BuildStatus = self.source.read_enum().await?;
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

    async fn build_paths(
        &mut self,
        drv_paths: &[DerivedPath],
        settings: &BuildSettings,
    ) -> Result<(), Error> {
        debug!("Build paths {:?}", drv_paths);
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

        let status: BuildStatus = self.source.read_enum().await?;
        if status.success() {
            Ok(())
        } else {
            let error_msg = self.source.read_string().await?;
            Err(Error::Custom(status.into(), error_msg))
        }
    }

    async fn add_to_store<SR: AsyncRead + Unpin>(
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
            self.sink.write_string_coll(&info.sigs).await?;
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
            let mut test_store = LegacyLocalStore::new(store_dir.clone(), "localhost".into(), read, write);

            r.block_on(async {
                let store = AssertStore::$assert($ae $(, $ae2)*);
                let (read, write) = tokio::io::split(server);
                let server = crate::nix_store::serve(read, write, store, true);

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
        fn test_store_legacy_query_valid_paths(
            paths in any::<StorePathSet>(),
            lock in ::proptest::bool::ANY,
            maybe_substitute in ::proptest::bool::ANY,
            result in any::<StorePathSet>(),
        )
        {
            store_cmd!(
                assert_legacy_query_valid_paths(&paths, lock, maybe_substitute.into(), Ok(result.clone())),
                legacy_query_valid_paths(&paths, lock, maybe_substitute.into()),
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
        )
        {
            let now = Instant::now();
            eprintln!("Run test {}", drv_path);
            drv.name = drv_path.name_from_drv();
            store_cmd!(
                assert_build_derivation(&drv_path, &drv, &settings, Ok(result.clone())),
                build_derivation(&drv_path, &drv, &settings),
                result
            );
            eprintln!("Completed test {} in {}", drv_path, now.elapsed().as_secs());
        }
    }

    proptest! {
        #[test]
        fn test_store_build_paths(
            drv_paths in any::<Vec<DerivedPath>>(),
            settings in any::<BuildSettings>(),
        )
        {
            store_cmd!(
                assert_build_paths(&drv_paths, &settings, Ok(())),
                build_paths(&drv_paths, &settings),
                ()
            );
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
