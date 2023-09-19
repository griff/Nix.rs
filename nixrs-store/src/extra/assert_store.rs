use async_trait::async_trait;
use bytes::Bytes;
use pretty_assertions::assert_eq;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::legacy_worker::LegacyStore;
use crate::StoreDirProvider;
use crate::{BasicDerivation, BuildResult, BuildSettings, CheckSignaturesFlag, Error, Store};
use crate::{DerivedPath, RepairFlag, StoreDir, StorePath, StorePathSet};
use crate::{SubstituteFlag, ValidPathInfo};

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone)]
pub enum Message {
    AddTempRoot(StorePath),
    QueryValidPaths {
        paths: StorePathSet,
        maybe_substitute: SubstituteFlag,
    },
    QueryPathInfo(StorePath),
    LegacyQueryValidPaths {
        paths: StorePathSet,
        lock: bool,
        maybe_substitute: SubstituteFlag,
    },
    NarFromPath(StorePath),
    ExportPaths(StorePathSet),
    ImportPaths(Bytes),
    BuildDerivation {
        drv_path: StorePath,
        drv: BasicDerivation,
        settings: BuildSettings,
    },
    BuildPaths {
        drv_paths: Vec<DerivedPath>,
        settings: BuildSettings,
    },
    AddToStore {
        info: ValidPathInfo,
        source: Bytes,
        repair: RepairFlag,
        check_sigs: CheckSignaturesFlag,
    },
    QueryClosure {
        paths: StorePathSet,
        include_outputs: bool,
    },
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone)]
pub enum MessageResponse {
    Empty,
    StorePathSet(StorePathSet),
    BuildResult((BuildResult, Bytes)),
    Bytes(Bytes),
    ValidPathInfo(Option<ValidPathInfo>),
}

impl From<()> for MessageResponse {
    fn from(_: ()) -> Self {
        MessageResponse::Empty
    }
}
impl From<StorePathSet> for MessageResponse {
    fn from(v: StorePathSet) -> Self {
        MessageResponse::StorePathSet(v)
    }
}
impl From<(BuildResult, Bytes)> for MessageResponse {
    fn from(v: (BuildResult, Bytes)) -> Self {
        MessageResponse::BuildResult(v)
    }
}
impl From<Bytes> for MessageResponse {
    fn from(v: Bytes) -> Self {
        MessageResponse::Bytes(v)
    }
}
impl From<Option<ValidPathInfo>> for MessageResponse {
    fn from(v: Option<ValidPathInfo>) -> Self {
        MessageResponse::ValidPathInfo(v)
    }
}

fn take(dest: &mut Result<MessageResponse, Error>) -> Result<MessageResponse, Error> {
    std::mem::replace(dest, Ok(MessageResponse::Empty))
}

pub struct AssertStore {
    store_dir: StoreDir,
    expected: Message,
    response: Result<MessageResponse, Error>,
}

impl AssertStore {
    pub fn assert_query_valid_paths(
        paths: &StorePathSet,
        maybe_substitute: SubstituteFlag,
        response: Result<StorePathSet, Error>,
    ) -> AssertStore {
        let store_dir = StoreDir::new("/nix/store").unwrap();
        let expected = Message::QueryValidPaths {
            paths: paths.clone(),
            maybe_substitute,
        };
        let response = response.map(|e| e.into());
        AssertStore {
            store_dir,
            expected,
            response,
        }
    }
    pub fn assert_query_path_info(
        path: &StorePath,
        response: Result<Option<ValidPathInfo>, Error>,
    ) -> AssertStore {
        let store_dir = StoreDir::new("/nix/store").unwrap();
        let expected = Message::QueryPathInfo(path.clone());
        let response = response.map(|e| e.into());
        AssertStore {
            store_dir,
            expected,
            response,
        }
    }
    pub fn assert_query_valid_paths_locked(
        paths: &StorePathSet,
        lock: bool,
        maybe_substitute: SubstituteFlag,
        response: Result<StorePathSet, Error>,
    ) -> AssertStore {
        let store_dir = StoreDir::new("/nix/store").unwrap();
        let expected = Message::LegacyQueryValidPaths {
            paths: paths.clone(),
            lock,
            maybe_substitute,
        };
        let response = response.map(|e| e.into());
        AssertStore {
            store_dir,
            expected,
            response,
        }
    }
    pub fn assert_nar_from_path(path: &StorePath, response: Result<Bytes, Error>) -> AssertStore {
        let store_dir = StoreDir::new("/nix/store").unwrap();
        let expected = Message::NarFromPath(path.clone());
        let response = response.map(|e| e.into());
        AssertStore {
            store_dir,
            expected,
            response,
        }
    }
    pub fn assert_export_paths(
        paths: &StorePathSet,
        response: Result<Bytes, Error>,
    ) -> AssertStore {
        let store_dir = StoreDir::new("/nix/store").unwrap();
        let expected = Message::ExportPaths(paths.clone());
        let response = response.map(|e| e.into());
        AssertStore {
            store_dir,
            expected,
            response,
        }
    }
    pub fn assert_import_paths(buf: Bytes, response: Result<(), Error>) -> AssertStore {
        let store_dir = StoreDir::new("/nix/store").unwrap();
        let expected = Message::ImportPaths(buf);
        let response = response.map(|e| e.into());
        AssertStore {
            store_dir,
            expected,
            response,
        }
    }
    pub fn assert_build_derivation(
        drv_path: &StorePath,
        drv: &BasicDerivation,
        settings: &BuildSettings,
        response: Result<(BuildResult, Bytes), Error>,
    ) -> AssertStore {
        let store_dir = StoreDir::new("/nix/store").unwrap();
        let expected = Message::BuildDerivation {
            drv_path: drv_path.clone(),
            drv: drv.clone(),
            settings: settings.clone(),
        };
        let response = response.map(|e| e.into());
        AssertStore {
            store_dir,
            expected,
            response,
        }
    }
    pub fn assert_build_paths(
        drv_paths: &[DerivedPath],
        settings: &BuildSettings,
        response: Result<Bytes, Error>,
    ) -> AssertStore {
        let store_dir = StoreDir::new("/nix/store").unwrap();
        let expected = Message::BuildPaths {
            drv_paths: drv_paths.into(),
            settings: settings.clone(),
        };
        let response = response.map(|e| e.into());
        AssertStore {
            store_dir,
            expected,
            response,
        }
    }
    pub fn assert_add_to_store(
        info: &ValidPathInfo,
        source: Bytes,
        repair: RepairFlag,
        check_sigs: CheckSignaturesFlag,
        response: Result<(), Error>,
    ) -> AssertStore {
        let store_dir = StoreDir::new("/nix/store").unwrap();
        let expected = Message::AddToStore {
            info: info.clone(),
            source,
            repair,
            check_sigs,
        };
        let response = response.map(|e| e.into());
        AssertStore {
            store_dir,
            expected,
            response,
        }
    }
    pub fn assert_query_closure(
        paths: &StorePathSet,
        include_outputs: bool,
        response: Result<StorePathSet, Error>,
    ) -> AssertStore {
        let store_dir = StoreDir::new("/nix/store").unwrap();
        let expected = Message::QueryClosure {
            paths: paths.clone(),
            include_outputs,
        };
        let response = response.map(|e| e.into());
        AssertStore {
            store_dir,
            expected,
            response,
        }
    }
}

impl StoreDirProvider for AssertStore {
    fn store_dir(&self) -> StoreDir {
        self.store_dir.clone()
    }
}

#[async_trait]
impl Store for AssertStore {
    async fn query_valid_paths(
        &mut self,
        paths: &StorePathSet,
        maybe_substitute: SubstituteFlag,
    ) -> Result<StorePathSet, Error> {
        let actual = Message::QueryValidPaths {
            paths: paths.clone(),
            maybe_substitute,
        };
        assert_eq!(self.expected, actual, "query_valid_paths");
        match take(&mut self.response)? {
            MessageResponse::StorePathSet(set) => Ok(set),
            e => panic!("Invalid response {:?} for query_valid_paths", e),
        }
    }

    async fn query_path_info(&mut self, path: &StorePath) -> Result<Option<ValidPathInfo>, Error> {
        let actual = Message::QueryPathInfo(path.clone());
        assert_eq!(self.expected, actual, "query_path_info");
        match take(&mut self.response)? {
            MessageResponse::ValidPathInfo(res) => Ok(res),
            e => panic!("Invalid response {:?} for query_path_info", e),
        }
    }

    async fn nar_from_path<W: AsyncWrite + Send + Unpin>(
        &mut self,
        path: &StorePath,
        mut sink: W,
    ) -> Result<(), Error> {
        let actual = Message::NarFromPath(path.clone());
        assert_eq!(self.expected, actual, "nar_from_path");
        match take(&mut self.response)? {
            MessageResponse::Bytes(set) => {
                sink.write_all(&set).await?;
                sink.flush().await?;
                Ok(())
            }
            e => panic!("Invalid response {:?} for nar_from_path", e),
        }
    }

    async fn add_to_store<R: AsyncRead + Send + Unpin>(
        &mut self,
        info: &ValidPathInfo,
        mut source: R,
        repair: RepairFlag,
        check_sigs: CheckSignaturesFlag,
    ) -> Result<(), Error> {
        let mut buf = Vec::new();
        source.read_to_end(&mut buf).await?;
        let actual = Message::AddToStore {
            info: info.clone(),
            source: buf.into(),
            repair,
            check_sigs,
        };
        assert_eq!(self.expected, actual, "add_to_store");
        match take(&mut self.response)? {
            MessageResponse::Empty => Ok(()),
            e => panic!("Invalid response {:?} for add_to_store", e),
        }
    }
    async fn build_derivation<W: AsyncWrite + Send + Unpin>(
        &mut self,
        drv_path: &StorePath,
        drv: &BasicDerivation,
        settings: &BuildSettings,
        mut build_log: W,
    ) -> Result<BuildResult, Error> {
        let actual = Message::BuildDerivation {
            drv_path: drv_path.clone(),
            drv: drv.clone(),
            settings: settings.clone(),
        };
        assert_eq!(self.expected, actual, "build_derivation");
        match take(&mut self.response)? {
            MessageResponse::BuildResult((res, set)) => {
                build_log.write_all(&set).await?;
                build_log.flush().await?;
                Ok(res)
            }
            e => panic!("Invalid response {:?} for build_derivation", e),
        }
    }
    async fn build_paths<W: AsyncWrite + Send + Unpin>(
        &mut self,
        drv_paths: &[DerivedPath],
        settings: &BuildSettings,
        mut build_log: W,
    ) -> Result<(), Error> {
        let actual = Message::BuildPaths {
            drv_paths: drv_paths.into(),
            settings: settings.clone(),
        };
        assert_eq!(self.expected, actual, "build_paths");
        match take(&mut self.response)? {
            MessageResponse::Bytes(set) => {
                build_log.write_all(&set).await?;
                build_log.flush().await?;
                Ok(())
            }
            MessageResponse::Empty => Ok(()),
            e => panic!("Invalid response {:?} for build_paths", e),
        }
    }
}

#[async_trait]
impl LegacyStore for AssertStore {
    async fn query_valid_paths_locked(
        &mut self,
        paths: &StorePathSet,
        lock: bool,
        maybe_substitute: SubstituteFlag,
    ) -> Result<StorePathSet, Error> {
        let actual = Message::LegacyQueryValidPaths {
            paths: paths.clone(),
            lock,
            maybe_substitute,
        };
        assert_eq!(self.expected, actual, "legacy_query_valid_paths");
        match take(&mut self.response)? {
            MessageResponse::StorePathSet(set) => Ok(set),
            e => panic!("Invalid response {:?} for legacy_query_valid_paths", e),
        }
    }
    async fn export_paths<W: AsyncWrite + Send + Unpin>(
        &mut self,
        paths: &StorePathSet,
        mut sink: W,
    ) -> Result<(), Error> {
        let actual = Message::ExportPaths(paths.clone());
        assert_eq!(self.expected, actual, "export_paths");
        match take(&mut self.response)? {
            MessageResponse::Bytes(set) => {
                sink.write_all(&set).await?;
                sink.flush().await?;
                Ok(())
            }
            e => panic!("Invalid response {:?} for export_paths", e),
        }
    }
    async fn import_paths<R: AsyncRead + Send + Unpin>(
        &mut self,
        mut source: R,
    ) -> Result<(), Error> {
        let mut buf = Vec::new();
        source.read_to_end(&mut buf).await?;
        let actual = Message::ImportPaths(buf.into());
        assert_eq!(self.expected, actual, "import_paths");
        match take(&mut self.response)? {
            MessageResponse::Empty => Ok(()),
            e => panic!("Invalid response {:?} for import_paths", e),
        }
    }
    async fn query_closure(
        &mut self,
        paths: &StorePathSet,
        include_outputs: bool,
    ) -> Result<StorePathSet, Error> {
        let actual = Message::QueryClosure {
            paths: paths.clone(),
            include_outputs,
        };
        assert_eq!(self.expected, actual, "query_closure");
        match take(&mut self.response)? {
            MessageResponse::StorePathSet(set) => Ok(set),
            e => panic!("Invalid response {:?} for query_closure", e),
        }
    }
}
