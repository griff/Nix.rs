use async_trait::async_trait;
use bytes::Bytes;
use pretty_assertions::assert_eq;
use proptest::test_runner::TestCaseError;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::path_info::ValidPathInfo;
use crate::pretty_prop_assert_eq;
use crate::store::legacy_worker::LegacyStore;
use crate::store::settings::BuildSettings;
use crate::store::{BasicDerivation, BuildMode, BuildResult, CheckSignaturesFlag, Error, Store};
use crate::store::{DerivedPath, RepairFlag, SubstituteFlag};
use crate::store_path::{StoreDir, StoreDirProvider, StorePath, StorePathSet};

use super::daemon::{DaemonStore, QueryMissingResult, TrustedFlag};

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
        build_mode: BuildMode,
        settings: BuildSettings,
    },
    BuildPaths {
        drv_paths: Vec<DerivedPath>,
        build_mode: BuildMode,
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
    QueryMissing(Vec<DerivedPath>),
    IsValidPath(StorePath),
    AddMultipleToStore {
        source: Bytes,
        repair: RepairFlag,
        check_sigs: CheckSignaturesFlag,
    },
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone)]
pub enum MessageResponse {
    Empty,
    Bool(bool),
    StorePathSet(StorePathSet),
    BuildResult(BuildResult),
    Bytes(Bytes),
    ValidPathInfo(Option<ValidPathInfo>),
    QueryMissingResult(QueryMissingResult),
}

impl From<()> for MessageResponse {
    fn from(_: ()) -> Self {
        MessageResponse::Empty
    }
}
impl From<bool> for MessageResponse {
    fn from(val: bool) -> Self {
        MessageResponse::Bool(val)
    }
}

impl From<StorePathSet> for MessageResponse {
    fn from(v: StorePathSet) -> Self {
        MessageResponse::StorePathSet(v)
    }
}
impl From<BuildResult> for MessageResponse {
    fn from(v: BuildResult) -> Self {
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
impl From<QueryMissingResult> for MessageResponse {
    fn from(v: QueryMissingResult) -> Self {
        MessageResponse::QueryMissingResult(v)
    }
}

fn take(dest: &mut Result<MessageResponse, Error>) -> Result<MessageResponse, Error> {
    std::mem::replace(dest, Ok(MessageResponse::Empty))
}

#[derive(Debug)]
pub struct AssertStore {
    trusted_client: Option<TrustedFlag>,
    store_dir: StoreDir,
    expected: Message,
    actual: Option<Message>,
    response: Result<MessageResponse, Error>,
}

impl AssertStore {
    pub fn assert_query_valid_paths(
        trusted_client: Option<TrustedFlag>,
        paths: &StorePathSet,
        maybe_substitute: SubstituteFlag,
        response: Result<StorePathSet, Error>,
    ) -> AssertStore {
        let store_dir = Default::default();
        let expected = Message::QueryValidPaths {
            paths: paths.clone(),
            maybe_substitute,
        };
        let response = response.map(|e| e.into());
        AssertStore {
            trusted_client,
            store_dir,
            expected,
            response,
            actual: None,
        }
    }
    pub fn assert_query_path_info(
        trusted_client: Option<TrustedFlag>,
        path: &StorePath,
        response: Result<Option<ValidPathInfo>, Error>,
    ) -> AssertStore {
        let store_dir = Default::default();
        let expected = Message::QueryPathInfo(path.clone());
        let response = response.map(|e| e.into());
        AssertStore {
            trusted_client,
            store_dir,
            expected,
            response,
            actual: None,
        }
    }
    pub fn assert_query_valid_paths_locked(
        paths: &StorePathSet,
        lock: bool,
        maybe_substitute: SubstituteFlag,
        response: Result<StorePathSet, Error>,
    ) -> AssertStore {
        let store_dir = Default::default();
        let expected = Message::LegacyQueryValidPaths {
            paths: paths.clone(),
            lock,
            maybe_substitute,
        };
        let response = response.map(|e| e.into());
        AssertStore {
            trusted_client: None,
            store_dir,
            expected,
            response,
            actual: None,
        }
    }
    pub fn assert_nar_from_path(
        trusted_client: Option<TrustedFlag>,
        path: &StorePath,
        response: Result<Bytes, Error>,
    ) -> AssertStore {
        let store_dir = Default::default();
        let expected = Message::NarFromPath(path.clone());
        let response = response.map(|e| e.into());
        AssertStore {
            trusted_client,
            store_dir,
            expected,
            response,
            actual: None,
        }
    }
    pub fn assert_export_paths(
        trusted_client: Option<TrustedFlag>,
        paths: &StorePathSet,
        response: Result<Bytes, Error>,
    ) -> AssertStore {
        let store_dir = Default::default();
        let expected = Message::ExportPaths(paths.clone());
        let response = response.map(|e| e.into());
        AssertStore {
            trusted_client,
            store_dir,
            expected,
            response,
            actual: None,
        }
    }
    pub fn assert_import_paths(
        trusted_client: Option<TrustedFlag>,
        buf: Bytes,
        response: Result<(), Error>,
    ) -> AssertStore {
        let store_dir = Default::default();
        let expected = Message::ImportPaths(buf);
        let response = response.map(|e| e.into());
        AssertStore {
            trusted_client,
            store_dir,
            expected,
            response,
            actual: None,
        }
    }
    pub fn assert_build_derivation(
        trusted_client: Option<TrustedFlag>,
        drv_path: &StorePath,
        drv: &BasicDerivation,
        build_mode: BuildMode,
        settings: &BuildSettings,
        response: Result<BuildResult, Error>,
    ) -> AssertStore {
        let store_dir = Default::default();
        let expected = Message::BuildDerivation {
            drv_path: drv_path.clone(),
            drv: drv.clone(),
            build_mode,
            settings: settings.clone(),
        };
        let response = response.map(|e| e.into());
        AssertStore {
            trusted_client,
            store_dir,
            expected,
            response,
            actual: None,
        }
    }
    pub fn assert_build_paths(
        trusted_client: Option<TrustedFlag>,
        drv_paths: &[DerivedPath],
        build_mode: BuildMode,
        settings: &BuildSettings,
        response: Result<(), Error>,
    ) -> AssertStore {
        let store_dir = Default::default();
        let expected = Message::BuildPaths {
            drv_paths: drv_paths.into(),
            build_mode,
            settings: settings.clone(),
        };
        let response = response.map(|e| e.into());
        AssertStore {
            trusted_client,
            store_dir,
            expected,
            response,
            actual: None,
        }
    }
    pub fn assert_add_to_store(
        trusted_client: Option<TrustedFlag>,
        info: &ValidPathInfo,
        source: Bytes,
        repair: RepairFlag,
        check_sigs: CheckSignaturesFlag,
        response: Result<(), Error>,
    ) -> AssertStore {
        let store_dir = Default::default();
        let expected = Message::AddToStore {
            info: info.clone(),
            source,
            repair,
            check_sigs,
        };
        let response = response.map(|e| e.into());
        AssertStore {
            trusted_client,
            store_dir,
            expected,
            response,
            actual: None,
        }
    }
    pub fn assert_query_closure(
        paths: &StorePathSet,
        include_outputs: bool,
        response: Result<StorePathSet, Error>,
    ) -> AssertStore {
        let store_dir = Default::default();
        let expected = Message::QueryClosure {
            paths: paths.clone(),
            include_outputs,
        };
        let response = response.map(|e| e.into());
        AssertStore {
            trusted_client: None,
            store_dir,
            expected,
            response,
            actual: None,
        }
    }

    pub fn assert_is_valid_path(path: &StorePath, response: Result<bool, Error>) -> AssertStore {
        let store_dir = Default::default();
        let expected = Message::IsValidPath(path.clone());
        let response = response.map(|e| e.into());
        AssertStore {
            trusted_client: None,
            store_dir,
            expected,
            response,
            actual: None,
        }
    }

    pub fn assert_add_multiple_to_store(
        trusted_client: Option<TrustedFlag>,
        source: Bytes,
        repair: RepairFlag,
        check_sigs: CheckSignaturesFlag,
        response: Result<(), Error>,
    ) -> AssertStore {
        let store_dir = Default::default();
        let expected = Message::AddMultipleToStore {
            source,
            repair,
            check_sigs,
        };
        let response = response.map(|e| e.into());
        AssertStore {
            trusted_client,
            store_dir,
            expected,
            response,
            actual: None,
        }
    }
    pub fn assert_query_missing(
        targets: &[DerivedPath],
        response: Result<QueryMissingResult, Error>,
    ) -> AssertStore {
        let store_dir = Default::default();
        let expected = Message::QueryMissing(targets.into());
        let response = response.map(|e| e.into());
        AssertStore {
            trusted_client: None,
            store_dir,
            expected,
            response,
            actual: None,
        }
    }

    pub fn prop_assert_eq(self) -> Result<(), TestCaseError> {
        pretty_prop_assert_eq!(self.expected, self.actual.unwrap());
        Ok(())
    }

    pub fn assert_eq(self) {
        ::pretty_assertions::assert_eq!(self.expected, self.actual.unwrap());
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
        assert_eq!(None, self.actual.take(), "existing result");
        self.actual = Some(actual);
        match take(&mut self.response)? {
            MessageResponse::StorePathSet(set) => Ok(set),
            e => panic!("Invalid response {:?} for query_valid_paths", e),
        }
    }

    async fn query_path_info(&mut self, path: &StorePath) -> Result<Option<ValidPathInfo>, Error> {
        let actual = Message::QueryPathInfo(path.clone());
        assert_eq!(None, self.actual.take(), "existing result");
        self.actual = Some(actual);
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
        assert_eq!(None, self.actual.take(), "existing result");
        self.actual = Some(actual);
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
        assert_eq!(None, self.actual.take(), "existing result");
        self.actual = Some(actual);
        match take(&mut self.response)? {
            MessageResponse::Empty => Ok(()),
            e => panic!("Invalid response {:?} for add_to_store", e),
        }
    }
    async fn build_derivation(
        &mut self,
        drv_path: &StorePath,
        drv: &BasicDerivation,
        build_mode: BuildMode,
    ) -> Result<BuildResult, Error> {
        let actual = Message::BuildDerivation {
            drv_path: drv_path.clone(),
            drv: drv.clone(),
            build_mode,
            settings: BuildSettings::default(),
        };
        assert_eq!(None, self.actual.take(), "existing result");
        self.actual = Some(actual);
        match take(&mut self.response)? {
            MessageResponse::BuildResult(res) => Ok(res),
            e => panic!("Invalid response {:?} for build_derivation", e),
        }
    }
    async fn build_paths(
        &mut self,
        drv_paths: &[DerivedPath],
        build_mode: BuildMode,
    ) -> Result<(), Error> {
        let actual = Message::BuildPaths {
            drv_paths: drv_paths.into(),
            build_mode,
            settings: BuildSettings::default(),
        };
        assert_eq!(None, self.actual.take(), "existing result");
        self.actual = Some(actual);
        match take(&mut self.response)? {
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
        assert_eq!(None, self.actual.take(), "existing result");
        self.actual = Some(actual);
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
        assert_eq!(None, self.actual.take(), "existing result");
        self.actual = Some(actual);
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
        assert_eq!(None, self.actual.take(), "existing result");
        self.actual = Some(actual);
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
        assert_eq!(None, self.actual.take(), "existing result");
        self.actual = Some(actual);
        match take(&mut self.response)? {
            MessageResponse::StorePathSet(set) => Ok(set),
            e => panic!("Invalid response {:?} for query_closure", e),
        }
    }
}

#[async_trait]
impl DaemonStore for AssertStore {
    fn is_trusted_client(&self) -> Option<TrustedFlag> {
        self.trusted_client
    }

    async fn set_options(&mut self) -> Result<(), Error> {
        Ok(())
    }

    async fn is_valid_path(&mut self, path: &StorePath) -> Result<bool, Error> {
        let actual = Message::IsValidPath(path.clone());
        assert_eq!(None, self.actual.take(), "existing result");
        self.actual = Some(actual);
        match take(&mut self.response)? {
            MessageResponse::Bool(res) => Ok(res),
            e => panic!("Invalid response {:?} for is_valid_path", e),
        }
    }

    async fn add_multiple_to_store<R: AsyncRead + Send + Unpin>(
        &mut self,
        mut source: R,
        repair: RepairFlag,
        check_sigs: CheckSignaturesFlag,
    ) -> Result<(), Error> {
        let mut buf = Vec::new();
        source.read_to_end(&mut buf).await?;
        let actual = Message::AddMultipleToStore {
            source: buf.into(),
            repair,
            check_sigs,
        };
        assert_eq!(None, self.actual.take(), "existing result");
        self.actual = Some(actual);
        match take(&mut self.response)? {
            MessageResponse::Empty => Ok(()),
            e => panic!("Invalid response {:?} for add_multiple_to_store", e),
        }
    }

    async fn query_missing(
        &mut self,
        targets: &[DerivedPath],
    ) -> Result<QueryMissingResult, Error> {
        let actual = Message::QueryMissing(targets.into());
        assert_eq!(None, self.actual.take(), "existing result");
        self.actual = Some(actual);
        match take(&mut self.response)? {
            MessageResponse::QueryMissingResult(res) => Ok(res),
            e => panic!("Invalid response {:?} for query_missing", e),
        }
    }
}
