use async_trait::async_trait;
use tokio::io::{AsyncRead, AsyncWrite};

use crate::{
    path_info::ValidPathInfo,
    store_path::{StoreDirProvider, StorePath, StorePathSet},
};

use super::{
    CheckSignaturesFlag, DerivedPath, Error, RepairFlag, Store, SubstituteFlag,
    daemon::{DaemonStore, QueryMissingResult, TrustedFlag},
    legacy_worker::LegacyStore,
};

#[derive(Debug)]
pub struct FailStore;

impl StoreDirProvider for FailStore {
    fn store_dir(&self) -> crate::store_path::StoreDir {
        Default::default()
    }
}

#[async_trait]
impl Store for FailStore {
    async fn query_path_info(&mut self, _path: &StorePath) -> Result<Option<ValidPathInfo>, Error> {
        Err(Error::UnsupportedOperation("query_path_info".into()))
    }

    async fn nar_from_path<W: AsyncWrite + Send + Unpin>(
        &mut self,
        _path: &StorePath,
        _sink: W,
    ) -> Result<(), Error> {
        Err(Error::UnsupportedOperation("nar_from_path".into()))
    }

    async fn add_to_store<R: AsyncRead + Send + Unpin>(
        &mut self,
        _info: &ValidPathInfo,
        _source: R,
        _repair: RepairFlag,
        _check_sigs: CheckSignaturesFlag,
    ) -> Result<(), Error> {
        Err(Error::UnsupportedOperation("add_to_store".into()))
    }
}

#[async_trait]
impl LegacyStore for FailStore {
    async fn query_valid_paths_locked(
        &mut self,
        _paths: &StorePathSet,
        _lock: bool,
        _maybe_substitute: SubstituteFlag,
    ) -> Result<StorePathSet, Error> {
        Err(Error::UnsupportedOperation(
            "query_valid_paths_locked".into(),
        ))
    }
    async fn export_paths<SW: AsyncWrite + Send + Unpin>(
        &mut self,
        _paths: &StorePathSet,
        _sink: SW,
    ) -> Result<(), Error> {
        Err(Error::UnsupportedOperation("export_paths".into()))
    }
    async fn import_paths<SR: AsyncRead + Send + Unpin>(
        &mut self,
        _source: SR,
    ) -> Result<(), Error> {
        Err(Error::UnsupportedOperation("import_paths".into()))
    }
    async fn query_closure(
        &mut self,
        _paths: &StorePathSet,
        _include_outputs: bool,
    ) -> Result<StorePathSet, Error> {
        Err(Error::UnsupportedOperation("query_closure".into()))
    }
}

#[async_trait]
impl DaemonStore for FailStore {
    fn is_trusted_client(&self) -> Option<TrustedFlag> {
        None
    }
    async fn set_options(&mut self) -> Result<(), Error> {
        Err(Error::UnsupportedOperation("set_options".into()))
    }
    async fn is_valid_path(&mut self, _path: &StorePath) -> Result<bool, Error> {
        Err(Error::UnsupportedOperation("is_valid_path".into()))
    }

    async fn add_multiple_to_store<R: AsyncRead + Send + Unpin>(
        &mut self,
        _source: R,
        _repair: RepairFlag,
        _check_sigs: CheckSignaturesFlag,
    ) -> Result<(), Error> {
        Err(Error::UnsupportedOperation("add_multiple_to_store".into()))
    }

    async fn query_missing(
        &mut self,
        _targets: &[DerivedPath],
    ) -> Result<QueryMissingResult, Error> {
        Err(Error::UnsupportedOperation("query_missing".into()))
    }
}
