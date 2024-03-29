use std::fmt;

use async_trait::async_trait;
use tokio::io::{AsyncRead, AsyncWrite};
use tracing::instrument;

use crate::path_info::ValidPathInfo;
use crate::store::store_api::BuildMode;
use crate::store::{
    compute_fs_closure_slow, BasicDerivation, BuildResult, CheckSignaturesFlag, DerivedPath, Error,
    RepairFlag, Store, SubstituteFlag,
};
use crate::store_path::{StoreDir, StoreDirProvider, StorePath, StorePathSet};

use super::LegacyStore;

#[derive(Clone, Debug)]
pub struct LegacyWrapStore<S> {
    store: S,
}

impl<S> LegacyWrapStore<S> {
    pub fn new(store: S) -> LegacyWrapStore<S> {
        Self { store }
    }
}

impl<S: StoreDirProvider> StoreDirProvider for LegacyWrapStore<S> {
    fn store_dir(&self) -> StoreDir {
        self.store.store_dir()
    }
}

#[async_trait]
impl<S: Store + Send> Store for LegacyWrapStore<S> {
    async fn query_valid_paths(
        &mut self,
        paths: &StorePathSet,
        maybe_substitute: SubstituteFlag,
    ) -> Result<StorePathSet, Error> {
        self.store.query_valid_paths(paths, maybe_substitute).await
    }

    async fn query_path_info(&mut self, path: &StorePath) -> Result<Option<ValidPathInfo>, Error> {
        self.store.query_path_info(path).await
    }

    /// Export path from the store
    async fn nar_from_path<W: AsyncWrite + fmt::Debug + Send + Unpin>(
        &mut self,
        path: &StorePath,
        sink: W,
    ) -> Result<(), Error> {
        self.store.nar_from_path(path, sink).await
    }

    /// Import a path into the store.
    async fn add_to_store<R: AsyncRead + fmt::Debug + Send + Unpin>(
        &mut self,
        info: &ValidPathInfo,
        source: R,
        repair: RepairFlag,
        check_sigs: CheckSignaturesFlag,
    ) -> Result<(), Error> {
        self.store
            .add_to_store(info, source, repair, check_sigs)
            .await
    }

    async fn build_derivation(
        &mut self,
        drv_path: &StorePath,
        drv: &BasicDerivation,
        build_mode: BuildMode,
    ) -> Result<BuildResult, Error> {
        self.store.build_derivation(drv_path, drv, build_mode).await
    }

    async fn build_paths(
        &mut self,
        drv_paths: &[DerivedPath],
        build_mode: BuildMode,
    ) -> Result<(), Error> {
        self.store.build_paths(drv_paths, build_mode).await
    }
}

#[async_trait]
impl<S> LegacyStore for LegacyWrapStore<S>
where
    S: Store + fmt::Debug + Send,
{
    async fn query_valid_paths_locked(
        &mut self,
        paths: &StorePathSet,
        _lock: bool,
        maybe_substitute: SubstituteFlag,
    ) -> Result<StorePathSet, Error> {
        self.store.query_valid_paths(paths, maybe_substitute).await
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

    #[instrument(skip_all)]
    async fn query_closure(
        &mut self,
        paths: &StorePathSet,
        include_outputs: bool,
    ) -> Result<StorePathSet, Error> {
        if include_outputs {
            Err(Error::UnsupportedOperation("query_closure".into()))
        } else {
            compute_fs_closure_slow(&mut self.store, paths, false).await
        }
    }
}
