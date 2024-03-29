use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::Mutex;

use crate::path_info::ValidPathInfo;
use crate::store::{legacy_worker::LegacyStore, Store};
use crate::store::{
    BasicDerivation, BuildMode, BuildResult, CheckSignaturesFlag, DerivedPath, Error, RepairFlag,
    SubstituteFlag,
};
use crate::store_path::{StoreDir, StoreDirProvider, StorePath, StorePathSet};

#[derive(Clone)]
pub struct MutexStore<S> {
    store_dir: StoreDir,
    store: Arc<Mutex<S>>,
}

impl<S> StoreDirProvider for MutexStore<S> {
    fn store_dir(&self) -> StoreDir {
        self.store_dir.clone()
    }
}

#[async_trait]
impl<S> Store for MutexStore<S>
where
    S: Store + Send,
{
    async fn query_valid_paths(
        &mut self,
        paths: &StorePathSet,
        maybe_substitute: SubstituteFlag,
    ) -> Result<StorePathSet, Error> {
        let mut store = self.store.lock().await;
        store.query_valid_paths(paths, maybe_substitute).await
    }

    async fn query_path_info(&mut self, path: &StorePath) -> Result<Option<ValidPathInfo>, Error> {
        let mut store = self.store.lock().await;
        store.query_path_info(path).await
    }

    async fn nar_from_path<W: AsyncWrite + fmt::Debug + Send + Unpin>(
        &mut self,
        path: &StorePath,
        sink: W,
    ) -> Result<(), Error> {
        let mut store = self.store.lock().await;
        store.nar_from_path(path, sink).await
    }

    async fn add_to_store<R: AsyncRead + fmt::Debug + Send + Unpin>(
        &mut self,
        info: &ValidPathInfo,
        source: R,
        repair: RepairFlag,
        check_sigs: CheckSignaturesFlag,
    ) -> Result<(), Error> {
        let mut store = self.store.lock().await;
        store.add_to_store(info, source, repair, check_sigs).await
    }

    async fn build_derivation(
        &mut self,
        drv_path: &StorePath,
        drv: &BasicDerivation,
        build_mode: BuildMode,
    ) -> Result<BuildResult, Error> {
        let mut store = self.store.lock().await;
        store.build_derivation(drv_path, drv, build_mode).await
    }

    async fn build_paths(
        &mut self,
        drv_paths: &[DerivedPath],
        build_mode: BuildMode,
    ) -> Result<(), Error> {
        let mut store = self.store.lock().await;
        store.build_paths(drv_paths, build_mode).await
    }
}

#[async_trait]
impl<S> LegacyStore for MutexStore<S>
where
    S: LegacyStore + Send,
{
    async fn query_valid_paths_locked(
        &mut self,
        paths: &StorePathSet,
        lock: bool,
        maybe_substitute: SubstituteFlag,
    ) -> Result<StorePathSet, Error> {
        let mut store = self.store.lock().await;
        store
            .query_valid_paths_locked(paths, lock, maybe_substitute)
            .await
    }

    async fn export_paths<W: AsyncWrite + fmt::Debug + Send + Unpin>(
        &mut self,
        paths: &StorePathSet,
        sink: W,
    ) -> Result<(), Error> {
        let mut store = self.store.lock().await;
        store.export_paths(paths, sink).await
    }

    async fn import_paths<R: AsyncRead + fmt::Debug + Send + Unpin>(
        &mut self,
        source: R,
    ) -> Result<(), Error> {
        let mut store = self.store.lock().await;
        store.import_paths(source).await
    }

    async fn query_closure(
        &mut self,
        paths: &StorePathSet,
        include_outputs: bool,
    ) -> Result<StorePathSet, Error> {
        let mut store = self.store.lock().await;
        store.query_closure(paths, include_outputs).await
    }
}
