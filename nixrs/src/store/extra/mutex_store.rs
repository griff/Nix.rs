use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::Mutex;

use crate::store::{legacy_worker::LegacyStore, Store, StoreDir, StoreDirProvider};

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
        paths: &crate::store::StorePathSet,
        maybe_substitute: crate::store::SubstituteFlag,
    ) -> Result<crate::store::StorePathSet, crate::store::Error> {
        let mut store = self.store.lock().await;
        store.query_valid_paths(paths, maybe_substitute).await
    }

    async fn query_path_info(
        &mut self,
        path: &crate::store::StorePath,
    ) -> Result<Option<crate::store::ValidPathInfo>, crate::store::Error> {
        let mut store = self.store.lock().await;
        store.query_path_info(path).await
    }

    async fn nar_from_path<W: tokio::io::AsyncWrite + Send + Unpin>(
        &mut self,
        path: &crate::store::StorePath,
        sink: W,
    ) -> Result<(), crate::store::Error> {
        let mut store = self.store.lock().await;
        store.nar_from_path(path, sink).await
    }

    async fn add_to_store<R: tokio::io::AsyncRead + Send + Unpin>(
        &mut self,
        info: &crate::store::ValidPathInfo,
        source: R,
        repair: crate::store::RepairFlag,
        check_sigs: crate::store::CheckSignaturesFlag,
    ) -> Result<(), crate::store::Error> {
        let mut store = self.store.lock().await;
        store.add_to_store(info, source, repair, check_sigs).await
    }

    async fn build_derivation<W: tokio::io::AsyncWrite + Send + Unpin>(
        &mut self,
        drv_path: &crate::store::StorePath,
        drv: &crate::store::BasicDerivation,
        settings: &crate::store::BuildSettings,
        build_log: W,
    ) -> Result<crate::store::BuildResult, crate::store::Error> {
        let mut store = self.store.lock().await;
        store
            .build_derivation(drv_path, drv, settings, build_log)
            .await
    }

    async fn build_paths<W: tokio::io::AsyncWrite + Send + Unpin>(
        &mut self,
        drv_paths: &[crate::store::DerivedPath],
        settings: &crate::store::BuildSettings,
        build_log: W,
    ) -> Result<(), crate::store::Error> {
        let mut store = self.store.lock().await;
        store.build_paths(drv_paths, settings, build_log).await
    }
}

#[async_trait]
impl<S> LegacyStore for MutexStore<S>
where
    S: LegacyStore + Send,
{
    async fn query_valid_paths_locked(
        &mut self,
        paths: &crate::store::StorePathSet,
        lock: bool,
        maybe_substitute: crate::store::SubstituteFlag,
    ) -> Result<crate::store::StorePathSet, crate::store::Error> {
        let mut store = self.store.lock().await;
        store
            .query_valid_paths_locked(paths, lock, maybe_substitute)
            .await
    }

    async fn export_paths<W: tokio::io::AsyncWrite + Send + Unpin>(
        &mut self,
        paths: &crate::store::StorePathSet,
        sink: W,
    ) -> Result<(), crate::store::Error> {
        let mut store = self.store.lock().await;
        store.export_paths(paths, sink).await
    }

    async fn import_paths<R: tokio::io::AsyncRead + Send + Unpin>(
        &mut self,
        source: R,
    ) -> Result<(), crate::store::Error> {
        let mut store = self.store.lock().await;
        store.import_paths(source).await
    }

    async fn query_closure(
        &mut self,
        paths: &crate::store::StorePathSet,
        include_outputs: bool,
    ) -> Result<crate::store::StorePathSet, crate::store::Error> {
        let mut store = self.store.lock().await;
        store.query_closure(paths, include_outputs).await
    }
}
