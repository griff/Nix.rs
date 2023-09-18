use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::Mutex;

use crate::{StoreDir, store_api::StoreDirProvider, Store, legacy_worker::LegacyStore};

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
    where S: Store + Send,
{
    async fn query_valid_paths(
        &mut self,
        paths: &crate::StorePathSet,
        maybe_substitute: crate::SubstituteFlag,
    ) -> Result<crate::StorePathSet, crate::Error> {
        let mut store = self.store.lock().await;
        store.query_valid_paths(paths, maybe_substitute).await
    }

    async fn query_path_info(
        &mut self,
        path: &crate::StorePath,
    ) -> Result<Option<crate::ValidPathInfo>, crate::Error> {
        let mut store = self.store.lock().await;
        store.query_path_info(path).await
    }

    async fn nar_from_path<W: tokio::io::AsyncWrite + Send + Unpin>(
        &mut self,
        path: &crate::StorePath,
        sink: W,
    ) -> Result<(), crate::Error> {
        let mut store = self.store.lock().await;
        store.nar_from_path(path, sink).await
    }

    async fn add_to_store<R: tokio::io::AsyncRead + Send + Unpin>(
        &mut self,
        info: &crate::ValidPathInfo,
        source: R,
        repair: crate::RepairFlag,
        check_sigs: crate::CheckSignaturesFlag,
    ) -> Result<(), crate::Error> {
        let mut store = self.store.lock().await;
        store.add_to_store(info, source, repair, check_sigs).await
    }

    async fn build_derivation<W: tokio::io::AsyncWrite + Send + Unpin>(
        &mut self,
        drv_path: &crate::StorePath,
        drv: &crate::BasicDerivation,
        settings: &crate::BuildSettings,
        build_log: W,
    ) -> Result<crate::BuildResult, crate::Error> {
        let mut store = self.store.lock().await;
        store.build_derivation(drv_path, drv, settings, build_log).await
    }

    async fn build_paths<W: tokio::io::AsyncWrite + Send + Unpin>(
        &mut self,
        drv_paths: &[crate::DerivedPath],
        settings: &crate::BuildSettings,
        build_log: W,
    ) -> Result<(), crate::Error> {
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
        paths: &crate::StorePathSet,
        lock: bool,
        maybe_substitute: crate::SubstituteFlag,
    ) -> Result<crate::StorePathSet, crate::Error> {
        let mut store = self.store.lock().await;
        store.query_valid_paths_locked(paths, lock, maybe_substitute).await
    }

    async fn export_paths<W: tokio::io::AsyncWrite + Send + Unpin>(
        &mut self,
        paths: &crate::StorePathSet,
        sink: W,
    ) -> Result<(), crate::Error> {
        let mut store = self.store.lock().await;
        store.export_paths(paths, sink).await
    }

    async fn import_paths<R: tokio::io::AsyncRead + Send + Unpin>(
        &mut self,
        source: R,
    ) -> Result<(), crate::Error> {
        let mut store = self.store.lock().await;
        store.import_paths(source).await
    }

    async fn query_closure(
        &mut self,
        paths: &crate::StorePathSet,
        include_outputs: bool,
    ) -> Result<crate::StorePathSet, crate::Error> {
        let mut store = self.store.lock().await;
        store.query_closure(paths, include_outputs).await
    }
}
