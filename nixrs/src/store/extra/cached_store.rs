use std::time::{Duration, Instant};

use async_trait::async_trait;
use caches::{lru::CacheError, Cache, LRUCache, RawLRU};
use lazy_static::lazy_static;

use crate::store::{
    legacy_worker::LegacyStore, Store, StoreDir, StoreDirProvider, StorePath, ValidPathInfo,
};

lazy_static! {
    static ref TTL_POSITIVE_NAR_INFO_CACHE: Duration = Duration::from_secs(30 * 24 * 3600);
    static ref TTL_NEGATIVE_NAR_INFO_CACHE: Duration = Duration::from_secs(3600);
}

struct PathInfoCacheValue {
    value: Option<ValidPathInfo>,
    time_point: Instant,
}

impl PathInfoCacheValue {
    fn valid_path(info: ValidPathInfo) -> PathInfoCacheValue {
        PathInfoCacheValue {
            value: Some(info),
            time_point: Instant::now(),
        }
    }

    fn invalid_path() -> PathInfoCacheValue {
        PathInfoCacheValue {
            value: None,
            time_point: Instant::now(),
        }
    }

    fn is_known_now(&self) -> bool {
        let duration: Duration = if self.value.is_some() {
            *TTL_POSITIVE_NAR_INFO_CACHE
        } else {
            *TTL_NEGATIVE_NAR_INFO_CACHE
        };
        return self.time_point.elapsed() < duration;
    }
}

pub struct CachedStore<S> {
    store: S,
    cache: RawLRU<StorePath, PathInfoCacheValue>,
}

impl<S> CachedStore<S> {
    pub fn new(store: S) -> Result<CachedStore<S>, CacheError> {
        Self::with_size(store, 65536)
    }

    pub fn with_size(store: S, lru_size: usize) -> Result<CachedStore<S>, CacheError> {
        Ok(CachedStore {
            store,
            cache: LRUCache::new(lru_size)?,
        })
    }
}

impl<S: StoreDirProvider> StoreDirProvider for CachedStore<S> {
    fn store_dir(&self) -> StoreDir {
        self.store.store_dir()
    }
}

#[async_trait]
impl<S> Store for CachedStore<S>
where
    S: Store + Send,
{
    async fn query_valid_paths(
        &mut self,
        paths: &crate::store::StorePathSet,
        maybe_substitute: crate::store::SubstituteFlag,
    ) -> Result<crate::store::StorePathSet, crate::store::Error> {
        self.store.query_valid_paths(paths, maybe_substitute).await
    }

    async fn query_path_info(
        &mut self,
        path: &crate::store::StorePath,
    ) -> Result<Option<crate::store::ValidPathInfo>, crate::store::Error> {
        if let Some(cache) = self.cache.get(path) {
            if cache.is_known_now() {
                if let Some(value) = cache.value.as_ref() {
                    return Ok(Some(value.clone()));
                } else {
                    return Ok(None);
                }
            } else {
                self.cache.remove(path);
            }
        }
        match self.store.query_path_info(path).await {
            Ok(Some(info)) => {
                self.cache
                    .put(path.clone(), PathInfoCacheValue::valid_path(info.clone()));
                Ok(Some(info))
            }
            Ok(None) => {
                self.cache
                    .put(path.clone(), PathInfoCacheValue::invalid_path());
                Ok(None)
            }
            Err(err) => Err(err),
        }
    }

    async fn nar_from_path<W: tokio::io::AsyncWrite + Send + Unpin>(
        &mut self,
        path: &crate::store::StorePath,
        sink: W,
    ) -> Result<(), crate::store::Error> {
        self.store.nar_from_path(path, sink).await
    }

    async fn add_to_store<R: tokio::io::AsyncRead + Send + Unpin>(
        &mut self,
        info: &crate::store::ValidPathInfo,
        source: R,
        repair: crate::store::RepairFlag,
        check_sigs: crate::store::CheckSignaturesFlag,
    ) -> Result<(), crate::store::Error> {
        self.store
            .add_to_store(info, source, repair, check_sigs)
            .await
    }

    async fn build_derivation<W: tokio::io::AsyncWrite + Send + Unpin>(
        &mut self,
        drv_path: &crate::store::StorePath,
        drv: &crate::store::BasicDerivation,
        settings: &crate::store::BuildSettings,
        build_log: W,
    ) -> Result<crate::store::BuildResult, crate::store::Error> {
        self.store
            .build_derivation(drv_path, drv, settings, build_log)
            .await
    }

    async fn build_paths<W: tokio::io::AsyncWrite + Send + Unpin>(
        &mut self,
        drv_paths: &[crate::store::DerivedPath],
        settings: &crate::store::BuildSettings,
        build_log: W,
    ) -> Result<(), crate::store::Error> {
        self.store.build_paths(drv_paths, settings, build_log).await
    }
}

#[async_trait]
impl<S> LegacyStore for CachedStore<S>
where
    S: LegacyStore + Send,
{
    async fn query_valid_paths_locked(
        &mut self,
        paths: &crate::store::StorePathSet,
        lock: bool,
        maybe_substitute: crate::store::SubstituteFlag,
    ) -> Result<crate::store::StorePathSet, crate::store::Error> {
        self.store
            .query_valid_paths_locked(paths, lock, maybe_substitute)
            .await
    }

    async fn export_paths<W: tokio::io::AsyncWrite + Send + Unpin>(
        &mut self,
        paths: &crate::store::StorePathSet,
        sink: W,
    ) -> Result<(), crate::store::Error> {
        self.store.export_paths(paths, sink).await
    }

    async fn import_paths<R: tokio::io::AsyncRead + Send + Unpin>(
        &mut self,
        source: R,
    ) -> Result<(), crate::store::Error> {
        self.store.import_paths(source).await
    }

    async fn query_closure(
        &mut self,
        paths: &crate::store::StorePathSet,
        include_outputs: bool,
    ) -> Result<crate::store::StorePathSet, crate::store::Error> {
        self.store.query_closure(paths, include_outputs).await
    }
}
