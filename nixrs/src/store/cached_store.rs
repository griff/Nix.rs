use std::fmt;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use caches::{lru::CacheError, Cache, LRUCache, RawLRU};
use lazy_static::lazy_static;
use tokio::io::{AsyncRead, AsyncWrite};

use crate::path_info::ValidPathInfo;
use crate::store::legacy_worker::LegacyStore;
use crate::store::{
    BasicDerivation, BuildResult, CheckSignaturesFlag, DerivedPath, Error,
    RepairFlag, Store, SubstituteFlag,
};
use crate::store_path::{StoreDir, StoreDirProvider, StorePath, StorePathSet};

use super::store_api::BuildMode;

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
        paths: &StorePathSet,
        maybe_substitute: SubstituteFlag,
    ) -> Result<StorePathSet, Error> {
        self.store.query_valid_paths(paths, maybe_substitute).await
    }

    async fn query_path_info(&mut self, path: &StorePath) -> Result<Option<ValidPathInfo>, Error> {
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

    async fn nar_from_path<W: AsyncWrite + fmt::Debug + Send + Unpin>(
        &mut self,
        path: &StorePath,
        sink: W,
    ) -> Result<(), Error> {
        self.store.nar_from_path(path, sink).await
    }

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
        self.store
            .build_derivation(drv_path, drv, build_mode)
            .await
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
impl<S> LegacyStore for CachedStore<S>
where
    S: LegacyStore + Send,
{
    async fn query_valid_paths_locked(
        &mut self,
        paths: &StorePathSet,
        lock: bool,
        maybe_substitute: SubstituteFlag,
    ) -> Result<StorePathSet, Error> {
        self.store
            .query_valid_paths_locked(paths, lock, maybe_substitute)
            .await
    }

    async fn export_paths<W: AsyncWrite + fmt::Debug + Send + Unpin>(
        &mut self,
        paths: &StorePathSet,
        sink: W,
    ) -> Result<(), Error> {
        self.store.export_paths(paths, sink).await
    }

    async fn import_paths<R: AsyncRead + fmt::Debug + Send + Unpin>(&mut self, source: R) -> Result<(), Error> {
        self.store.import_paths(source).await
    }

    async fn query_closure(
        &mut self,
        paths: &StorePathSet,
        include_outputs: bool,
    ) -> Result<StorePathSet, Error> {
        self.store.query_closure(paths, include_outputs).await
    }
}
