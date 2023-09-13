use std::time::{Instant, Duration};

use async_trait::async_trait;
use caches::{LRUCache, RawLRU, lru::CacheError, Cache};
use lazy_static::lazy_static;
use nixrs_util::io::StatePrint;

use crate::{StoreDir, StorePath, ValidPathInfo, Store};

lazy_static! {
    static ref TTL_POSITIVE_NAR_INFO_CACHE : Duration = Duration::from_secs(30 * 24 * 3600);
    static ref TTL_NEGATIVE_NAR_INFO_CACHE : Duration = Duration::from_secs(3600);
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
        let duration : Duration  = if self.value.is_some() {
            *TTL_POSITIVE_NAR_INFO_CACHE
        } else {
            *TTL_NEGATIVE_NAR_INFO_CACHE
        };
        return self.time_point.elapsed() < duration;
    }
}

pub struct CachedStore<S> {
    store_dir: StoreDir,
    store: S,
    cache: RawLRU<StorePath, PathInfoCacheValue>,
    cache_valid_path: bool,
}

impl<S: Store> CachedStore<S> {
    pub fn new(store: S) -> Result<CachedStore<S>, CacheError> {
        Self::with_size(store, 65536)
    }

    pub fn with_size(store: S, lru_size: usize) -> Result<CachedStore<S>, CacheError> {
        Ok(CachedStore {
            store_dir: store.store_dir(),
            store,
            cache: LRUCache::new(lru_size)?,
            cache_valid_path: false,
        })
    }
}


#[async_trait(?Send)]
impl<S> Store for CachedStore<S>
where
    S: Store,
{
    fn store_dir(&self) -> StoreDir {
        self.store_dir.clone()
    }

    async fn query_valid_paths(
        &mut self,
        paths: &crate::StorePathSet,
        maybe_substitute: crate::SubstituteFlag,
    ) -> Result<crate::StorePathSet, crate::Error> {
        self.store.query_valid_paths(paths, maybe_substitute).await
    }

    async fn add_temp_root(&self, path: &crate::StorePath) {
        self.store.add_temp_root(path).await
    }

    async fn query_path_info(
        &mut self,
        path: &crate::StorePath,
    ) -> Result<crate::ValidPathInfo, crate::Error> {

        if let Some(cache) = self.cache.get(path) {
            if cache.is_known_now() {
                if let Some(value) = cache.value.as_ref() {
                    return Ok(value.clone());
                } else {
                    return Err(crate::Error::InvalidPath(self.store_dir.print(path)));
                }
            } else {
                self.cache.remove(path);
            }
        }
        match self.store.query_path_info(path).await {
            Ok(info) => {
                self.cache.put(path.clone(), PathInfoCacheValue::valid_path(info.clone()));
                Ok(info)
            },
            Err(err @ crate::Error::InvalidPath(_)) => {
                self.cache.put(path.clone(), PathInfoCacheValue::invalid_path());
                Err(err)
            }
            Err(err) => Err(err),
        }
    }

    async fn nar_from_path<W: tokio::io::AsyncWrite + Unpin>(
        &mut self,
        path: &crate::StorePath,
        sink: W,
    ) -> Result<(), crate::Error> {
        self.store.nar_from_path(path, sink).await
    }

    async fn export_paths<W: tokio::io::AsyncWrite + Unpin>(
        &mut self,
        paths: &crate::StorePathSet,
        sink: W,
    ) -> Result<(), crate::Error> {
        self.store.export_paths(paths, sink).await
    }

    async fn import_paths<R: tokio::io::AsyncRead + Unpin>(
        &mut self,
        source: R,
    ) -> Result<(), crate::Error> {
        self.store.import_paths(source).await
    }

    async fn build_derivation<W: tokio::io::AsyncWrite + Unpin>(
        &mut self,
        drv_path: &crate::StorePath,
        drv: &crate::BasicDerivation,
        settings: &crate::BuildSettings,
        build_log: W,
    ) -> Result<crate::BuildResult, crate::Error> {
        self.store.build_derivation(drv_path, drv, settings, build_log).await
    }

    async fn build_paths<W: tokio::io::AsyncWrite + Unpin>(
        &mut self,
        drv_paths: &[crate::DerivedPath],
        settings: &crate::BuildSettings,
        build_log: W,
    ) -> Result<(), crate::Error> {
        self.store.build_paths(drv_paths, settings, build_log).await
    }

    async fn add_to_store<R: tokio::io::AsyncRead + Unpin>(
        &mut self,
        info: &crate::ValidPathInfo,
        source: R,
        repair: crate::RepairFlag,
        check_sigs: crate::CheckSignaturesFlag,
    ) -> Result<(), crate::Error> {
        self.store.add_to_store(info, source, repair, check_sigs).await
    }

    async fn query_closure(
        &mut self,
        paths: &crate::StorePathSet,
        include_outputs: bool,
    ) -> Result<crate::StorePathSet, crate::Error> {
        self.store.query_closure(paths, include_outputs).await
    }

    async fn legacy_query_valid_paths(
        &mut self,
        paths: &crate::StorePathSet,
        lock: bool,
        maybe_substitute: crate::SubstituteFlag,
    ) -> Result<crate::StorePathSet, crate::Error> {
        self.store
            .legacy_query_valid_paths(paths, lock, maybe_substitute)
            .await
    }

    async fn is_valid_path(&mut self, path: &crate::StorePath) -> Result<bool, crate::Error> {
        if let Some(cache) = self.cache.get(path) {
            if cache.is_known_now() {
                return Ok(cache.value.is_some());
            } else {
                self.cache.remove(path);
            }
        }
        if self.cache_valid_path {
            match self.query_path_info(path).await {
                Ok(_) => Ok(true),
                Err(crate::Error::InvalidPath(_)) => Ok(false),
                Err(err) => Err(err),
            }
        } else {
            self.store.is_valid_path(path).await
        }
    }
}