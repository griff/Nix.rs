use std::borrow::Cow;
use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use futures::Stream;
use nixrs_legacy::store::binary_cache::{BinaryStoreWrap, HttpBinaryCache};
use nixrs_legacy::store::{
    CheckSignaturesFlag, RepairFlag, Store, compute_fs_closure, copy_paths, copy_store_path,
};
use nixrs_legacy::store_path::{StoreDirProvider, StorePath};
use nixrs_legacy::store_paths;
use tokio::sync::Mutex;
use tvix_castore::blobservice::BlobService;
use tvix_castore::proto as castorepb;
use tvix_castore::{Error, directoryservice::DirectoryService};
use tvix_store::pathinfoservice::from_addr;
use tvix_store::{pathinfoservice::PathInfoService, proto::PathInfo};

use crate::store::TvixStore;

struct Stores {
    recursive: bool,
    nar_store: BinaryStoreWrap<HttpBinaryCache>,
    tvix_store: TvixStore,
}

impl Stores {
    async fn copy_digest(&mut self, digest: [u8; 20]) -> Result<bool, nixrs_legacy::store::Error> {
        let store_path = StorePath::from_parts(digest, "x")?;
        if let Some(info) = self.nar_store.query_path_info(&store_path).await? {
            if self.recursive {
                let start_paths = store_paths![info.path.clone()];
                let store_paths =
                    compute_fs_closure(self.nar_store.clone(), start_paths, false).await?;
                let p: Vec<String> = store_paths.iter().map(|p| p.to_string()).collect();
                info!("Copying paths for {}: {}", info.path, p[..].join(", "));
                copy_paths(&mut self.nar_store, &mut self.tvix_store, &store_paths).await?;
            } else {
                info!("Copying path {}", info.path);
                copy_store_path(
                    &mut self.nar_store,
                    &mut self.tvix_store,
                    &info.path,
                    RepairFlag::NoRepair,
                    CheckSignaturesFlag::NoCheckSigs,
                )
                .await?;
            }
            Ok(true)
        } else {
            Ok(false)
        }
    }
}

pub struct SubstitutePathInfoService {
    stores: Arc<Mutex<Stores>>,
    local: Arc<dyn PathInfoService>,
}

#[async_trait]
impl PathInfoService for SubstitutePathInfoService {
    fn from_url(
        url: &url::Url,
        blob_service: Arc<dyn BlobService>,
        directory_service: Arc<dyn DirectoryService>,
    ) -> Result<Self, Error>
    where
        Self: Sized,
    {
        match url.scheme().strip_prefix("sub+") {
            None => Err(Error::StorageError("invalid scheme".to_string())),
            Some(_) => {
                let mut base_url = Cow::Borrowed("https://cache.nixos.org");
                let mut recursive = false;
                for (key, value) in url.query_pairs() {
                    if key == "substituter" {
                        base_url = value;
                    }
                    if key == "recursive" {
                        recursive = true;
                    }
                }
                let url_str = url.to_string();
                let uri = url_str.strip_prefix("sub+").unwrap();
                let cache = HttpBinaryCache::new(base_url.as_ref())
                    .map_err(|err| Error::StorageError(format!("binary cache error {}", err)))?;
                let nar_store = BinaryStoreWrap::new(cache);
                let local = from_addr(uri, blob_service.clone(), directory_service.clone())?;
                let tvix_store = TvixStore {
                    store_dir: nar_store.store_dir(),
                    blob_service,
                    directory_service,
                    path_info_service: local.clone(),
                };
                let stores = Stores {
                    recursive,
                    nar_store,
                    tvix_store,
                };
                let stores = Arc::new(Mutex::new(stores));
                Ok(SubstitutePathInfoService { local, stores })
            }
        }
    }

    async fn get(&self, digest: [u8; 20]) -> Result<Option<PathInfo>, Error> {
        if let Some(info) = self.local.get(digest).await? {
            return Ok(Some(info));
        }
        let mut stores = self.stores.lock().await;
        if stores
            .copy_digest(digest)
            .await
            .map_err(|err| Error::StorageError(format!("copy error {}", err)))?
        {
            info!("Copy done");
            self.local.get(digest).await
        } else {
            warn!("Not found");
            Ok(None)
        }
    }

    async fn put(&self, path_info: PathInfo) -> Result<PathInfo, Error> {
        self.local.put(path_info).await
    }

    async fn calculate_nar(
        &self,
        root_node: &castorepb::node::Node,
    ) -> Result<(u64, [u8; 32]), Error> {
        self.local.calculate_nar(root_node).await
    }

    fn list(&self) -> Pin<Box<dyn Stream<Item = Result<PathInfo, Error>> + Send>> {
        self.local.list()
    }
}
