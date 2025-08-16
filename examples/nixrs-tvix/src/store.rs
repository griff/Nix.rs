use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use futures::{SinkExt, TryStreamExt};
use nixrs_legacy::archive::{NAREncoder, parse_nar};
use nixrs_legacy::path_info::ValidPathInfo;
use nixrs_legacy::store::{CheckSignaturesFlag, Error, RepairFlag, Store};
use nixrs_legacy::store_path::{StoreDir, StoreDirProvider, StorePath};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::pin;
use tokio_util::codec::FramedWrite;
use tvix_castore::blobservice::BlobService;
use tvix_castore::directoryservice::DirectoryService;
use tvix_store::pathinfoservice::PathInfoService;

use crate::nar::{nar_source, store_nar};
use crate::path_info::{path_info_from_valid_path_info, valid_path_info_from_path_info};

#[derive(Clone)]
pub struct TvixStore {
    pub store_dir: StoreDir,
    pub blob_service: Arc<dyn BlobService>,
    pub directory_service: Arc<dyn DirectoryService>,
    pub path_info_service: Arc<dyn PathInfoService>,
}

impl fmt::Debug for TvixStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TvixStore")
            .field("store_dir", &self.store_dir)
            .finish()
    }
}

impl StoreDirProvider for TvixStore {
    fn store_dir(&self) -> StoreDir {
        self.store_dir.clone()
    }
}

#[async_trait]
impl Store for TvixStore {
    async fn query_path_info(&mut self, path: &StorePath) -> Result<Option<ValidPathInfo>, Error> {
        tracing::debug!("query_path_info {}", path);
        let path = path.clone();
        let path_service = self.path_info_service.clone();
        let info = path_service
            .get(path.hash.into())
            .await
            .map_err(|err| Error::Misc(format!("tvix store error {:?}", err)))?;
        if let Some(path_info) = info {
            tracing::trace!("found path {:?}", path_info);
            let ret = valid_path_info_from_path_info(path_info)?;
            Ok(Some(ret))
        } else {
            tracing::trace!("found no path");
            Ok(None)
        }
    }
    async fn nar_from_path<W: AsyncWrite + Send + Unpin>(
        &mut self,
        path: &StorePath,
        sink: W,
    ) -> Result<(), Error> {
        tracing::debug!("nar_from_path {}", path);
        let path = path.clone();
        let path_service = self.path_info_service.clone();
        let info = path_service
            .get(path.hash.into())
            .await
            .map_err(|err| Error::Misc(format!("tvix store error {:?}", err)))?;
        if let Some(info) = info {
            let s = nar_source(
                self.blob_service.clone(),
                self.directory_service.clone(),
                info.node
                    .ok_or_else(|| Error::InvalidPath(path.to_string()))?
                    .node
                    .ok_or_else(|| Error::InvalidPath(path.to_string()))?,
            );
            let mut framed = FramedWrite::new(sink, NAREncoder);
            pin!(s);
            framed.send_all(&mut s).await?;
            Ok(())
        } else {
            Err(Error::InvalidPath(path.to_string()))
        }
    }

    /// Import a path into the store.
    async fn add_to_store<R: AsyncRead + Send + Unpin>(
        &mut self,
        info: &ValidPathInfo,
        source: R,
        _repair: RepairFlag,
        _check_sigs: CheckSignaturesFlag,
    ) -> Result<(), Error> {
        tracing::debug!("add_to_store {}", info.path);
        let p = parse_nar(source).err_into();
        let node = store_nar(self.blob_service.clone(), self.directory_service.clone(), p)
            .await
            .map_err(|err| Error::Misc(format!("tvix store error {:?}", err)))?;
        let path_info = path_info_from_valid_path_info(info, node);
        let path_service = self.path_info_service.clone();
        path_service
            .put(path_info)
            .await
            .map_err(|err| Error::Misc(format!("tvix store error {:?}", err)))?;
        Ok(())
    }
}
