use async_trait::async_trait;
#[cfg(feature = "compress-tools")]
use compress_tools::tokio_support::uncompress_data;
#[cfg(feature = "compress-tools")]
use futures::TryFutureExt;
use tokio::io::{AsyncRead, AsyncWrite};
#[cfg(feature = "compress-tools")]
use tokio::try_join;

use crate::path_info::{Compression, NarInfo, ValidPathInfo};
use crate::store::{CheckSignaturesFlag, Error, RepairFlag, Store};
use crate::store_path::{StoreDir, StoreDirProvider, StorePath};

use super::BinaryCache;

fn nar_info_file_for(path: &StorePath) -> String {
    format!("{}.narinfo", path.hash)
}

#[derive(Clone)]
pub struct BinaryStoreWrap<B> {
    cache: B,
}

impl<B> BinaryStoreWrap<B>
where
    B: BinaryCache + Send + Sync,
{
    pub fn new(cache: B) -> Self {
        Self { cache }
    }
    pub async fn nar_info_for_path(&self, path: &StorePath) -> Result<Option<NarInfo>, Error> {
        let file = nar_info_file_for(path);
        if !self.cache.file_exists(&file).await? {
            return Ok(None);
        }
        let mut buf = Vec::new();
        self.cache.get_file(&file, &mut buf).await?;
        let s = String::from_utf8(buf).map_err(|_| Error::BadNarInfo)?;
        let info = NarInfo::parse(&self.store_dir(), &s).map_err(|_| Error::BadNarInfo)?;
        Ok(Some(info))
    }
}

impl<B: StoreDirProvider> StoreDirProvider for BinaryStoreWrap<B> {
    fn store_dir(&self) -> StoreDir {
        self.cache.store_dir()
    }
}

#[async_trait]
impl<B> Store for BinaryStoreWrap<B>
where
    B: BinaryCache + Send + Sync,
{
    async fn query_path_info(&mut self, path: &StorePath) -> Result<Option<ValidPathInfo>, Error> {
        if let Some(nar_info) = self.nar_info_for_path(path).await? {
            Ok(Some(nar_info.path_info))
        } else {
            Ok(None)
        }
    }

    /// Export path from the store
    async fn nar_from_path<W: AsyncWrite + Send + Unpin>(
        &mut self,
        path: &StorePath,
        sink: W,
    ) -> Result<(), Error> {
        if let Some(nar_info) = self.nar_info_for_path(path).await? {
            match nar_info.compression {
                Compression::None => self.cache.get_file(&nar_info.url, sink).await,
                Compression::Unknown(_) | Compression::BR => {
                    Err(Error::UnsupportedCompression(nar_info.compression))
                }
                #[cfg(not(feature = "compress-tools"))]
                _ => Err(Error::UnsupportedCompression(nar_info.compression)),
                #[cfg(feature = "compress-tools")]
                _ => {
                    let (read, write) = tokio::io::duplex(64_000);
                    let fut1 = uncompress_data(read, sink).map_err(Error::from);
                    let fut2 = self.cache.get_file(&nar_info.url, write);
                    try_join!(fut1, fut2)?;
                    Ok(())
                }
            }
        } else {
            Err(Error::InvalidPath(path.to_string()))
        }
    }

    /// Import a path into the store.
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

#[cfg(test)]
mod tests {
    #[cfg(feature = "compress-tools")]
    use crate::hash::{Algorithm, HashSink};

    use crate::store::binary_cache::file::FileBinaryCache;
    use crate::store_path::StorePath;

    use super::*;

    #[tokio::test]

    async fn test_info_missing() {
        let path = StorePath::new_from_base_name("7rjj86a15146cq1d3qy068lml7n8ykzm-plakker-12.3.0")
            .unwrap();
        let mut store = BinaryStoreWrap::new(FileBinaryCache::new("test-data/binary-cache"));
        let info = store.query_path_info(&path).await.unwrap();
        assert_eq!(None, info);
    }

    #[tokio::test]
    async fn test_info_gcc() {
        let path =
            StorePath::new_from_base_name("7rjj86a15146cq1d3qy068lml7n7ykzm-gcc-wrapper-12.3.0")
                .unwrap();
        let mut store = BinaryStoreWrap::new(FileBinaryCache::new("test-data/binary-cache"));
        let info = store.query_path_info(&path).await.unwrap().unwrap();
        assert_eq!(info.path, path);
    }

    #[cfg(feature = "compress-tools")]
    #[tokio::test]
    async fn test_nar_from_path_gcc() {
        let path =
            StorePath::new_from_base_name("7rjj86a15146cq1d3qy068lml7n7ykzm-gcc-wrapper-12.3.0")
                .unwrap();
        let mut store = BinaryStoreWrap::new(FileBinaryCache::new("test-data/binary-cache"));
        let info = store.query_path_info(&path).await.unwrap().unwrap();

        let mut sink = HashSink::new(Algorithm::SHA256);
        store.nar_from_path(&path, &mut sink).await.unwrap();
        assert_eq!((info.nar_size, info.nar_hash), sink.finish());
    }

    #[cfg(feature = "compress-tools")]
    #[tokio::test]
    async fn test_nar_from_path_hello() {
        let path =
            StorePath::new_from_base_name("ycbqd7822qcnasaqy0mmiv2j9n9m62yl-hello-2.12.1").unwrap();
        let mut store = BinaryStoreWrap::new(FileBinaryCache::new("test-data/binary-cache"));
        let info = store.query_path_info(&path).await.unwrap().unwrap();

        let mut sink = HashSink::new(Algorithm::SHA256);
        store.nar_from_path(&path, &mut sink).await.unwrap();
        assert_eq!((info.nar_size, info.nar_hash), sink.finish());
    }
}
