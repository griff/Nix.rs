use async_trait::async_trait;
use reqwest::{Client, Url, StatusCode, header::CONTENT_TYPE, IntoUrl};
use tokio::io::{AsyncRead, AsyncWrite, AsyncReadExt, AsyncWriteExt};

use crate::{StoreDir, StoreDirProvider, Error};
use super::BinaryCache;

pub struct HTTPBinaryCache {
    store_dir: StoreDir,
    client: Client,
    base_url: Url,
}

impl HTTPBinaryCache {
    pub fn new<U: IntoUrl>(url: U) -> Result<HTTPBinaryCache, Error> {
        let store_dir = Default::default();
        Self::with_store(url, store_dir)
    }

    pub fn with_store<U: IntoUrl>(url: U, store_dir: StoreDir) -> Result<HTTPBinaryCache, Error> {
        let client = reqwest::Client::builder().build()?;
        let base_url = url.into_url()?;
        Ok(HTTPBinaryCache {
            store_dir, client, base_url
        })
    }
}

impl StoreDirProvider for HTTPBinaryCache {
    fn store_dir(&self) -> StoreDir {
        self.store_dir.clone()
    }
}

#[async_trait]
impl BinaryCache for HTTPBinaryCache {
    async fn file_exists(&self, path: &str) -> Result<bool, Error> {
        let url = self.base_url.join(path)?;
        let resp = self.client.head(url).send().await?;
        let status = resp.status();
        if status.is_success() {
            Ok(true)
        } else if status == StatusCode::NOT_FOUND || status == StatusCode::FORBIDDEN {
            Ok(false)
        } else {
            Err(resp.error_for_status().unwrap_err().into())
        }
    }
    async fn upsert_file<R>(&self, path: &str, mut stream: R, mime_type: &str) -> Result<(), Error>
        where R: AsyncRead + Send + Unpin
    {
        let mut content = Vec::new();
        stream.read_to_end(&mut content).await?;
        let url = self.base_url.join(path)?;
        let resp = self.client
            .put(url)
            .body(content)
            .header(CONTENT_TYPE, mime_type)
            .send().await?;
        if resp.status().is_success() {
            Ok(())
        } else {
            Err(resp.error_for_status().unwrap_err().into())
        }
    }

    /// Dump the contents of the specified file to a sink.
    async fn get_file<W>(&self, path: &str, mut sink: W) -> Result<(), Error>
        where W: AsyncWrite + Send + Unpin
    {
        let url = self.base_url.join(path)?;
        let mut resp = self.client
            .get(url)
            .send().await?;
        if resp.status().is_success() {
            while let Some(chunk) = resp.chunk().await? {
                sink.write_all(&chunk).await?;
            }
            Ok(())
        } else {
            Err(resp.error_for_status().unwrap_err().into())
        }
    }
}


#[cfg(test)]
mod tests {
    use nixrs_util::hash::{HashSink, Algorithm};

    use crate::{StorePath, Store};
    use crate::binary_cache::BinaryStoreWrap;

    use super::*;

    #[tokio::test]

    async fn test_info_missing() {
        let path = StorePath::new_from_base_name("7rjj86a15146cq1d3qy068lml7n8ykzm-plakker-12.3.0").unwrap();
        let mut store = BinaryStoreWrap::new(HTTPBinaryCache::new("https://cache.nixos.org").unwrap());
        let info = store.query_path_info(&path).await.unwrap();
        assert_eq!(None, info);
    }

    #[tokio::test]
    async fn test_info_gcc() {
        let path = StorePath::new_from_base_name("7rjj86a15146cq1d3qy068lml7n7ykzm-gcc-wrapper-12.3.0").unwrap();
        let mut store = BinaryStoreWrap::new(HTTPBinaryCache::new("https://cache.nixos.org").unwrap());
        let info = store.query_path_info(&path).await.unwrap().unwrap();
        assert_eq!(info.path, path);
    }

    #[tokio::test]
    async fn test_nar_from_path_gcc() {
        let path = StorePath::new_from_base_name("7rjj86a15146cq1d3qy068lml7n7ykzm-gcc-wrapper-12.3.0").unwrap();
        let mut store = BinaryStoreWrap::new(HTTPBinaryCache::new("https://cache.nixos.org").unwrap());
        let info = store.query_path_info(&path).await.unwrap().unwrap();

        let mut sink = HashSink::new(Algorithm::SHA256);
        store.nar_from_path(&path, &mut sink).await.unwrap();
        assert_eq!((info.nar_size, info.nar_hash), sink.finish());
    }

    #[tokio::test]
    async fn test_nar_from_path_hello() {
        let path = StorePath::new_from_base_name("ycbqd7822qcnasaqy0mmiv2j9n9m62yl-hello-2.12.1").unwrap();
        let mut store = BinaryStoreWrap::new(HTTPBinaryCache::new("https://cache.nixos.org").unwrap());
        let info = store.query_path_info(&path).await.unwrap().unwrap();

        let mut sink = HashSink::new(Algorithm::SHA256);
        store.nar_from_path(&path, &mut sink).await.unwrap();
        assert_eq!((info.nar_size, info.nar_hash), sink.finish());
    }
}