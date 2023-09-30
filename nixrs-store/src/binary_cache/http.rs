use async_trait::async_trait;
use reqwest::{header::CONTENT_TYPE, Client, IntoUrl, StatusCode, Url};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use super::BinaryCache;
use crate::{Error, StoreDir, StoreDirProvider};

#[derive(Clone, Debug)]
pub struct HttpBinaryCache {
    store_dir: StoreDir,
    client: Client,
    base_url: Url,
}

impl HttpBinaryCache {
    pub fn new<U: IntoUrl>(url: U) -> Result<HttpBinaryCache, Error> {
        let store_dir = Default::default();
        Self::with_store(url, store_dir)
    }

    pub fn with_store<U: IntoUrl>(url: U, store_dir: StoreDir) -> Result<HttpBinaryCache, Error> {
        let client = reqwest::Client::builder().build()?;
        let base_url = url.into_url()?;
        Ok(HttpBinaryCache {
            store_dir,
            client,
            base_url,
        })
    }
}

impl StoreDirProvider for HttpBinaryCache {
    fn store_dir(&self) -> StoreDir {
        self.store_dir.clone()
    }
}

#[async_trait]
impl BinaryCache for HttpBinaryCache {
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
    where
        R: AsyncRead + Send + Unpin,
    {
        let mut content = Vec::new();
        stream.read_to_end(&mut content).await?;
        let url = self.base_url.join(path)?;
        let resp = self
            .client
            .put(url)
            .body(content)
            .header(CONTENT_TYPE, mime_type)
            .send()
            .await?;
        if resp.status().is_success() {
            Ok(())
        } else {
            Err(resp.error_for_status().unwrap_err().into())
        }
    }

    /// Dump the contents of the specified file to a sink.
    async fn get_file<W>(&self, path: &str, mut sink: W) -> Result<(), Error>
    where
        W: AsyncWrite + Send + Unpin,
    {
        let url = self.base_url.join(path)?;
        let mut resp = self.client.get(url).send().await?;
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
    use nixrs_util::hash::{Algorithm, HashSink};

    use crate::binary_cache::BinaryStoreWrap;
    use crate::{Store, StorePath};

    use super::*;

    #[tokio::test]

    async fn test_info_missing() {
        let path = StorePath::new_from_base_name("7rjj86a15146cq1d3qy068lml7n8ykzm-plakker-12.3.0")
            .unwrap();
        let mut store =
            BinaryStoreWrap::new(HttpBinaryCache::new("https://cache.nixos.org").unwrap());
        let info = store.query_path_info(&path).await.unwrap();
        assert_eq!(None, info);
    }

    #[tokio::test]
    async fn test_info_gcc() {
        let path =
            StorePath::new_from_base_name("7rjj86a15146cq1d3qy068lml7n7ykzm-gcc-wrapper-12.3.0")
                .unwrap();
        let mut store =
            BinaryStoreWrap::new(HttpBinaryCache::new("https://cache.nixos.org").unwrap());
        let info = store.query_path_info(&path).await.unwrap().unwrap();
        assert_eq!(info.path, path);
    }

    #[tokio::test]
    async fn test_nar_from_path_gcc() {
        let path =
            StorePath::new_from_base_name("7rjj86a15146cq1d3qy068lml7n7ykzm-gcc-wrapper-12.3.0")
                .unwrap();
        let mut store =
            BinaryStoreWrap::new(HttpBinaryCache::new("https://cache.nixos.org").unwrap());
        let info = store.query_path_info(&path).await.unwrap().unwrap();

        let mut sink = HashSink::new(Algorithm::SHA256);
        store.nar_from_path(&path, &mut sink).await.unwrap();
        assert_eq!((info.nar_size, info.nar_hash), sink.finish());
    }

    #[tokio::test]
    async fn test_nar_from_path_hello() {
        let path =
            StorePath::new_from_base_name("ycbqd7822qcnasaqy0mmiv2j9n9m62yl-hello-2.12.1").unwrap();
        let mut store =
            BinaryStoreWrap::new(HttpBinaryCache::new("https://cache.nixos.org").unwrap());
        let info = store.query_path_info(&path).await.unwrap().unwrap();

        let mut sink = HashSink::new(Algorithm::SHA256);
        store.nar_from_path(&path, &mut sink).await.unwrap();
        assert_eq!((info.nar_size, info.nar_hash), sink.finish());
    }
}
