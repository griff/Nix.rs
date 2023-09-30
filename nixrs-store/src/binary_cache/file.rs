use std::{path::{PathBuf, Path}, sync::Arc};

use async_trait::async_trait;
use tokio::{io::{AsyncRead, AsyncWrite}, fs};

use crate::{StoreDir, StoreDirProvider, Error};

use super::BinaryCache;

#[derive(Clone, Debug)]
pub struct FileBinaryCache {
    store_dir: StoreDir,
    base_path: Arc<PathBuf>,
}

impl FileBinaryCache {
    pub fn new(base_path: impl AsRef<Path>) -> FileBinaryCache {
        Self::with_store(base_path, Default::default())
    }
    pub fn with_store(base_path: impl AsRef<Path>, store_dir: StoreDir) -> FileBinaryCache {
        let base_path = Arc::new(base_path.as_ref().to_owned());
        FileBinaryCache {
            base_path, store_dir
        }
    }
}

impl StoreDirProvider for FileBinaryCache {
    fn store_dir(&self) -> StoreDir {
        self.store_dir.clone()
    }
}

#[async_trait]
impl BinaryCache for FileBinaryCache {
    async fn file_exists(&self, path: &str) -> Result<bool, Error> {
        let path = self.base_path.join(path);
        Ok(fs::try_exists(path).await?)
    }
    async fn upsert_file<R>(&self, path: &str, mut stream: R, _mime_type: &str) -> Result<(), Error>
        where R: AsyncRead + Send + Unpin
    {
        let path = self.base_path.join(path);
        let mut f = fs::File::create(path).await?;
        tokio::io::copy(&mut stream, &mut f).await?;
        Ok(())
    }

    /// Dump the contents of the specified file to a sink.
    async fn get_file<W>(&self, path: &str, mut sink: W) -> Result<(), Error>
        where W: AsyncWrite + Send + Unpin
    {
        let path = self.base_path.join(path);
        let mut f = fs::File::open(path).await?;
        tokio::io::copy(&mut f, &mut sink).await?;
        Ok(())
    }
}