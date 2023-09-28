use std::io::Cursor;

use async_trait::async_trait;
use tokio::io::{AsyncRead, AsyncWrite};

use crate::{StoreDirProvider, Error};

#[async_trait]
pub trait BinaryCache: StoreDirProvider {
    async fn file_exists(&self, path: &str) -> Result<bool, Error>;
    async fn upsert_file<R>(&self, path: &str, stream: R, mime_type: &str) -> Result<(), Error>
        where R: AsyncRead + Send + Unpin;
    async fn upsert_file_data(&self, path: &str, data: &[u8], mime_type: &str) -> Result<(), Error> {
        let stream = Cursor::new(data);
        self.upsert_file(path, stream, mime_type).await
    }
    /// Dump the contents of the specified file to a sink.
    async fn get_file<W>(&self, path: &str, sink: W) -> Result<(), Error>
        where W: AsyncWrite + Send + Unpin;
}