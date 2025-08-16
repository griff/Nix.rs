use std::fmt;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll, ready};

use thrussh::server::Handle;
use thrussh::{ChannelId, CryptoVec};
use tokio::io::AsyncWrite;
use tracing::debug;

async fn make_ext_data_write_fut(
    id: ChannelId,
    ext: u32,
    data: Option<(Handle, CryptoVec)>,
) -> Result<(), CryptoVec> {
    match data {
        Some((mut handle, data)) => handle.extended_data(id, ext, data).await,
        None => unreachable!("this future should not be pollable in this state"),
    }
}

pub struct ExtendedDataWrite {
    id: ChannelId,
    ext: u32,
    handle: Handle,
    is_write_fut_valid: bool,
    write_fut: tokio_util::sync::ReusableBoxFuture<'static, Result<(), CryptoVec>>,
}

impl ExtendedDataWrite {
    pub fn new(id: ChannelId, ext: u32, handle: Handle) -> ExtendedDataWrite {
        ExtendedDataWrite {
            id,
            ext,
            handle,
            is_write_fut_valid: false,
            write_fut: tokio_util::sync::ReusableBoxFuture::new(make_ext_data_write_fut(
                id, ext, None,
            )),
        }
    }
}

impl Clone for ExtendedDataWrite {
    fn clone(&self) -> Self {
        ExtendedDataWrite::new(self.id, self.ext, self.handle.clone())
    }
}

impl fmt::Debug for ExtendedDataWrite {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ExtendedDataWrite")
            .field("id", &self.id)
            .field("ext", &self.ext)
            .field("is_write_fut_valid", &self.is_write_fut_valid)
            .field("write_fut", &self.write_fut)
            .finish()
    }
}

impl AsyncWrite for ExtendedDataWrite {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        ready!(self.as_mut().poll_flush(cx))?;

        let id = self.id;
        let ext = self.ext;
        let handle = self.handle.clone();
        self.write_fut.set(make_ext_data_write_fut(
            id,
            ext,
            Some((handle, CryptoVec::from_slice(buf))),
        ));
        self.is_write_fut_valid = true;
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        if !self.is_write_fut_valid {
            return Poll::Ready(Ok(()));
        }

        match self.write_fut.poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(_)) => {
                self.is_write_fut_valid = false;
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(_)) => {
                self.is_write_fut_valid = false;
                debug!("ChannelStream AsyncWrite EOF");
                Poll::Ready(Err(io::Error::new(io::ErrorKind::BrokenPipe, "EOF")))
            }
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        self.poll_flush(cx)
    }
}
