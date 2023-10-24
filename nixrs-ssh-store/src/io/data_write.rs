use std::fmt;
use std::io;
use std::pin::Pin;
use std::task::{ready, Context, Poll};

use tracing::{debug, error, trace};
use thrussh::server::Handle;
use thrussh::{ChannelId, CryptoVec};
use tokio::io::AsyncWrite;

async fn make_eof_fut(id: ChannelId, data: Option<Handle>) -> Result<(), ()> {
    match data {
        Some(mut handle) => {
            debug!("Sending EOF on stdout");
            handle.eof(id).await
        }
        None => unreachable!("this future should not be pollable in this state"),
    }
}

async fn make_data_write_fut(id: ChannelId, data: Option<(Handle, CryptoVec)>) -> io::Result<()> {
    match data {
        Some((mut handle, data)) => {
            let len = data.len();
            trace!("Sending data to stdout {}", len);
            match handle.data(id, data).await {
                Ok(_) => {
                    trace!("Data sent to stdout {}", len);
                    Ok(())
                }
                Err(_) => {
                    error!("Data not sent to stdout {}", len);
                    Err(io::Error::new(io::ErrorKind::BrokenPipe, "Channel closed"))
                }
            }
        }
        None => unreachable!("this future should not be pollable in this state"),
    }
}

pub struct DataWrite {
    id: ChannelId,
    handle: Handle,
    is_write_fut_valid: bool,
    write_fut: tokio_util::sync::ReusableBoxFuture<'static, io::Result<()>>,
    is_eof_fut_valid: bool,
    eof_fut: tokio_util::sync::ReusableBoxFuture<'static, Result<(), ()>>,
}

impl DataWrite {
    pub fn new(id: ChannelId, handle: Handle) -> DataWrite {
        DataWrite {
            id,
            handle,
            is_write_fut_valid: false,
            write_fut: tokio_util::sync::ReusableBoxFuture::new(make_data_write_fut(id, None)),
            is_eof_fut_valid: false,
            eof_fut: tokio_util::sync::ReusableBoxFuture::new(make_eof_fut(id, None)),
        }
    }
}

impl fmt::Debug for DataWrite {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DataWrite").field("id", &self.id).field("is_write_fut_valid", &self.is_write_fut_valid).field("write_fut", &self.write_fut).field("is_eof_fut_valid", &self.is_eof_fut_valid).field("eof_fut", &self.eof_fut).finish()
    }
}

impl AsyncWrite for DataWrite {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        ready!(self.as_mut().poll_flush(cx))?;

        trace!("Poll write DataWrite {}", buf.len());
        let id = self.id;
        let handle = self.handle.clone();
        self.write_fut.set(make_data_write_fut(
            id,
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
            Poll::Ready(Err(err)) => {
                self.is_write_fut_valid = false;
                debug!("ChannelStream AsyncWrite EOF");
                Poll::Ready(Err(err))
            }
        }
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), io::Error>> {
        ready!(self.as_mut().poll_flush(cx))?;

        if !self.is_eof_fut_valid {
            let id = self.id;
            let handle = self.handle.clone();
            self.eof_fut.set(make_eof_fut(id, Some(handle)));
            self.is_eof_fut_valid = true;
        }

        match self.eof_fut.poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(_)) => {
                self.is_eof_fut_valid = false;
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(_)) => {
                self.is_eof_fut_valid = false;
                debug!("ChannelStream AsyncWrite EOF");
                Poll::Ready(Err(io::Error::new(io::ErrorKind::Other, "EOF")))
            }
        }
    }
}
