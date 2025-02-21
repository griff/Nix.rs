use std::cmp::min;
use std::io;
use std::pin::Pin;
use std::task::{ready, Context, Poll};

use bytes::{Buf as _, BufMut, BytesMut};
use pin_project_lite::pin_project;
use tokio::io::AsyncWrite;

use crate::daemon::{DEFAULT_BUF_SIZE, RESERVED_BUF_SIZE};

pin_project! {
    #[derive(Debug)]
    pub struct FramedWriter<W> {
        #[pin]
        inner: W,
        flushing: bool,
        shutdown: bool,
        buf: BytesMut,
    }
}

impl<W> FramedWriter<W>
where
    W: AsyncWrite,
{
    pub fn new(writer: W) -> Self {
        let mut buf = BytesMut::with_capacity(DEFAULT_BUF_SIZE);
        buf.put_u64_le(0);
        FramedWriter {
            inner: writer,
            flushing: false,
            shutdown: false,
            buf,
        }
    }
    fn poll_flush_buf(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        let mut this = self.project();
        if !*this.flushing && this.buf.len() > 8 {
            *this.flushing = true;
            let len = this.buf.len() - 8;
            eprintln!("FramedWriter:poll_flush_buf: set length {}", len);
            BufMut::put_u64_le(&mut &mut this.buf[..8], len as u64);
        }
        if *this.flushing {
            while !this.buf.is_empty() {
                eprintln!("FramedWriter:poll_flush_buf: write {}", this.buf.len());
                let n = ready!(this.inner.as_mut().poll_write(cx, &this.buf[..]))?;
                if n == 0 {
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::WriteZero,
                        "failed to write the buffer",
                    )));
                }
                this.buf.advance(n);
            }
            *this.flushing = false;
            this.buf.reserve(RESERVED_BUF_SIZE);
            this.buf.put_u64_le(0);
        } else {
            this.buf.reserve(RESERVED_BUF_SIZE);
        }
        eprintln!("FramedWriter:poll_flush_buf: done");
        Poll::Ready(Ok(()))
    }

    pub fn remaining_mut(&self) -> usize {
        self.buf.capacity() - self.buf.len()
    }
}

impl<W> AsyncWrite for FramedWriter<W>
where
    W: AsyncWrite,
{
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        eprintln!("FramedWriter:poll_write: check buf");
        if self.remaining_mut() == 0 {
            ready!(self.as_mut().poll_flush_buf(cx))?;
        }
        let write = min(buf.len(), self.remaining_mut());
        eprintln!("FramedWriter:poll_write: write slice");
        self.project().buf.put_slice(&buf[..write]);
        eprintln!("FramedWriter:poll_write: done");
        Poll::Ready(Ok(write))
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        eprintln!("FramedWriter:poll_flush: flush buf");
        ready!(self.as_mut().poll_flush_buf(cx))?;
        eprintln!("FramedWriter:poll_flush: flush writer");
        self.project().inner.poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), io::Error>> {
        eprintln!("FramedWriter:poll_shutdown: flush");
        ready!(self.as_mut().poll_flush_buf(cx))?;
        let mut this = self.as_mut().project();
        if !*this.shutdown {
            while !this.buf.is_empty() {
                eprintln!("FramedWriter:poll_shutdown: write {}", this.buf.len());
                let n = ready!(this.inner.as_mut().poll_write(cx, &this.buf[..]))?;
                if n == 0 {
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::WriteZero,
                        "failed to write the buffer",
                    )));
                }
                this.buf.advance(n);
            }
        }
        *this.shutdown = true;
        ready!(this.inner.poll_flush(cx))?;
        eprintln!("FramedWriter:poll_shutdown: done");
        Poll::Ready(Ok(()))
    }
}

#[cfg(test)]
mod test {
    use hex_literal::hex;
    use tokio::io::AsyncWriteExt;
    use tokio_test::io::Builder;

    use super::*;

    #[tokio::test]
    async fn test_write_frames() {
        let mut mock = Builder::new()
            .write(&hex!(
                "0100 0000 0000 0000 20 0400 0000 0000 0000 4142 4344"
            ))
            .write(&hex!("0100 0000 0000 0000 45 0000 0000 0000 0000 46"))
            .build();
        let mut writer = FramedWriter::new(&mut mock);
        writer.write_all(b" ").await.unwrap();
        writer.flush().await.unwrap();
        writer.flush().await.unwrap();
        writer.flush().await.unwrap();
        writer.write_all(b"ABCD").await.unwrap();
        writer.flush().await.unwrap();
        writer.write_all(b"E").await.unwrap();
        writer.shutdown().await.unwrap();
        mock.write_all(b"F").await.unwrap();
    }
}
