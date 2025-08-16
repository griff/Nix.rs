use std::future::Future;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll, ready};

use pin_project_lite::pin_project;
use tokio::io::AsyncRead;
use tokio::io::ReadBuf;

const BUF_SIZE: usize = 64_000;

pin_project! {
    #[derive(Debug)]
    #[must_use = "futures do nothing unless you `.await` or poll them"]
    pub struct DrainAll<R> {
        #[pin]
        reader: R,
        read: u64,
        buf: [u8; BUF_SIZE],
    }
}

impl<R> DrainAll<R> {
    pub fn new(reader: R) -> DrainAll<R> {
        Self {
            reader,
            read: 0,
            buf: [0u8; BUF_SIZE],
        }
    }
}

impl<R> Future for DrainAll<R>
where
    R: AsyncRead,
{
    type Output = io::Result<u64>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut me = self.project();
        let mut buf = ReadBuf::new(&mut me.buf[..]);
        loop {
            ready!(me.reader.as_mut().poll_read(cx, &mut buf))?;
            let read = buf.filled().len();
            if read == 0 {
                return Poll::Ready(Ok(*me.read));
            }
            *me.read += read as u64;
            buf.clear();
        }
    }
}

pin_project! {
    #[derive(Debug)]
    pub struct DrainExact<R> {
        #[pin]
        reader: R,
        read: u64,
        len: u64,
        buf: [u8; BUF_SIZE],
    }
}

impl<R> DrainExact<R> {
    pub fn new(reader: R, len: u64) -> DrainExact<R> {
        Self {
            reader,
            len,
            read: 0,
            buf: [0u8; BUF_SIZE],
        }
    }
}

impl<R> Future for DrainExact<R>
where
    R: AsyncRead,
{
    type Output = io::Result<()>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut me = self.project();
        loop {
            let needed = *me.len - *me.read;
            let mut buf = if needed < me.buf.len() as u64 {
                ReadBuf::new(&mut me.buf[..needed as usize])
            } else {
                ReadBuf::new(&mut me.buf[..])
            };

            ready!(me.reader.as_mut().poll_read(cx, &mut buf))?;
            let read = buf.filled().len();
            if read == 0 {
                if me.read != me.len {
                    return Poll::Ready(Err(io::ErrorKind::UnexpectedEof.into()));
                } else {
                    return Poll::Ready(Ok(()));
                }
            }
            *me.read += read as u64;
            if me.read == me.len {
                return Poll::Ready(Ok(()));
            }
        }
    }
}
