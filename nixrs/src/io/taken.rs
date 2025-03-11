use std::io;
use std::ops::DerefMut;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::Bytes;
use pin_project_lite::pin_project;
use tokio::io::{AsyncRead, ReadBuf};

use super::AsyncBytesRead;

pin_project! {
    pub struct TakenReader<R> {
        reader: Arc<parking_lot::Mutex<Option<R>>>,
    }
}

impl<R> TakenReader<R> {
    pub fn new(reader: R) -> (Stealer<R>, TakenReader<R>) {
        let reader = Arc::new(parking_lot::Mutex::new(Some(reader)));
        (
            Stealer {
                reader: reader.clone(),
                loot: None,
            },
            TakenReader { reader },
        )
    }

    fn with_lock<F, T>(self: Pin<&mut Self>, f: F) -> Poll<io::Result<T>>
    where
        F: FnOnce(Pin<&mut R>) -> Poll<io::Result<T>>,
        R: Unpin,
    {
        let mut guard = self.reader.as_ref().lock();
        if let Some(reader) = guard.as_mut() {
            let a = Pin::new(reader);
            f(a)
        } else {
            Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "reader has been stolen",
            )))
        }
    }
}

impl<R> AsyncRead for TakenReader<R>
where
    R: AsyncRead + Unpin,
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        self.with_lock(|reader| reader.poll_read(cx, buf))
    }
}

impl<R> AsyncBytesRead for TakenReader<R>
where
    R: AsyncBytesRead + Unpin,
{
    fn poll_force_fill_buf(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<Bytes>> {
        self.with_lock(|reader| reader.poll_force_fill_buf(cx))
    }

    fn poll_fill_buf(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<Bytes>> {
        self.with_lock(|reader| reader.poll_fill_buf(cx))
    }

    fn prepare(self: Pin<&mut Self>, amt: usize) {
        let mut guard = self.reader.as_ref().lock();
        if let Some(reader) = guard.as_mut() {
            let a = Pin::new(reader);
            a.prepare(amt);
        }
    }

    fn consume(self: Pin<&mut Self>, amt: usize) {
        let mut guard = self.reader.as_ref().lock();
        if let Some(reader) = guard.as_mut() {
            let a = Pin::new(reader);
            a.consume(amt);
        }
    }
}

pub struct Stealer<R> {
    reader: Arc<parking_lot::Mutex<Option<R>>>,
    loot: Option<R>,
}

impl<R> Stealer<R> {
    pub fn loot(self) -> R {
        if let Some(reader) = self.loot {
            reader
        } else {
            let mut guard = self.reader.as_ref().lock();
            guard.take().unwrap()
        }
    }
}

impl<R> AsyncRead for Stealer<R>
where
    R: AsyncRead + Unpin,
{
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        if let Some(reader) = self.loot.as_mut() {
            Pin::new(reader).poll_read(cx, buf)
        } else {
            let this = self.deref_mut();
            let mut guard = this.reader.as_ref().lock();
            let mut reader = guard.take().unwrap();
            let ret = Pin::new(&mut reader).poll_read(cx, buf);
            this.loot = Some(reader);
            ret
        }
    }
}
