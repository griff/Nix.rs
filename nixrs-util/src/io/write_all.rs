use std::future::Future;
use std::io;
use std::mem;
use std::pin::Pin;
use std::task::{Context, Poll};

use pin_project_lite::pin_project;
use tokio::io::AsyncWrite;

use crate::ready;

pin_project! {
    #[derive(Debug)]
    #[must_use = "futures do nothing unless you `.await` or poll them"]
    pub struct WriteAll<'a, W> {
        writer: W,
        buf: &'a [u8],
    }
}

impl<'a, W> WriteAll<'a, W> {
    pub fn inner(self) -> W {
        self.writer
    }
}

pub(crate) fn write_all<'a, W>(writer: W, buf: &'a [u8]) -> WriteAll<'a, W>
where
    W: AsyncWrite + Unpin,
{
    WriteAll { writer, buf }
}

impl<W> Future for WriteAll<'_, W>
where
    W: AsyncWrite + Unpin,
{
    type Output = io::Result<()>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let mut me = self.project();
        while !me.buf.is_empty() {
            let n = ready!(Pin::new(&mut me.writer).poll_write(cx, me.buf))?;
            {
                let (_, rest) = mem::replace(&mut *me.buf, &[]).split_at(n);
                *me.buf = rest;
            }
            if n == 0 {
                return Poll::Ready(Err(io::ErrorKind::WriteZero.into()));
            }
        }

        Poll::Ready(Ok(()))
    }
}
