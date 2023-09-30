use std::io;
use std::pin::Pin;
use std::task::{ready, Context, Poll};

use pin_project_lite::pin_project;
use tokio::io::{AsyncBufRead, AsyncRead, ReadBuf};

pin_project! {
    pub struct OffsetReader<R> {
        #[pin]
       inner: R,
       offset: u64,
    }
}

impl<R> OffsetReader<R> {
    pub fn new(reader: R) -> OffsetReader<R> {
        OffsetReader {
            inner: reader,
            offset: 0,
        }
    }

    pub fn offset(&self) -> u64 {
        self.offset
    }
}

impl<R: AsyncRead> AsyncRead for OffsetReader<R> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.project();
        let before = buf.remaining();
        ready!(this.inner.poll_read(cx, buf))?;
        *this.offset += (before - buf.remaining()) as u64;
        Poll::Ready(Ok(()))
    }
}

impl<R: AsyncBufRead> AsyncBufRead for OffsetReader<R> {
    fn poll_fill_buf(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<&[u8]>> {
        self.project().inner.poll_fill_buf(cx)
    }

    fn consume(self: Pin<&mut Self>, amt: usize) {
        let this = self.project();
        *this.offset += amt as u64;
        this.inner.consume(amt)
    }
}
