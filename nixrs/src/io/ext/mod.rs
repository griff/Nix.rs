mod buf;
mod chunked;

use std::pin::Pin;

use bytes::{Buf, Bytes};
use futures::{future::poll_fn, io};
use tracing::trace;

use crate::io::AsyncBytesRead;

pub use buf::{BytesBuf, Limited};
pub use chunked::{Chunked, ChunkedMut};

pub trait AsyncBytesReadExt: AsyncBytesRead {
    fn force_fill_buf(&mut self) -> impl Future<Output = io::Result<<Self as AsyncBytesRead>::Buf>>
    where
        Self: Unpin;
    fn fill_buf(&mut self) -> impl Future<Output = io::Result<<Self as AsyncBytesRead>::Buf>>
    where
        Self: Unpin;
    fn consume(&mut self, amt: usize)
    where
        Self: Unpin;
    fn drain_all(&mut self) -> impl Future<Output = io::Result<u64>>
    where
        Self: Unpin;
}

impl<R> AsyncBytesReadExt for R
where
    R: AsyncBytesRead,
{
    async fn force_fill_buf(&mut self) -> io::Result<R::Buf>
    where
        Self: Unpin,
    {
        let mut pined = Pin::new(self);
        poll_fn(|cx| pined.as_mut().poll_force_fill_buf(cx)).await
    }

    async fn fill_buf(&mut self) -> io::Result<R::Buf>
    where
        Self: Unpin,
    {
        let mut pined = Pin::new(self);
        poll_fn(|cx| pined.as_mut().poll_fill_buf(cx)).await
    }

    fn consume(&mut self, amt: usize)
    where
        Self: Unpin,
    {
        Pin::new(self).consume(amt);
    }

    async fn drain_all(&mut self) -> io::Result<u64>
    where
        Self: Unpin,
    {
        let mut drained = 0;
        loop {
            trace!(drained, "AsyncBytesReadExt:drain_all: Reading");
            let amt = self.fill_buf().await?.remaining();
            trace!(drained, amt, "AsyncBytesReadExt:drain_all: Read");
            if amt == 0 {
                return Ok(drained);
            }
            drained += amt as u64;
            self.consume(amt);
        }
    }
}
