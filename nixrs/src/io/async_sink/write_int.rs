use std::future::Future;
use std::io;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

use bytes::BufMut;
use pin_project_lite::pin_project;
use tokio::io::AsyncWrite;

pin_project! {
    #[derive(Debug)]
    #[must_use = "futures do nothing unless you `.await` or poll them"]
    pub struct WriteU64<W> {
        #[pin]
        dst: W,
        buf: [u8; 8],
        written: u8,
    }
}

impl<W> WriteU64<W> {
    pub(crate) fn new(dst: W, value: u64) -> Self {
        let mut writer = WriteU64 {
            dst,
            buf: [0; 8],
            written: 0,
        };
        BufMut::put_u64_le(&mut &mut writer.buf[..], value);
        writer
    }
    pub(crate) fn inner(self) -> W {
        self.dst
    }
}

impl<W> Future for WriteU64<W>
where
    W: AsyncWrite,
{
    type Output = io::Result<()>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut me = self.project();

        if *me.written == 8 {
            return Poll::Ready(Ok(()));
        }

        while *me.written < 8 {
            *me.written += match me
                .dst
                .as_mut()
                .poll_write(cx, &me.buf[*me.written as usize..])
            {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Ready(Ok(0)) => {
                    return Poll::Ready(Err(io::ErrorKind::WriteZero.into()));
                }
                Poll::Ready(Ok(n)) => n as u8,
            };
        }
        Poll::Ready(Ok(()))
    }
}
