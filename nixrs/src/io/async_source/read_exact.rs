use std::future::Future;
use std::io;
use std::io::ErrorKind::UnexpectedEof;
use std::mem;
use std::pin::Pin;
use std::task::{ready, Context, Poll};

use bytes::BufMut;
use pin_project_lite::pin_project;
use tokio::io::AsyncRead;

pin_project! {
    #[derive(Debug)]
    #[must_use = "futures do nothing unless you `.await` or poll them"]
    pub struct ReadExact<R> {
        #[pin]
        src: R,
        buf: Vec<u8>,
    }
}

impl<R> ReadExact<R> {
    pub(crate) fn new(src: R, buf: Vec<u8>) -> Self {
        ReadExact { src, buf }
    }
    pub(crate) fn inner(self) -> R {
        self.src
    }
}

impl<R> Future for ReadExact<R>
where
    R: AsyncRead + Unpin,
{
    type Output = io::Result<Vec<u8>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        use mem::MaybeUninit;
        use tokio::io::ReadBuf;

        let mut me = self.project();

        while me.buf.len() < me.buf.capacity() {
            let n = {
                let dst = me.buf.chunk_mut();
                let dst = unsafe { &mut *(dst as *mut _ as *mut [MaybeUninit<u8>]) };
                let mut buf = ReadBuf::uninit(dst);
                let ptr = buf.filled().as_ptr();
                ready!(me.src.as_mut().poll_read(cx, &mut buf)?);

                assert_eq!(ptr, buf.filled().as_ptr());
                buf.filled().len()
            };
            if n == 0 {
                return Poll::Ready(Err(UnexpectedEof.into()));
            }
            unsafe {
                me.buf.advance_mut(n);
            }
        }
        Poll::Ready(Ok(mem::take(me.buf)))
    }
}
