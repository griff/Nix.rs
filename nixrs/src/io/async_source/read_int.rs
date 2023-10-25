use std::future::Future;
use std::io;
use std::io::ErrorKind::UnexpectedEof;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{ready, Context, Poll};
use std::time::{Duration, SystemTime};

use bytes::Buf;
use pin_project_lite::pin_project;
use tokio::io::AsyncRead;
use tokio::io::ReadBuf;

pin_project! {
    #[derive(Debug)]
    #[must_use = "futures do nothing unless you `.await` or poll them"]
    pub struct ReadU64<R> {
        #[pin]
        src: R,
        buf: [u8; 8],
        read: u8,
    }
}

impl<R> ReadU64<R> {
    pub(crate) fn new(src: R) -> Self {
        ReadU64 {
            src,
            buf: [0; 8],
            read: 0,
        }
    }
    pub(crate) fn inner(self) -> R {
        self.src
    }
}

impl<R: AsyncRead> Future for ReadU64<R> {
    type Output = io::Result<u64>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut me = self.project();
        if *me.read == 8 {
            return Poll::Ready(Ok(Buf::get_u64_le(&mut &me.buf[..])));
        }
        while *me.read < 8 {
            let mut buf = ReadBuf::new(&mut me.buf[*me.read as usize..]);

            *me.read += match me.src.as_mut().poll_read(cx, &mut buf) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Ready(Ok(())) => {
                    let n = buf.filled().len();
                    if n == 0 {
                        return Poll::Ready(Err(UnexpectedEof.into()));
                    }

                    n as u8
                }
            };
        }

        let num = Buf::get_u64_le(&mut &me.buf[..]);
        Poll::Ready(Ok(num))
    }
}

macro_rules! reader {
    ($name:ident, $t:ty, |$v:ident| { $e:expr }) => {
        pin_project! {
            #[derive(Debug)]
            #[must_use = "futures do nothing unless you `.await` or poll them"]
            pub struct $name<R> {
                #[pin]
                inner: ReadU64<R>,
            }
        }
        impl<R> $name<R> {
            pub(crate) fn new(src: R) -> $name<R> {
                $name {
                    inner: ReadU64::new(src),
                }
            }
            #[allow(dead_code)]
            pub(crate) fn inner(self) -> R {
                self.inner.inner()
            }
        }

        impl<R> Future for $name<R>
        where
            R: AsyncRead,
        {
            type Output = io::Result<$t>;

            fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
                let $v = ready!(self.project().inner.poll(cx))?;
                $e
            }
        }
    };
}

reader!(ReadUsize, usize, |v| {
    if v > usize::MAX as u64 {
        Poll::Ready(Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{} larger than {}", v, usize::MAX),
        )))
    } else {
        Poll::Ready(Ok(v as usize))
    }
});
reader!(ReadBool, bool, |v| { Poll::Ready(Ok(v != 0)) });
reader!(ReadSeconds, Duration, |v| {
    Poll::Ready(Ok(Duration::from_secs(v)))
});
reader!(ReadTime, SystemTime, |v| {
    Poll::Ready(Ok(SystemTime::UNIX_EPOCH + Duration::from_secs(v)))
});

pin_project! {
    #[derive(Debug)]
    #[must_use = "futures do nothing unless you `.await` or poll them"]
    pub struct ReadEnum<R,T> {
        #[pin]
        inner: ReadU64<R>,
        _to: PhantomData<T>,
    }
}

impl<R, T> ReadEnum<R, T> {
    pub fn new(src: R) -> ReadEnum<R, T> {
        ReadEnum {
            inner: ReadU64::new(src),
            _to: PhantomData,
        }
    }
    #[allow(dead_code)]
    pub(crate) fn inner(self) -> R {
        self.inner.inner()
    }
}

impl<R, T> Future for ReadEnum<R, T>
where
    R: AsyncRead,
    T: From<u64>,
{
    type Output = io::Result<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let v = ready!(self.project().inner.poll(cx))?;
        Poll::Ready(Ok(v.into()))
    }
}

pin_project! {
    #[derive(Debug)]
    #[must_use = "futures do nothing unless you `.await` or poll them"]
    pub struct ReadFlag<R,F> {
        #[pin]
        inner: ReadBool<R>,
        _to: PhantomData<F>,
    }
}

impl<R, F> ReadFlag<R, F> {
    pub fn new(src: R) -> ReadFlag<R, F> {
        ReadFlag {
            inner: ReadBool::new(src),
            _to: PhantomData,
        }
    }
    #[allow(dead_code)]
    pub(crate) fn inner(self) -> R {
        self.inner.inner()
    }
}

impl<R, F> Future for ReadFlag<R, F>
where
    R: AsyncRead,
    F: From<bool>,
{
    type Output = io::Result<F>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let v = ready!(self.project().inner.poll(cx))?;
        Poll::Ready(Ok(v.into()))
    }
}
