use std::future::Future;
use std::io;
use std::mem;
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Bytes;
use tokio::io::AsyncRead;

use super::read_exact::ReadExact;
use super::read_int::ReadUsize;
use super::read_padding::ReadPadding;

#[derive(Debug)]
pub enum ReadBytes<R> {
    Invalid,
    ReadSize(usize, ReadUsize<R>),
    ReadData(ReadExact<R>),
    ReadPadding(Vec<u8>, ReadPadding<R>),
    Done(R),
}

impl<R> ReadBytes<R> {
    pub fn new(src: R) -> Self {
        Self::ReadSize(usize::MAX, ReadUsize::new(src))
    }
    pub fn with_limit(src: R, limit: usize) -> Self {
        Self::ReadSize(limit, ReadUsize::new(src))
    }
    pub fn inner(self) -> R {
        match self {
            Self::Invalid => panic!("invalid state"),
            Self::ReadSize(_, r) => r.inner(),
            Self::ReadData(r) => r.inner(),
            Self::ReadPadding(_, r) => r.inner(),
            Self::Done(r) => r,
        }
    }
}

impl<R> Future for ReadBytes<R>
where
    R: AsyncRead + Unpin,
{
    type Output = io::Result<Bytes>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        loop {
            match mem::replace(&mut *self, Self::Invalid) {
                Self::Invalid => panic!("invalid state"),
                Self::Done(_) => panic!("polling completed future"),
                Self::ReadSize(limit, mut reader) => {
                    let len = match Pin::new(&mut reader).poll(cx) {
                        Poll::Pending => {
                            *self = Self::ReadSize(limit, reader);
                            return Poll::Pending;
                        }
                        Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
                        Poll::Ready(Ok(v)) => v,
                    };
                    if len > limit {
                        return Poll::Ready(Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("string is to long: {}", len),
                        )));
                    }
                    let src = reader.inner();
                    if len == 0 {
                        *self = Self::Done(src);
                        return Poll::Ready(Ok(Bytes::new()));
                    }
                    *self = Self::ReadData(ReadExact::new(src, Vec::with_capacity(len)));
                }
                Self::ReadData(mut reader) => {
                    let v = match Pin::new(&mut reader).poll(cx) {
                        Poll::Pending => {
                            *self = Self::ReadData(reader);
                            return Poll::Pending;
                        }
                        Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
                        Poll::Ready(Ok(v)) => v,
                    };
                    let src = reader.inner();
                    let size = v.len() as u64;
                    *self = Self::ReadPadding(v, ReadPadding::new(src, size));
                }
                Self::ReadPadding(buf, mut padding) => {
                    match Pin::new(&mut padding).poll(cx) {
                        Poll::Pending => {
                            *self = Self::ReadPadding(buf, padding);
                            return Poll::Pending;
                        }
                        Poll::Ready(res) => res?,
                    }
                    let s = Bytes::from(buf);
                    *self = Self::Done(padding.inner());
                    return Poll::Ready(Ok(s));
                }
            }
        }
    }
}
