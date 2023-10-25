use std::future::Future;
use std::io;
use std::mem;
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Bytes;
use bytes::BytesMut;
use tokio::io::AsyncRead;

use super::read_exact::ReadExact;
use super::read_int::ReadUsize;
use super::read_padding::ReadPadding;

#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub enum ReadBytes<R> {
    Invalid,
    ReadSize(BytesMut, usize, ReadUsize<R>),
    ReadData(ReadExact<R>),
    ReadPadding(Bytes, ReadPadding<R>),
    Done(R),
}

impl<R> ReadBytes<R> {
    pub fn new(src: R, buf: BytesMut) -> Self {
        Self::ReadSize(buf, usize::MAX, ReadUsize::new(src))
    }
    pub fn with_limit(src: R, limit: usize, buf: BytesMut) -> Self {
        Self::ReadSize(buf, limit, ReadUsize::new(src))
    }
    pub fn inner(self) -> R {
        match self {
            Self::Invalid => panic!("invalid state"),
            Self::ReadSize(_, _, r) => r.inner(),
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
                Self::ReadSize(mut buffer, limit, mut reader) => {
                    let len = match Pin::new(&mut reader).poll(cx) {
                        Poll::Pending => {
                            *self = Self::ReadSize(buffer, limit, reader);
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
                    buffer.reserve(len);
                    *self = Self::ReadData(ReadExact::new(src, len, buffer));
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
                    *self = Self::Done(padding.inner());
                    return Poll::Ready(Ok(buf));
                }
            }
        }
    }
}
