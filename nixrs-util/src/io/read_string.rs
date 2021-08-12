use std::future::Future;
use std::io;
use std::mem;
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::AsyncRead;

use super::read_int::ReadUsize;
use super::read_padding::ReadPadding;
use super::read_exact::ReadExact;


#[derive(Debug)]
pub enum ReadString<R> {
    Invalid,
    ReadSize(usize, ReadUsize<R>),
    ReadData(ReadExact<R>),
    ReadPadding(Vec<u8>, ReadPadding<R>),
    Done(R),
}

impl<R> ReadString<R> {
    pub fn new(src: R) -> ReadString<R> {
        ReadString::ReadSize(usize::MAX, ReadUsize::new(src))
    }
    pub fn with_limit(src: R, limit: usize) -> ReadString<R> {
        ReadString::ReadSize(limit, ReadUsize::new(src))
    }
    pub fn inner(self) -> R {
        match self {
            ReadString::Invalid => panic!("invalid state"),
            ReadString::ReadSize(_, r) => r.inner(),
            ReadString::ReadData(r) => r.inner(),
            ReadString::ReadPadding(_, r) => r.inner(),
            ReadString::Done(r) => r,
        }
    }
}


impl<R> Future for ReadString<R>
    where R: AsyncRead + Unpin
{
    type Output = io::Result<String>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        loop {
            match mem::replace(&mut *self, ReadString::Invalid) {
                ReadString::Invalid => panic!("invalid state"),
                ReadString::Done(_) => panic!("polling completed future"),
                ReadString::ReadSize(limit, mut reader) => {
                    let len = match Pin::new(&mut reader).poll(cx) {
                        Poll::Pending => {
                            *self = ReadString::ReadSize(limit, reader);
                            return Poll::Pending;
                        },
                        Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
                        Poll::Ready(Ok(v)) => v,
                    };
                    if len > limit {
                        return Poll::Ready(Err(io::Error::new(io::ErrorKind::InvalidData,
                            format!("string is to long: {}", len))));
                    }
                    let src = reader.inner();
                    if len == 0 {
                        *self = ReadString::Done(src);
                        return Poll::Ready(Ok(String::new()));
                    }
                    *self = ReadString::ReadData(ReadExact::new(src, Vec::with_capacity(len)));
                },
                ReadString::ReadData(mut reader) => {
                    let v = match Pin::new(&mut reader).poll(cx) {
                        Poll::Pending => {
                            *self = ReadString::ReadData(reader);
                            return Poll::Pending;
                        },
                        Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
                        Poll::Ready(Ok(v)) => v,
                    };
                    let src = reader.inner();
                    let size = v.len() as u64;
                    *self = ReadString::ReadPadding(v, ReadPadding::new(src, size));
                },
                ReadString::ReadPadding(buf, mut padding) => {
                    match Pin::new(&mut padding).poll(cx) {
                        Poll::Pending => {
                            *self = ReadString::ReadPadding(buf, padding);
                            return Poll::Pending;
                        }
                        Poll::Ready(res) => res?,
                    }
                    let s = String::from_utf8(buf)
                        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "String is not UTF-8"))?;
                    *self = ReadString::Done(padding.inner());
                    return Poll::Ready(Ok(s));
                }
            }    
        }
    }
}
