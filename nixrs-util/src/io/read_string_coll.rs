use std::future::Future;
use std::io;
use std::mem;
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::AsyncRead;

use super::read_int::ReadUsize;
use super::read_string::ReadString;
use super::CollectionRead;

#[derive(Debug)]
pub enum ReadStringColl<R, C> {
    Invalid,
    ReadSize(ReadUsize<R>),
    ReadData(usize, C, ReadString<R>),
}

impl<R, C> ReadStringColl<R, C> {
    pub fn new(src: R) -> ReadStringColl<R, C>
    where
        C: CollectionRead<String>,
    {
        ReadStringColl::ReadSize(ReadUsize::new(src))
    }
}

impl<R, C> Future for ReadStringColl<R, C>
where
    R: AsyncRead + Unpin,
    C: CollectionRead<String> + Unpin,
{
    type Output = io::Result<C>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        loop {
            match mem::replace(&mut *self, ReadStringColl::Invalid) {
                ReadStringColl::Invalid => panic!("invalid state"),
                ReadStringColl::ReadSize(mut reader) => {
                    let len = match Pin::new(&mut reader).poll(cx) {
                        Poll::Pending => {
                            *self = ReadStringColl::ReadSize(reader);
                            return Poll::Pending;
                        }
                        Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
                        Poll::Ready(Ok(v)) => v,
                    };
                    let src = reader.inner();
                    let coll = C::make(len);
                    if len == 0 {
                        return Poll::Ready(Ok(coll));
                    } else {
                        *self = ReadStringColl::ReadData(len, coll, ReadString::new(src));
                    }
                }
                ReadStringColl::ReadData(len, mut coll, mut reader) => {
                    let s = match Pin::new(&mut reader).poll(cx) {
                        Poll::Pending => {
                            *self = ReadStringColl::ReadData(len, coll, reader);
                            return Poll::Pending;
                        }
                        Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
                        Poll::Ready(Ok(v)) => v,
                    };
                    coll.push(s);
                    if coll.len() == len {
                        return Poll::Ready(Ok(coll));
                    } else {
                        let src = reader.inner();
                        *self = ReadStringColl::ReadData(len, coll, ReadString::new(src));
                    }
                }
            }
        }
    }
}
