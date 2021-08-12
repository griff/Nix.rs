use std::future::Future;
use std::io;
use std::marker::PhantomData;
use std::mem;
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::AsyncRead;

use crate::StateParse;

use super::read_int::ReadUsize;
use super::read_string::ReadString;
use super::CollectionRead;

pub enum ReadParsedColl<R, S, T, C> {
    Invalid(PhantomData<T>),
    ReadSize(S, ReadUsize<R>),
    ReadData {
        state: S,
        coll: C,
        len: usize,
        reader: ReadString<R>,
    },
}

impl<R, S, T, C> ReadParsedColl<R, S, T, C> {
    pub fn new(src: R, state: S) -> ReadParsedColl<R, S, T, C>
    where
        S: StateParse<T>,
        C: CollectionRead<T>,
    {
        ReadParsedColl::ReadSize(state, ReadUsize::new(src))
    }
}

impl<R, S, T, C> Future for ReadParsedColl<R, S, T, C>
where
    R: AsyncRead + Unpin,
    S: StateParse<T> + Unpin,
    C: CollectionRead<T> + Unpin,
    S::Err: From<io::Error>,
    Self: Unpin,
{
    type Output = Result<C, S::Err>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        loop {
            match mem::replace(&mut *self, ReadParsedColl::Invalid(PhantomData)) {
                ReadParsedColl::Invalid(_) => panic!("invalid state"),
                ReadParsedColl::ReadSize(state, mut reader) => {
                    let len = match Pin::new(&mut reader).poll(cx) {
                        Poll::Pending => {
                            *self = ReadParsedColl::ReadSize(state, reader);
                            return Poll::Pending;
                        }
                        Poll::Ready(Err(err)) => return Poll::Ready(Err(err.into())),
                        Poll::Ready(Ok(v)) => v,
                    };
                    let src = reader.inner();
                    let coll = C::make(len);
                    if len == 0 {
                        return Poll::Ready(Ok(coll));
                    } else {
                        *self = ReadParsedColl::ReadData {
                            state,
                            coll,
                            len,
                            reader: ReadString::new(src),
                        };
                    }
                }
                ReadParsedColl::ReadData {
                    state,
                    mut coll,
                    len,
                    mut reader,
                } => {
                    let s = match Pin::new(&mut reader).poll(cx) {
                        Poll::Pending => {
                            *self = ReadParsedColl::ReadData {
                                state,
                                coll,
                                len,
                                reader,
                            };
                            return Poll::Pending;
                        }
                        Poll::Ready(Err(err)) => return Poll::Ready(Err(err.into())),
                        Poll::Ready(Ok(v)) => v,
                    };
                    let p = state.parse(&s)?;
                    coll.push(p);
                    if coll.len() == len {
                        return Poll::Ready(Ok(coll));
                    } else {
                        let src = reader.inner();
                        *self = ReadParsedColl::ReadData {
                            state,
                            coll,
                            len,
                            reader: ReadString::new(src),
                        };
                    }
                }
            }
        }
    }
}
