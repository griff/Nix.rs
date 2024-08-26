use std::future::Future;
use std::io;
use std::iter::IntoIterator;
use std::mem;
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::AsyncWrite;

use super::write_int::WriteU64;
use super::write_slice::{write_str, WriteSlice};
use super::CollectionSize;

#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub enum WriteStringColl<'a, W, I> {
    Invalid,
    WriteSize(I, WriteU64<W>),
    WriteData(I, WriteSlice<'a, W>),
    Done(W),
}

pub fn write_string_coll<'a, W, C, I>(dst: W, coll: C) -> WriteStringColl<'a, W, I>
where
    C: CollectionSize + IntoIterator<Item = &'a String, IntoIter = I>,
    I: Iterator<Item = &'a String>,
{
    let len = coll.len();
    let it = coll.into_iter();
    WriteStringColl::WriteSize(it, WriteU64::new(dst, len as u64))
}

impl<'a, W, I> Future for WriteStringColl<'a, W, I>
where
    W: AsyncWrite + Unpin,
    I: Iterator<Item = &'a String> + Unpin,
{
    type Output = io::Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        loop {
            match mem::replace(&mut *self, WriteStringColl::Invalid) {
                WriteStringColl::Invalid => panic!("invalid state"),
                WriteStringColl::Done(_) => panic!("polling completed future"),
                WriteStringColl::WriteSize(mut it, mut writer) => {
                    match Pin::new(&mut writer).poll(cx) {
                        Poll::Pending => {
                            *self = WriteStringColl::WriteSize(it, writer);
                            return Poll::Pending;
                        }
                        Poll::Ready(res) => res?,
                    }
                    let dst = writer.inner();
                    if let Some(next) = it.next() {
                        *self = WriteStringColl::WriteData(it, write_str(dst, next));
                    } else {
                        *self = WriteStringColl::Done(dst);
                        return Poll::Ready(Ok(()));
                    }
                }
                WriteStringColl::WriteData(mut it, mut writer) => {
                    match Pin::new(&mut writer).poll(cx) {
                        Poll::Pending => {
                            *self = WriteStringColl::WriteData(it, writer);
                            return Poll::Pending;
                        }
                        Poll::Ready(res) => res?,
                    }
                    let dst = writer.inner();
                    if let Some(next) = it.next() {
                        *self = WriteStringColl::WriteData(it, write_str(dst, next));
                    } else {
                        *self = WriteStringColl::Done(dst);
                        return Poll::Ready(Ok(()));
                    }
                }
            }
        }
    }
}
