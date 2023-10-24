use std::future::Future;
use std::io;
use std::iter::IntoIterator;
use std::mem;
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::AsyncWrite;

use super::write_int::WriteU64;
use super::write_string::{write_string, WriteString};
use super::CollectionSize;

pub fn write_owned_string_coll<W, C, I>(dst: W, coll: C) -> WriteOwnedStringColl<W, I>
where
    C: CollectionSize + IntoIterator<Item = String, IntoIter = I>,
    I: Iterator<Item = String>,
{
    let len = coll.len();
    let it = coll.into_iter();
    WriteOwnedStringColl::WriteSize(it, WriteU64::new(dst, len as u64))
}

#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub enum WriteOwnedStringColl<W, I> {
    Invalid,
    WriteSize(I, WriteU64<W>),
    WriteData(I, WriteString<W>),
    Done(W),
}

impl<'a, W, I> Future for WriteOwnedStringColl<W, I>
where
    W: AsyncWrite + Unpin,
    I: Iterator<Item = String> + Unpin,
{
    type Output = io::Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        loop {
            match mem::replace(&mut *self, WriteOwnedStringColl::Invalid) {
                WriteOwnedStringColl::Invalid => panic!("invalid state"),
                WriteOwnedStringColl::Done(_) => panic!("polling completed future"),
                WriteOwnedStringColl::WriteSize(mut it, mut writer) => {
                    match Pin::new(&mut writer).poll(cx) {
                        Poll::Pending => {
                            *self = WriteOwnedStringColl::WriteSize(it, writer);
                            return Poll::Pending;
                        }
                        Poll::Ready(res) => res?,
                    }
                    let dst = writer.inner();
                    if let Some(next) = it.next() {
                        *self = WriteOwnedStringColl::WriteData(it, write_string(dst, next));
                    } else {
                        *self = WriteOwnedStringColl::Done(dst);
                        return Poll::Ready(Ok(()));
                    }
                }
                WriteOwnedStringColl::WriteData(mut it, mut writer) => {
                    match Pin::new(&mut writer).poll(cx) {
                        Poll::Pending => {
                            *self = WriteOwnedStringColl::WriteData(it, writer);
                            return Poll::Pending;
                        }
                        Poll::Ready(res) => res?,
                    }
                    let dst = writer.inner();
                    if let Some(next) = it.next() {
                        *self = WriteOwnedStringColl::WriteData(it, write_string(dst, next));
                    } else {
                        *self = WriteOwnedStringColl::Done(dst);
                        return Poll::Ready(Ok(()));
                    }
                }
            }
        }
    }
}
