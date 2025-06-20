use std::future::Future;
use std::io;
use std::mem;
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::AsyncWrite;

use crate::io::calc_padding;
use crate::io::STATIC_PADDING;

use super::write_all::{write_all, WriteAll};
use super::write_int::WriteU64;

#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub enum WriteSlice<'a, W> {
    Invalid,
    WriteSize(&'a [u8], WriteU64<W>),
    WriteData(u8, WriteAll<'a, W>),
    WritePadding(WriteAll<'static, W>),
    Done(W),
}

pub(crate) fn write_str<W>(dst: W, s: &str) -> WriteSlice<'_, W> {
    let buf = s.as_bytes();
    let len = buf.len();
    WriteSlice::WriteSize(buf, WriteU64::new(dst, len as u64))
}

pub(crate) fn write_buf<W>(dst: W, buf: &[u8]) -> WriteSlice<'_, W> {
    let len = buf.len();
    WriteSlice::WriteSize(buf, WriteU64::new(dst, len as u64))
}

impl<W> WriteSlice<'_, W> {
    pub fn inner(self) -> W {
        match self {
            WriteSlice::Invalid => panic!("invalid state"),
            WriteSlice::WriteSize(_, w) => w.inner(),
            WriteSlice::WriteData(_, w) => w.inner(),
            WriteSlice::WritePadding(w) => w.inner(),
            WriteSlice::Done(w) => w,
        }
    }
}

impl<W> Future for WriteSlice<'_, W>
where
    W: AsyncWrite + Unpin,
{
    type Output = io::Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        loop {
            match mem::replace(&mut *self, WriteSlice::Invalid) {
                WriteSlice::Invalid => panic!("invalid state"),
                WriteSlice::Done(_) => panic!("polling completed future"),
                WriteSlice::WriteSize(buf, mut writer) => {
                    match Pin::new(&mut writer).poll(cx) {
                        Poll::Pending => {
                            *self = WriteSlice::WriteSize(buf, writer);
                            return Poll::Pending;
                        }
                        Poll::Ready(res) => res?,
                    }
                    let dst = writer.inner();
                    if buf.is_empty() {
                        *self = WriteSlice::Done(dst);
                        return Poll::Ready(Ok(()));
                    }
                    let padding = calc_padding(buf.len() as u64);
                    *self = WriteSlice::WriteData(padding, write_all(dst, buf));
                }
                WriteSlice::WriteData(padding, mut writer) => {
                    match Pin::new(&mut writer).poll(cx) {
                        Poll::Pending => {
                            *self = WriteSlice::WriteData(padding, writer);
                            return Poll::Pending;
                        }
                        Poll::Ready(res) => res?,
                    }
                    let dst = writer.inner();
                    *self = WriteSlice::WritePadding(write_all(
                        dst,
                        &STATIC_PADDING[..padding as usize],
                    ));
                }
                WriteSlice::WritePadding(mut writer) => {
                    match Pin::new(&mut writer).poll(cx) {
                        Poll::Pending => {
                            *self = WriteSlice::WritePadding(writer);
                            return Poll::Pending;
                        }
                        Poll::Ready(res) => res?,
                    }
                    *self = WriteSlice::Done(writer.inner());
                    return Poll::Ready(Ok(()));
                }
            }
        }
    }
}
