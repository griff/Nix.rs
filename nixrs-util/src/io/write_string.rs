use std::future::Future;
use std::io;
use std::mem;
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::AsyncWrite;

use crate::io::calc_padding;

use super::write_int::WriteU64;
use super::write_all::{write_all, WriteAll};
use super::STATIC_PADDING;

#[derive(Debug)]
pub enum WriteStr<'a, W> {
    Invalid,
    WriteSize(&'a [u8], WriteU64<W>),
    WriteData(u8, WriteAll<'a, W>),
    WritePadding(WriteAll<'static, W>),
    Done(W),
}

pub(crate) fn write_string<'a, W>(dst: W, s:&'a str) -> WriteStr<'a, W>
{
    let buf = s.as_bytes();
    let len = buf.len();
    WriteStr::WriteSize(buf, WriteU64::new(dst, len as u64))
}

impl<'a, W> WriteStr<'a, W> {
    pub fn inner(self) -> W {
        match self {
            WriteStr::Invalid => panic!("invalid state"),
            WriteStr::WriteSize(_, w) => w.inner(),
            WriteStr::WriteData(_, w) => w.inner(),
            WriteStr::WritePadding(w) => w.inner(),
            WriteStr::Done(w) => w,
        }
    }
}


impl<'a, W> Future for WriteStr<'a, W>
    where W: AsyncWrite + Unpin
{
    type Output = io::Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        loop {
            match mem::replace(&mut *self, WriteStr::Invalid) {
                WriteStr::Invalid => panic!("invalid state"),
                WriteStr::Done(_) => panic!("polling completed future"),
                WriteStr::WriteSize(buf, mut writer) => {
                    match Pin::new(&mut writer).poll(cx) {
                        Poll::Pending => {
                            *self = WriteStr::WriteSize(buf, writer);
                            return Poll::Pending;
                        },
                        Poll::Ready(res) => res?,
                    }
                    let dst = writer.inner();
                    if buf.len() == 0 {
                        *self = WriteStr::Done(dst);
                        return Poll::Ready(Ok(()));
                    }
                    let padding = calc_padding(buf.len() as u64);
                    *self = WriteStr::WriteData(padding, write_all(dst, buf));
                },
                WriteStr::WriteData(padding, mut writer) => {
                    match Pin::new(&mut writer).poll(cx) {
                        Poll::Pending => {
                            *self = WriteStr::WriteData(padding, writer);
                            return Poll::Pending;
                        },
                        Poll::Ready(res) => res?,
                    }
                    let dst = writer.inner();
                    *self = WriteStr::WritePadding(write_all(dst, &STATIC_PADDING[..padding as usize]));
                },
                WriteStr::WritePadding(mut writer) => {
                    match Pin::new(&mut writer).poll(cx) {
                        Poll::Pending => {
                            *self = WriteStr::WritePadding(writer);
                            return Poll::Pending;
                        }
                        Poll::Ready(res) => res?,
                    }
                    *self = WriteStr::Done(writer.inner());
                    return Poll::Ready(Ok(()));
                }
            }    
        }
    }
}
