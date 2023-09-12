use std::future::Future;
use std::io;
use std::mem;
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::AsyncWrite;

use crate::io::calc_padding;

use super::write_all::{write_all, WriteAll};
use super::write_int::WriteU64;
use super::STATIC_PADDING;


pub(crate) fn write_string<W>(dst: W, s: String) -> WriteString<W> {
    let len = s.as_bytes().len();
    WriteString::WriteSize(s, WriteU64::new(dst, len as u64))
}


#[derive(Debug)]
pub enum WriteString<W> {
    Invalid,
    WriteSize(String, WriteU64<W>),
    WriteData(u8, String, usize, W),
    WritePadding(WriteAll<'static, W>),
    Done(W),
}


impl<W> WriteString<W> {
    pub fn inner(self) -> W {
        match self {
            WriteString::Invalid => panic!("invalid state"),
            WriteString::WriteSize(_, w) => w.inner(),
            WriteString::WriteData(_, _, _, w) => w,
            WriteString::WritePadding(w) => w.inner(),
            WriteString::Done(w) => w,
        }
    }
}

impl<W> Future for WriteString<W>
where
    W: AsyncWrite + Unpin,
{
    type Output = io::Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        loop {
            match mem::replace(&mut *self, WriteString::Invalid) {
                WriteString::Invalid => panic!("invalid state"),
                WriteString::Done(_) => panic!("polling completed future"),
                WriteString::WriteSize(buf, mut writer) => {
                    match Pin::new(&mut writer).poll(cx) {
                        Poll::Pending => {
                            *self = WriteString::WriteSize(buf, writer);
                            return Poll::Pending;
                        }
                        Poll::Ready(res) => res?,
                    }
                    let dst = writer.inner();
                    if buf.len() == 0 {
                        *self = WriteString::Done(dst);
                        return Poll::Ready(Ok(()));
                    }
                    let padding = calc_padding(buf.len() as u64);
                    *self = WriteString::WriteData(padding, buf, 0, dst);
                }
                WriteString::WriteData(padding, buf, mut written, mut writer) => {
                    let b = buf.as_bytes();
                    loop {
                        let remaining = &b[written..];
                        let next = match Pin::new(&mut writer).poll_write(cx, remaining) {
                            Poll::Pending => {
                                *self = WriteString::WriteData(padding, buf, written, writer);
                                return Poll::Pending;
                            }
                            Poll::Ready(res) => res?,
                        };
                        written += next;
                        if written >= b.len() {
                            break;
                        }
                    }
                    *self =
                    WriteString::WritePadding(write_all(writer, &STATIC_PADDING[..padding as usize]));
                }
                WriteString::WritePadding(mut writer) => {
                    match Pin::new(&mut writer).poll(cx) {
                        Poll::Pending => {
                            *self = WriteString::WritePadding(writer);
                            return Poll::Pending;
                        }
                        Poll::Ready(res) => res?,
                    }
                    *self = WriteString::Done(writer.inner());
                    return Poll::Ready(Ok(()));
                }
            }
        }
    }
}

