use std::io;
use std::pin::Pin;
use std::task::{Context, Poll, ready};

use bytes::{Buf, BufMut as _, Bytes};
use pin_project_lite::pin_project;
use tokio::io::AsyncRead;
use tracing::{debug, trace};

use crate::io::{AsyncBytesRead, BytesBuf, Chunked, ChunkedMut};
use crate::wire::TryReadU64;

pub const FRAMES_STACK: usize = 4;

#[derive(Debug, Default)]
enum FramedReadState {
    ReadLen(TryReadU64),
    ReadData(u64),
    #[default]
    Eof,
}

pin_project! {
    #[derive(Debug)]
    pub struct FramedReader<R: AsyncBytesRead> {
        #[pin]
        reader: R,
        frames: ChunkedMut<FRAMES_STACK, Bytes>,
        state: FramedReadState,
    }
}

impl<R> FramedReader<R>
where
    R: AsyncBytesRead,
{
    pub fn new(reader: R) -> Self {
        Self {
            reader,
            frames: ChunkedMut::empty(),
            state: FramedReadState::ReadLen(TryReadU64::new()),
        }
    }

    fn poll_reading(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<u64>> {
        let mut me = self.project();
        loop {
            match me.state {
                FramedReadState::ReadLen(r) => {
                    trace!("FramedReader:poll_fill_buf: ReadLen");
                    let len = ready!(r.poll_reader(cx, me.reader.as_mut()))?.ok_or_else(|| {
                        io::Error::new(io::ErrorKind::UnexpectedEof, "EOF in framed reader")
                    })?;
                    debug!(len, "FramedReader:poll_fill_buf: reading frame");
                    if len > 0 {
                        *me.state = FramedReadState::ReadData(len);
                    } else {
                        *me.state = FramedReadState::Eof;
                    }
                }
                FramedReadState::ReadData(remaining) => {
                    return Poll::Ready(Ok(*remaining));
                }
                FramedReadState::Eof => {
                    return Poll::Ready(Ok(0));
                }
            }
        }
    }

    fn poll_buffer(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<usize>> {
        let remaining = ready!(self.as_mut().poll_reading(cx))?;
        if remaining == 0 {
            return Poll::Ready(Ok(0));
        }
        trace!(remaining, "FramedReader:poll_fill_buf: ReadData");
        let mut buf = ready!(self.as_mut().project().reader.poll_fill_buf(cx))?;
        trace!(
            buf.len = buf.remaining(),
            "FramedReader:poll_fill_buf: Got data"
        );
        if !buf.has_remaining() {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "EOF in framed reader",
            )));
        }
        let amt = std::cmp::min(buf.remaining(), remaining as usize);
        let buf = buf.copy_to_bytes(amt);
        self.as_mut().project().frames.push(buf);
        self.as_mut().consume_reader(amt);
        Poll::Ready(Ok(amt))
    }

    fn consume_reader(self: Pin<&mut Self>, amt: usize) {
        let me = self.project();
        match me.state {
            FramedReadState::ReadData(remaining) => {
                trace!(
                    remaining,
                    amt,
                    new_remaining = *remaining - amt as u64,
                    "FramedReader:consume"
                );
                *remaining -= amt as u64;
                if *remaining == 0 {
                    debug!("FramedReader::consume Consumed frame");
                    *me.state = FramedReadState::ReadLen(TryReadU64::new());
                }
                me.reader.consume(amt);
            }
            _ => panic!("Consume called in invalid state {amt}"),
        }
    }
}

impl<R> AsyncRead for FramedReader<R>
where
    R: AsyncBytesRead,
{
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let rem = ready!(self.as_mut().poll_fill_buf(cx))?;
        if rem.has_remaining() {
            let amt = std::cmp::min(rem.remaining(), buf.remaining());
            buf.put(rem.take(amt));
            self.consume(amt);
        }
        Poll::Ready(Ok(()))
    }
}

impl<R> AsyncBytesRead for FramedReader<R>
where
    R: AsyncBytesRead,
{
    type Buf = Chunked<FRAMES_STACK, Bytes>;

    fn poll_fill_buf(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<Self::Buf>> {
        if self.as_ref().frames.has_remaining() {
            return Poll::Ready(Ok(self.as_ref().frames.clone().freeze()));
        }
        ready!(self.as_mut().poll_buffer(cx))?;
        Poll::Ready(Ok(self.as_ref().frames.clone().freeze()))
    }

    fn consume(self: Pin<&mut Self>, amt: usize) {
        self.project().frames.split_to(amt);
    }

    fn poll_force_fill_buf(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<Self::Buf>> {
        let read = ready!(self.as_mut().poll_buffer(cx))?;
        if read == 0 {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "EOF in framed reader",
            )));
        }
        Poll::Ready(Ok(self.as_ref().frames.clone().freeze()))
    }

    fn prepare(self: Pin<&mut Self>, additional: usize) {
        self.project().reader.prepare(additional);
    }
}

#[cfg(test)]
mod unittests {
    use hex_literal::hex;
    use tokio::io::AsyncReadExt as _;
    use tokio_test::io::Builder;

    use super::*;
    use crate::io::BytesReader;

    #[tokio::test]
    async fn test_read_frames() {
        let mut mock = BytesReader::builder().set_max_buf_size(3).build(
            Builder::new()
                .read(&hex!(
                    "0100 0000 0000 0000 20 0400 0000 0000 0000 4142 4344"
                ))
                .read(&hex!("0100 0000 0000 0000 45 0000 0000 0000 0000 46"))
                .build(),
        );
        let mut reader = FramedReader::new(&mut mock);

        let mut s = String::new();
        reader.read_to_string(&mut s).await.unwrap();
        assert_eq!(s, " ABCDE");

        let mut buf = Vec::new();
        mock.read_to_end(&mut buf).await.unwrap();
        assert_eq!(hex!("46"), &buf[..]);
    }
}
