use std::pin::Pin;
use std::task::{Context, ready, Poll};

use bytes::{Bytes, BytesMut, BufMut};
use pin_project_lite::pin_project;
use tokio::io::AsyncWrite;

#[derive(Debug)]
pub enum FramedSinkOp {
    WriteData(Bytes),
    Idle,
}

pin_project! {
    pub struct FramedSink<W> {
        state: FramedSinkOp,
        frame: usize,
        buf: BytesMut,
        shutdown: bool,
        #[pin]
        writer: W,
    }
}

impl<W: AsyncWrite> FramedSink<W> {
    pub fn new(writer: W) -> FramedSink<W> {
        Self::with_capacity(writer, 32 * 1024)
    }
    pub fn with_capacity(writer: W, capacity: usize) -> FramedSink<W> {
        FramedSink {
            state: FramedSinkOp::Idle,
            frame: 0,
            buf: BytesMut::with_capacity(capacity),
            shutdown: false,
            writer,
        }
    }
    pub fn poll_writing(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        let mut this = self.project();
        if let FramedSinkOp::WriteData(buf) = this.state {
            loop {
                let written = ready!(this.writer.as_mut().poll_write(cx, buf))?;
                if written < buf.len() {
                    // eprintln!("{} Truncate buf written={}", this.frame, written);
                    let _ = buf.split_to(written);
                } else {
                    // eprintln!("{} Written written={}", this.frame, written);
                    *this.state = FramedSinkOp::Idle;
                    break;
                }    
            }
        }
        Poll::Ready(Ok(()))
    }
}

impl<W: AsyncWrite> AsyncWrite for FramedSink<W> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        if let FramedSinkOp::WriteData(_) = &self.state {
            ready!(self.as_mut().poll_writing(cx))?;
        }
        let this = self.project();
        //let old_len = this.buf.len();
        this.buf.reserve(buf.len() + 8);
        this.buf.put_u64_le(buf.len() as u64);
        this.buf.extend_from_slice(buf);
        // eprintln!("{} Writing frame buf.len={} old_len={} this.buf={} this.buf.remaining={}", this.frame, buf.len(), old_len, this.buf.len(), this.buf.remaining());
        let next = this.buf.split().freeze();
        // eprintln!("{} Writing Next nex.len={} this.buf.len={}", this.frame, next.len(), this.buf.len());
        *this.frame += 1;
        *this.state = FramedSinkOp::WriteData(next);
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), std::io::Error>> {
        ready!(self.as_mut().poll_writing(cx))?;
        let this = self.project();
        this.writer.poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), std::io::Error>> {
        if !self.shutdown {
            ready!(self.as_mut().poll_write(cx, &[]))?;
            let this = self.as_mut().project();
            *this.shutdown = true;
        }
        self.poll_flush(cx)
    }
}


