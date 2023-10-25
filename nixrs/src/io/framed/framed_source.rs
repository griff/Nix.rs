use std::io;
use std::pin::Pin;
use std::task::{ready, Poll};

use bytes::Buf;
use pin_project_lite::pin_project;
use tokio::io::{AsyncRead, AsyncReadExt, ReadBuf};
use tracing::{debug, trace};

#[derive(Debug)]
pub enum FramedSourceOp {
    ReadSize(u8, [u8; 8]),
    ReadData(u64),
    Idle,
    Eof,
    Invalid,
}

pin_project! {
    #[derive(Debug)]
    pub struct FramedSource<R> {
        state: FramedSourceOp,
        frame: usize,
        #[pin]
        reader: R,
    }
}

impl<R: AsyncRead + Unpin> FramedSource<R> {
    pub fn new(reader: R) -> FramedSource<R> {
        FramedSource {
            state: FramedSourceOp::Idle,
            frame: 0,
            reader,
        }
    }

    pub async fn drain(mut self) -> io::Result<()> {
        if let FramedSourceOp::Eof = self.state {
            return Ok(());
        }
        let mut buf = [0u8; 65536];
        loop {
            let read = self.read(&mut buf).await?;
            if read == 0 {
                return Ok(());
            }
            trace!("Read drain {}", read);
        }
    }
}

impl<R: AsyncRead> AsyncRead for FramedSource<R> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let mut this = self.project();
        loop {
            match std::mem::replace(this.state, FramedSourceOp::Invalid) {
                FramedSourceOp::Invalid => panic!("Invalid framed reader state"),
                FramedSourceOp::ReadSize(mut read, mut sbuf) => {
                    while read < 8u8 {
                        // eprintln!("{} ReadSize read={}", this.frame, read);
                        let mut read_buf = ReadBuf::new(&mut sbuf[read as usize..]);
                        match this.reader.as_mut().poll_read(cx, &mut read_buf) {
                            Poll::Pending => {
                                *this.state = FramedSourceOp::ReadSize(read, sbuf);
                                return Poll::Pending;
                            }
                            Poll::Ready(Ok(_)) => (),
                            Poll::Ready(Err(err)) => {
                                *this.state = FramedSourceOp::ReadSize(read, sbuf);
                                return Poll::Ready(Err(err));
                            }
                        }

                        let n = read_buf.filled().len();
                        if n == 0 {
                            *this.state = FramedSourceOp::ReadSize(read, sbuf);
                            // eprintln!("EOF reading size");
                            return Poll::Ready(Err(io::ErrorKind::UnexpectedEof.into()));
                        }

                        read += n as u8
                    }
                    let size = Buf::get_u64_le(&mut &sbuf[..]);
                    // eprintln!("{} Reading frame size={}", this.frame, size);
                    *this.frame += 1;
                    if size == 0 {
                        *this.state = FramedSourceOp::Eof;
                        return Poll::Ready(Ok(()));
                    }
                    *this.state = FramedSourceOp::ReadData(size);
                }
                FramedSourceOp::ReadData(mut left) => {
                    if left == 0 {
                        *this.state = FramedSourceOp::Idle;
                        continue;
                    }
                    *this.state = FramedSourceOp::ReadData(left);
                    // eprintln!("{} ReadData buf.remaining={}", this.frame, buf.remaining());
                    if buf.remaining() == 0 {
                        return Poll::Ready(Ok(()));
                    }
                    let old_filled = buf.filled().len();
                    let read = if left < buf.remaining() as u64 {
                        let unfilled = unsafe { buf.unfilled_mut() };
                        let mut read_buf = ReadBuf::uninit(&mut unfilled[0..left as usize]);
                        ready!(this.reader.as_mut().poll_read(cx, &mut read_buf))?;
                        let read = read_buf.filled().len();
                        unsafe { buf.assume_init(read) };
                        buf.advance(read);
                        debug!(read, left, "Read small buf {}", read);
                        read
                    } else {
                        ready!(this.reader.as_mut().poll_read(cx, buf))?;
                        debug!(
                            old_filled,
                            left,
                            filled = buf.filled().len(),
                            read = buf.filled().len() - old_filled,
                            "Read buf"
                        );
                        buf.filled().len() - old_filled
                    };
                    if read == 0 {
                        // eprintln!("{} EOF reading data {} {} {} {}", this.frame, left, buf.remaining(), buf.filled().len(), old_filled);
                        return Poll::Ready(Err(io::ErrorKind::UnexpectedEof.into()));
                    }

                    left -= read as u64;
                    if left == 0 {
                        *this.state = FramedSourceOp::Idle;
                    } else {
                        *this.state = FramedSourceOp::ReadData(left);
                    }
                    // eprintln!("{} Reading done read={} left={} old_filled={}", this.frame, read, left, old_filled);

                    return Poll::Ready(Ok(()));
                }
                FramedSourceOp::Idle => {
                    let sbuf = [0u8; 8];
                    *this.state = FramedSourceOp::ReadSize(0, sbuf);
                }
                FramedSourceOp::Eof => {
                    return Poll::Ready(Ok(()));
                }
            }
        }
    }
}
