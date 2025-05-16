use std::future::Future;
use std::io;
use std::io::ErrorKind::UnexpectedEof;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

use pin_project_lite::pin_project;
use tokio::io::AsyncRead;
use tokio::io::ReadBuf;

use crate::io::calc_padding;

fn invalid_data(s: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, s)
}

pin_project! {
    #[derive(Debug)]
    #[must_use = "futures do nothing unless you `.await` or poll them"]
    pub struct ReadPadding<R> {
        #[pin]
        src: R,
        zero: [u8; 8],
        read: u8,
        padding: u8,
    }
}

impl<R> ReadPadding<R> {
    pub(crate) fn new(src: R, size: u64) -> Self {
        ReadPadding {
            src,
            zero: [0; 8],
            read: 0,
            padding: calc_padding(size),
        }
    }

    pub fn inner(self) -> R {
        self.src
    }
}

impl<R: AsyncRead> Future for ReadPadding<R> {
    type Output = io::Result<()>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut me = self.project();
        if *me.read == *me.padding {
            if *me.padding != 0 {
                for i in &me.zero[..*me.padding as usize] {
                    if *i != 0 {
                        return Poll::Ready(Err(invalid_data("non-zero padding")));
                    }
                }
            }
            return Poll::Ready(Ok(()));
        }
        while *me.read < *me.padding {
            let mut buf = ReadBuf::new(&mut me.zero[*me.read as usize..*me.padding as usize]);

            *me.read += match me.src.as_mut().poll_read(cx, &mut buf) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Ready(Ok(())) => {
                    let n = buf.filled().len();
                    if n == 0 {
                        return Poll::Ready(Err(UnexpectedEof.into()));
                    }

                    n as u8
                }
            };
        }

        for i in &me.zero[..*me.padding as usize] {
            if *i != 0 {
                return Poll::Ready(Err(invalid_data("non-zero padding")));
            }
        }

        Poll::Ready(Ok(()))
    }
}
