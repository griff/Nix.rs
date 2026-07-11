use std::io;
use std::pin::Pin;
use std::task::{Context, Poll, ready};

use bytes::{Buf, BufMut as _};
use pin_project_lite::pin_project;
use taniwha_io::{AsyncBytesRead, BytesBuf, DrainInto};
use tokio::io::{AsyncRead, ReadBuf};

use super::{ZEROS, calc_aligned};

pin_project! {
    pub struct PaddedReader<R> {
        #[pin]
        reader: R,
        len: u64,
        aligned: u64,
    }
}

#[cfg_attr(
    not(any(feature = "internal", feature = "archive", test)),
    expect(dead_code)
)]
impl<R: AsyncBytesRead> PaddedReader<R> {
    pub fn new(reader: R, len: u64, aligned: u64) -> Self {
        debug_assert_eq!(aligned, calc_aligned(len));
        Self {
            reader,
            len,
            aligned,
        }
    }

    pub fn remaining(&self) -> u64 {
        self.len
    }

    pub fn remaining_padded(&self) -> u64 {
        self.aligned
    }

    pub fn padding(&self) -> u8 {
        (self.aligned - self.len) as u8
    }

    pub fn has_padding(&self) -> bool {
        self.padding() > 0
    }
}

impl<R> AsyncRead for PaddedReader<R>
where
    R: AsyncBytesRead,
{
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let rem = ready!(self.as_mut().poll_fill_buf(cx))?;
        if rem.has_remaining() {
            let amt = std::cmp::min(rem.remaining(), buf.remaining());
            buf.put(rem.take(amt));
            self.consume(amt);
        }
        Poll::Ready(Ok(()))
    }
}

impl<R> AsyncBytesRead for PaddedReader<R>
where
    R: AsyncBytesRead,
{
    type Buf = R::Buf;

    fn poll_fill_buf(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<Self::Buf>> {
        let mut this = self.project();
        if *this.aligned == 0 {
            return Poll::Ready(Ok(<Self::Buf as BytesBuf>::empty()));
        }

        let mut buf = ready!(this.reader.as_mut().poll_fill_buf(cx))?;
        if !buf.has_remaining() {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "EOF before end of padding",
            )));
        }

        // check if buf has all data that is to be returned
        if buf.remaining() >= *this.len as usize {
            // force the reading of any padding
            while buf.remaining() < *this.aligned as usize {
                buf = ready!(this.reader.as_mut().poll_force_fill_buf(cx))?;
            }
            buf.truncate(*this.aligned as usize);

            // Check that padding is zeros
            let padding_len = (*this.aligned - *this.len) as usize;
            if padding_len > 0 {
                let mut padding = ZEROS;
                let mut padding_buf = buf.split_off(*this.len as usize);
                padding_buf.copy_to_slice(&mut padding[..padding_len]);
                debug_assert!(
                    !padding_buf.has_remaining(),
                    "padding_buf has more bytes {} > 0",
                    padding_buf.remaining()
                );
                if padding != ZEROS {
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "Non-zero padding",
                    )));
                }
            }
        }
        Poll::Ready(Ok(buf))
    }

    fn poll_force_fill_buf(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<Self::Buf>> {
        let mut this = self.project();
        if *this.aligned == 0 {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "EOF while force reading",
            )));
        }

        let mut buf = ready!(this.reader.as_mut().poll_force_fill_buf(cx))?;
        if !buf.has_remaining() {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "EOF before end of padding",
            )));
        }

        // check if buf has all data that is to be returned
        if buf.remaining() >= *this.len as usize {
            // force the reading of any padding
            while buf.remaining() < *this.aligned as usize {
                buf = ready!(this.reader.as_mut().poll_force_fill_buf(cx))?;
            }
            buf.truncate(*this.aligned as usize);

            // Check that padding is zeros
            let padding_len = (*this.aligned - *this.len) as usize;
            if padding_len > 0 {
                let mut padding = ZEROS;
                let mut padding_buf = buf.split_off(*this.len as usize);
                padding_buf.copy_to_slice(&mut padding[..padding_len]);
                debug_assert!(
                    !padding_buf.has_remaining(),
                    "padding_buf has more bytes {} > 0",
                    padding_buf.remaining()
                );
                if padding != ZEROS {
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "Non-zero padding",
                    )));
                }
            }
        }
        Poll::Ready(Ok(buf))
    }

    fn prepare(self: Pin<&mut Self>, additional: usize) {
        self.project().reader.prepare(additional)
    }

    fn consume(self: Pin<&mut Self>, amt: usize) {
        let this = self.project();
        assert!(amt <= *this.len as usize, "Consuming more than content");
        if amt as u64 == *this.len {
            this.reader.consume(*this.aligned as usize);
            *this.len = 0;
            *this.aligned = 0;
        } else {
            this.reader.consume(amt);
            *this.len -= amt as u64;
            *this.aligned -= amt as u64;
        }
    }
}

impl<R> DrainInto<R> for PaddedReader<R>
where
    R: AsyncBytesRead,
{
    fn poll_drain(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        loop {
            let buf = ready!(self.as_mut().poll_fill_buf(cx))?;
            if !buf.has_remaining() {
                break;
            }
            self.as_mut().consume(buf.remaining());
        }
        Poll::Ready(Ok(()))
    }

    fn into_inner(self) -> R {
        self.reader
    }
}

#[cfg(test)]
mod unittests {
    use std::future::poll_fn;
    use std::io;
    use std::pin::pin;

    use bytes::Bytes;
    use rstest::rstest;
    use taniwha_io::AsyncBytesRead as _;

    use super::PaddedReader;
    use crate::wire::calc_aligned;

    #[tokio::test]
    #[rstest]
    #[case::part_content(0, 5, 17, b" world! From Vino\0\0After")]
    #[case::rest_of_content(5, 11, 6, b"m Vino\0\0After")]
    #[case::all_content(0, 16, 6, b"m Vino\0\0After")]
    #[case::all_content_some_of_trailer(0, 19, 3, b"ino\0\0After")]
    #[case::all_content_all_trailer(0, 22, 0, b"After")]
    #[case::some_content_some_trailer(5, 14, 3, b"ino\0\0After")]
    #[case::some_trailer(16, 3, 3, b"ino\0\0After")]
    #[case::all_trailer(16, 6, 0, b"After")]
    #[case::rest_of_trailer(19, 3, 0, b"After")]
    #[case::no_consume(22, 0, 0, b"After")]
    async fn test_consume(
        #[case] pre: usize,
        #[case] consume: usize,
        #[case] remaining: u64,
        #[case] left: &'static [u8],
    ) {
        let input = Bytes::from_static(b"Hello world! From Vino\0\0After");
        let mut reader = io::Cursor::new(input.clone());
        let padded_reader = PaddedReader::new(&mut reader, 22, calc_aligned(22));
        {
            let mut padded = pin!(padded_reader);
            if pre > 0 {
                padded.as_mut().consume(pre);
            }
            padded.as_mut().consume(consume);
            let actual = padded.as_mut().remaining();
            assert_eq!(actual, remaining);
        }
        let mut reader = pin!(reader);
        let expected = poll_fn(|cx| reader.as_mut().poll_fill_buf(cx))
            .await
            .unwrap();
        assert_eq!(Bytes::from_static(left), expected);
    }

    #[tokio::test]
    #[should_panic(expected = "Consuming more than content")]
    async fn test_consume_assert() {
        let input = Bytes::from_static(b"Hello world! From Vino\0\0After");
        let mut reader = io::Cursor::new(input.clone());
        let padded_reader = PaddedReader::new(&mut reader, 22, calc_aligned(22));
        let mut padded = pin!(padded_reader);
        padded.as_mut().consume(23);
    }

    // Read to end
    // fill_buf
    // force_fill_buf
    // TryReadBytesLimited
    // Read line
}
