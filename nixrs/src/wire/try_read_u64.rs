use std::{
    future::poll_fn,
    io,
    pin::{Pin, pin},
    task::{Context, Poll, ready},
};

use bytes::Buf;
use tokio::io::{AsyncRead, ReadBuf};

#[derive(Debug, Clone)]
pub struct TryReadU64 {
    buf: [u8; 8],
    read: u8,
}

impl Default for TryReadU64 {
    fn default() -> Self {
        Self::new()
    }
}

impl TryReadU64 {
    pub fn new() -> Self {
        Self {
            buf: [0u8; 8],
            read: 0,
        }
    }
    pub async fn read<R: AsyncRead>(mut self, reader: R) -> io::Result<Option<u64>> {
        let mut reader = pin!(reader);
        poll_fn(move |cx| self.poll_reader(cx, reader.as_mut())).await
    }
    pub fn poll_reader<R>(
        &mut self,
        cx: &mut Context<'_>,
        mut reader: Pin<&mut R>,
    ) -> Poll<io::Result<Option<u64>>>
    where
        R: AsyncRead,
    {
        while self.read < 8 {
            let mut buf = ReadBuf::new(&mut self.buf[(self.read as usize)..]);
            ready!(reader.as_mut().poll_read(cx, &mut buf))?;
            if buf.filled().is_empty() {
                if self.read == 0 {
                    return Poll::Ready(Ok(None));
                } else {
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "EOF reading u64",
                    )));
                }
            }
            self.read += buf.filled().len() as u8;
        }
        let num = Buf::get_u64_le(&mut &self.buf[..]);
        Poll::Ready(Ok(Some(num)))
    }
}

#[cfg(test)]
mod unittests {
    use std::time::Duration;

    use hex_literal::hex;
    use taniwha_io::BytesReader;
    use tokio::io::AsyncReadExt as _;
    use tokio_test::io::Builder;

    use super::TryReadU64;

    #[test_log::test(tokio::test)]
    async fn test_read_u64_partial() {
        let mock = Builder::new()
            .read(&hex!("0100 0000"))
            .wait(Duration::ZERO)
            .read(&hex!("0000 0000 0123 4567 89AB CDEF"))
            .wait(Duration::ZERO)
            .read(&hex!("0100 0000"))
            .build();
        let mut reader = BytesReader::new(mock);

        assert_eq!(
            1,
            TryReadU64::new().read(&mut reader).await.unwrap().unwrap()
        );
        assert_eq!(hex!("0123 4567 89AB CDEF"), reader.buffer());

        let mut buf = Vec::new();
        reader.read_to_end(&mut buf).await.unwrap();
        assert_eq!(hex!("0123 4567 89AB CDEF 0100 0000"), &buf[..]);
    }
}
