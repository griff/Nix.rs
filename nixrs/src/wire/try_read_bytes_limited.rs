use std::fmt;
use std::future::poll_fn;
use std::io;
use std::ops::RangeInclusive;
use std::pin::Pin;
use std::task::{Context, Poll, ready};

use bytes::{Buf, Bytes};
use taniwha_io::AsyncBytesRead;
use tracing::field::Empty;
use tracing::{Span, trace_span};

use super::{TryReadU64, ZEROS, checked_calc_aligned};

#[derive(Debug, Clone)]
pub enum TryReadBytesLimited {
    ReadLen(RangeInclusive<usize>, TryReadU64, Span),
    Fill(usize, usize, Span),
    Done,
}

fn invalid_data<T: fmt::Display>(msg: T) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg.to_string())
}

impl TryReadBytesLimited {
    pub fn new(limit: RangeInclusive<usize>) -> Self {
        let span = trace_span!("TryReadBytesLimited", ?limit, len = Empty, aligned = Empty);
        Self::ReadLen(limit, TryReadU64::new(), span)
    }
    pub async fn read<R: AsyncBytesRead + Unpin>(
        mut self,
        reader: &mut R,
    ) -> io::Result<Option<Bytes>> {
        let mut reader = Pin::new(reader);
        poll_fn(move |cx| self.poll_reader(cx, reader.as_mut())).await
    }

    pub fn poll_reader<R>(
        &mut self,
        cx: &mut Context<'_>,
        mut reader: Pin<&mut R>,
    ) -> Poll<io::Result<Option<Bytes>>>
    where
        R: AsyncBytesRead,
    {
        loop {
            match self {
                Self::ReadLen(limit, try_read_u64, span) => {
                    let _guard = span.clone().entered();
                    if let Some(raw_len) = ready!(try_read_u64.poll_reader(cx, reader.as_mut()))? {
                        // Check that length is in range and convert to usize
                        let len = raw_len
                            .try_into()
                            .ok()
                            .filter(|v| limit.contains(v))
                            .ok_or_else(|| invalid_data("bytes length out of range"))?;
                        span.record("len", len);

                        // Calculate 64bit aligned length and convert to usize
                        let aligned: usize = checked_calc_aligned(raw_len)
                            .ok_or_else(|| invalid_data("aligned bytes length out of range"))?
                            .try_into()
                            .map_err(invalid_data)?;
                        span.record("aligned", aligned);

                        if aligned > 0 {
                            // Ensure that there is enough space in buffer for contents
                            reader.as_mut().prepare(aligned);
                            *self = Self::Fill(len, aligned, span.clone());
                        } else {
                            *self = Self::Done;
                            return Poll::Ready(Ok(Some(Bytes::new())));
                        }
                    } else {
                        *self = Self::Done;
                        return Poll::Ready(Ok(None));
                    }
                }
                Self::Fill(len, aligned, span) => {
                    let _guard = span.clone().entered();
                    let mut buf = ready!(reader.as_mut().poll_fill_buf(cx))?;
                    while buf.remaining() < *aligned {
                        buf = ready!(reader.as_mut().poll_force_fill_buf(cx))?;
                    }
                    let mut contents = buf.copy_to_bytes(*aligned);
                    reader.as_mut().consume(*aligned);

                    let padding = *aligned - *len;
                    // Ensure padding is all zeros
                    if contents[*len..] != ZEROS[..padding] {
                        *self = Self::Done;
                        return Poll::Ready(Err(invalid_data("non-zero padding")));
                    }

                    contents.truncate(*len);
                    *self = Self::Done;
                    return Poll::Ready(Ok(Some(contents)));
                }
                Self::Done => panic!("Polling completed future"),
            }
        }
    }
}

#[cfg(test)]
mod unittests {
    use std::time::Duration;

    use hex_literal::hex;
    use taniwha_io::BytesReader;
    use tokio_test::io::Builder;

    use super::TryReadBytesLimited;

    #[tokio::test]
    async fn test_try_read_bytes_missing_padding() {
        let mock = Builder::new()
            .read(&hex!("0200 0000 0000 0000"))
            .wait(Duration::ZERO)
            .read(&hex!("1234"))
            .build();
        let mut reader = BytesReader::new(mock);

        let ret = TryReadBytesLimited::new(0..=usize::MAX)
            .read(&mut reader)
            .await;

        assert_eq!(std::io::ErrorKind::UnexpectedEof, ret.unwrap_err().kind());
    }
}
