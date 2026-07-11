use bytes::{Buf, TryGetError};
use smallvec::SmallVec;

use crate::{BytesBuf, buf::panic_advance};

#[derive(Clone)]
pub struct Chunked<const N: usize, B>(ChunkedMut<N, B>);

impl<const N: usize, B: Buf> Buf for Chunked<N, B> {
    fn remaining(&self) -> usize {
        self.0.remaining()
    }

    fn chunk(&self) -> &[u8] {
        self.0.chunk()
    }

    fn advance(&mut self, cnt: usize) {
        self.0.advance(cnt);
    }

    fn copy_to_bytes(&mut self, len: usize) -> bytes::Bytes {
        self.0.copy_to_bytes(len)
    }
}

impl<const N: usize, B: BytesBuf> BytesBuf for Chunked<N, B> {
    fn empty() -> Self {
        Self(ChunkedMut::empty())
    }

    fn nth_chunk(&self, nth: usize) -> &[u8] {
        self.0.nth_chunk(nth)
    }

    fn truncate(&mut self, len: usize) {
        self.0.truncate(len);
    }

    fn split_off(&mut self, at: usize) -> Self {
        Self(self.0.split_off(at))
    }

    fn split_to(&mut self, at: usize) -> Self {
        Self(self.0.split_to(at))
    }
}

#[derive(Clone, Debug)]
pub struct ChunkedMut<const N: usize, B>(SmallVec<[B; N]>);

impl<const N: usize, B: BytesBuf> ChunkedMut<N, B> {
    pub fn from_vec(vec: SmallVec<[B; N]>) -> Self {
        Self(vec)
    }

    pub fn from_buf(buf: [B; N]) -> Self {
        Self::from_vec(SmallVec::from_buf(buf))
    }

    pub fn from_slice(slice: &[B]) -> Self
    where
        B: Clone,
    {
        Self::from_vec(SmallVec::from(slice))
    }

    pub fn push(&mut self, value: B) {
        self.0.push(value);
    }

    pub fn freeze(self) -> Chunked<N, B> {
        Chunked(self)
    }
}

impl<const N: usize, B: Buf> Buf for ChunkedMut<N, B> {
    fn remaining(&self) -> usize {
        self.0.iter().map(|b| b.remaining()).sum()
    }

    fn chunk(&self) -> &[u8] {
        self.0.first().map(|b| b.chunk()).unwrap_or_default()
    }

    fn advance(&mut self, mut cnt: usize) {
        let rem = self.remaining();
        if cnt > rem {
            panic_advance(&TryGetError {
                requested: cnt,
                available: rem,
            })
        }
        while let Some(chunk) = self.0.first() {
            if cnt < chunk.remaining() {
                break;
            }
            cnt -= chunk.remaining();
            self.0.remove(0);
        }
        if cnt > 0
            && let Some(chunk) = self.0.first_mut()
        {
            chunk.advance(cnt);
        }
    }

    fn copy_to_bytes(&mut self, len: usize) -> bytes::Bytes {
        // If len is within the first chunk call copy_to_bytes on that
        // This is to handle the case where the chunk can shallow-copy
        let first_rem = self.chunk().len();
        if first_rem > len {
            return self.0[0].copy_to_bytes(len);
        } else if first_rem == len {
            return self.0.remove(0).copy_to_bytes(len);
        }

        // Otherwise go with the slow default
        use bytes::buf::BufMut;

        if self.remaining() < len {
            panic_advance(&TryGetError {
                requested: len,
                available: self.remaining(),
            });
        }

        let mut ret = bytes::BytesMut::with_capacity(len);
        ret.put(self.take(len));
        ret.freeze()
    }
}

impl<const N: usize, B: BytesBuf> BytesBuf for ChunkedMut<N, B> {
    fn empty() -> Self {
        Self(SmallVec::default())
    }

    fn nth_chunk(&self, nth: usize) -> &[u8] {
        self.0.get(nth).map(|c| c.chunk()).unwrap_or_default()
    }

    fn truncate(&mut self, len: usize) {
        if len > self.remaining() || len == 0 {
            return;
        }
        let (chunk, index) = self
            .find_chunk_index(len - 1)
            .expect("BUG: find_chunk_index {len}-1 was out of bounds");
        self.0[chunk].truncate(index + 1);
        self.0.truncate(chunk + 1);
    }

    fn split_off(&mut self, at: usize) -> Self {
        let Some((chunk, index)) = self.find_chunk_index(at) else {
            let rem = self.remaining();
            if at == rem {
                return Self::empty();
            } else {
                panic!("{at} > {}", self.remaining())
            }
        };
        let mut new: SmallVec<[B; N]> = self.0.drain(chunk..).collect();
        if index > 0 {
            let other = new[0].split_to(index);
            self.0.push(other);
        }
        Self(new)
    }

    fn split_to(&mut self, at: usize) -> Self {
        let other = self.split_off(at);
        std::mem::replace(self, other)
    }
}
