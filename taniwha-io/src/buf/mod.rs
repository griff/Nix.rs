use std::{collections::VecDeque, io::IoSlice};

use bytes::{Buf, Bytes, TryGetError, buf::Take};

mod buffer;
mod chunked;
pub use buffer::{BufferMut, Filled};
pub use chunked::{Chunked, ChunkedMut};

pub trait BytesBuf: Buf {
    fn empty() -> Self;

    /// This function should never panic. `nth_chunk(0)` (i.e. `nth_chunk(0)`)
    /// should return an empty slice **if and only if** `remaining()` returns 0.
    /// In other words, `nth_chunk(0)` returning an empty slice implies that
    /// `remaining()` will return 0 and `remaining()` returning 0 implies that
    /// `nth_chunk(0)` will return an empty slice.
    ///
    /// If `nth_chunk(n)` returns an empty slice, then `nth_chunk(m)` must also
    /// return an empty slice for all `m >= n`.
    fn nth_chunk(&self, nth: usize) -> &[u8] {
        if nth == 0 { self.chunk() } else { &[] }
    }

    fn find_chunk_index(&self, mut index: usize) -> Option<(usize, usize)> {
        if index >= self.remaining() {
            return None;
        }
        let mut nth = 0;
        let mut chunk = self.nth_chunk(nth);
        while chunk.len() <= index && !chunk.is_empty() {
            index -= chunk.len();
            nth += 1;
            chunk = self.nth_chunk(nth);
        }
        debug_assert!(!chunk.is_empty());
        Some((nth, index))
    }

    fn truncate(&mut self, len: usize);
    fn split_off(&mut self, at: usize) -> Self;
    fn split_to(&mut self, at: usize) -> Self;

    fn limit(self, limit: usize) -> Limited<Self>
    where
        Self: Sized,
    {
        Limited(self.take(limit))
    }

    fn peek(self) -> Peek<Self>
    where
        Self: Sized,
    {
        Peek {
            original: self,
            absolute: 0,
            chunk: 0,
            chunk_index: 0,
        }
    }
}

impl BytesBuf for bytes::Bytes {
    fn empty() -> Self {
        bytes::Bytes::new()
    }

    fn truncate(&mut self, len: usize) {
        self.truncate(len);
    }

    fn split_off(&mut self, at: usize) -> Self {
        self.split_off(at)
    }

    fn split_to(&mut self, at: usize) -> Self {
        self.split_to(at)
    }
}
impl BytesBuf for &[u8] {
    fn empty() -> Self {
        &[]
    }

    fn truncate(&mut self, len: usize) {
        *self = &self[..len];
    }

    fn split_off(&mut self, at: usize) -> Self {
        let (left, right) = self.split_at(at);
        *self = left;
        right
    }

    fn split_to(&mut self, at: usize) -> Self {
        let (left, right) = self.split_at(at);
        *self = right;
        left
    }
}
impl BytesBuf for VecDeque<u8> {
    fn empty() -> Self {
        Self::new()
    }

    fn truncate(&mut self, len: usize) {
        self.truncate(len);
    }

    fn split_off(&mut self, at: usize) -> Self {
        self.split_off(at)
    }

    fn split_to(&mut self, at: usize) -> Self {
        let other = self.split_off(at);
        std::mem::replace(self, other)
    }
}

impl<T: BytesBuf> BytesBuf for Box<T> {
    fn nth_chunk(&self, nth: usize) -> &[u8] {
        (**self).nth_chunk(nth)
    }

    fn empty() -> Self {
        Box::new(T::empty())
    }

    fn truncate(&mut self, len: usize) {
        (**self).truncate(len);
    }

    fn split_off(&mut self, at: usize) -> Self {
        Box::new((**self).split_off(at))
    }

    fn split_to(&mut self, at: usize) -> Self {
        Box::new((**self).split_to(at))
    }
}

impl<T: BytesBuf> BytesBuf for Take<T> {
    fn nth_chunk(&self, nth: usize) -> &[u8] {
        let actual_limit = std::cmp::min(self.limit(), self.get_ref().remaining());
        if actual_limit > 0
            && let Some((limit_nth, index)) = self.get_ref().find_chunk_index(actual_limit - 1)
        {
            if nth < limit_nth {
                self.get_ref().nth_chunk(nth)
            } else {
                &self.get_ref().nth_chunk(limit_nth)[..index]
            }
        } else {
            &[]
        }
    }

    fn empty() -> Self {
        T::empty().take(0)
    }

    fn truncate(&mut self, len: usize) {
        self.get_mut().truncate(len);
    }

    fn split_off(&mut self, at: usize) -> Self {
        let rem = self.remaining();
        if at > rem {
            panic_advance(&TryGetError {
                requested: at,
                available: rem,
            })
        }
        let ret = self.get_mut().split_off(at);
        self.set_limit(at);
        ret.take(rem - at)
    }

    fn split_to(&mut self, at: usize) -> Self {
        let rem = self.remaining();
        if at > rem {
            panic_advance(&TryGetError {
                requested: at,
                available: rem,
            })
        }
        let ret = self.get_mut().split_to(at);
        self.set_limit(rem - at);
        ret.take(at)
    }
}

/// Panic with a nice error message.
#[cold]
pub(crate) fn panic_advance(error_info: &TryGetError) -> ! {
    panic!(
        "advance out of bounds: the len is {} but advancing by {}",
        error_info.available, error_info.requested
    );
}

pub struct Peek<B> {
    original: B,
    absolute: usize,
    chunk: usize,
    chunk_index: usize,
}

impl<B> Peek<B> {
    pub fn into_inner(self) -> B {
        self.original
    }
}

impl<B: BytesBuf> Buf for Peek<B> {
    fn remaining(&self) -> usize {
        self.original.remaining() - self.absolute
    }

    fn chunk(&self) -> &[u8] {
        &self.original.nth_chunk(self.chunk)[..self.chunk_index]
    }

    fn advance(&mut self, mut cnt: usize) {
        while cnt > 0 {
            let chunk = self.original.nth_chunk(self.chunk);
            let rem = chunk.len() - self.chunk_index;
            if rem <= cnt {
                cnt -= rem;
                self.absolute += rem;
                self.chunk += 1;
                self.chunk_index = 0;
            } else {
                self.absolute += cnt;
                self.chunk_index += cnt;
                cnt = 0;
            }
        }
    }
}

impl<B: BytesBuf> BytesBuf for Peek<B> {
    fn empty() -> Self {
        Self {
            original: B::empty(),
            absolute: 0,
            chunk: 0,
            chunk_index: 0,
        }
    }

    fn nth_chunk(&self, nth: usize) -> &[u8] {
        if nth == 0 && self.chunk_index > 0 {
            self.chunk()
        } else {
            self.original.nth_chunk(nth + self.chunk)
        }
    }

    fn find_chunk_index(&self, index: usize) -> Option<(usize, usize)> {
        if let Some((chunk, index)) = self.original.find_chunk_index(index + self.absolute) {
            if chunk > self.chunk {
                Some((chunk - self.chunk, index))
            } else {
                Some((0, index - self.chunk_index))
            }
        } else {
            None
        }
    }

    fn truncate(&mut self, len: usize) {
        self.original.truncate(len + self.absolute);
    }

    fn split_off(&mut self, at: usize) -> Self {
        let original = self.original.split_off(at + self.absolute);
        Peek {
            original,
            absolute: 0,
            chunk: 0,
            chunk_index: 0,
        }
    }

    fn split_to(&mut self, at: usize) -> Self {
        let original = self.original.split_to(at + self.absolute);
        let other = Peek {
            original,
            absolute: self.absolute,
            chunk: self.chunk,
            chunk_index: self.chunk_index,
        };
        self.absolute = 0;
        self.chunk = 0;
        self.chunk_index = 0;
        other
    }
}

pub struct Limited<B>(bytes::buf::Take<B>);

impl<B: BytesBuf> BytesBuf for Limited<B> {
    fn nth_chunk(&self, nth: usize) -> &[u8] {
        self.0.nth_chunk(nth)
    }

    fn empty() -> Self {
        Limited(B::empty().take(0))
    }

    fn truncate(&mut self, len: usize) {
        self.0.truncate(len);
    }

    fn split_off(&mut self, at: usize) -> Self {
        Limited(self.0.split_off(at))
    }

    fn split_to(&mut self, at: usize) -> Self {
        Limited(self.0.split_to(at))
    }
}

impl<B: Buf> Buf for Limited<B> {
    fn remaining(&self) -> usize {
        self.0.remaining()
    }

    fn chunk(&self) -> &[u8] {
        self.0.chunk()
    }

    fn advance(&mut self, cnt: usize) {
        self.0.advance(cnt)
    }

    fn copy_to_bytes(&mut self, len: usize) -> Bytes {
        self.0.copy_to_bytes(len)
    }

    fn chunks_vectored<'a>(&'a self, dst: &mut [IoSlice<'a>]) -> usize {
        self.0.chunks_vectored(dst)
    }
}
