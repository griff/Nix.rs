use std::io;
use std::time::{Duration, SystemTime};

use futures::future::{self, Either, Ready};
use tokio::io::AsyncWrite;

use super::state_print::StatePrint;
use super::CollectionSize;

mod map_printed_state;
mod write_all;
mod write_int;
mod write_owned_string_coll;
mod write_slice;
mod write_string;
mod write_string_coll;

use self::map_printed_state::{MapPrintedColl, MapPrintedState};
use self::write_int::WriteU64;
use self::write_owned_string_coll::{write_owned_string_coll, WriteOwnedStringColl};
use self::write_slice::{write_buf, write_str, WriteSlice};
use self::write_string::{write_string, WriteString};
use self::write_string_coll::{write_string_coll, WriteStringColl};



fn write_u64<W>(dst: &mut W, value: u64) -> WriteU64<&mut W> {
    WriteU64::new(dst, value)
}

pub trait AsyncSink {
    //fn write_u64(&mut self, value: u64) -> WriteU64<&mut Self>;
    fn write_usize(&mut self, value: usize) -> WriteU64<&mut Self>;
    fn write_bool(&mut self, value: bool) -> WriteU64<&mut Self>;
    fn write_enum<V: Into<u64>>(&mut self, value: V) -> WriteU64<&mut Self>;
    fn write_flag<V: Into<bool>>(&mut self, value: V) -> WriteU64<&mut Self>;
    fn write_seconds(&mut self, duration: Duration) -> WriteU64<&mut Self>;
    fn write_time(
        &mut self,
        time: SystemTime,
    ) -> Either<WriteU64<&mut Self>, Ready<io::Result<()>>>;
    fn write_buf<'a>(&mut self, buf: &'a [u8]) -> WriteSlice<'a, &mut Self>;
    fn write_str<'a>(&mut self, s: &'a str) -> WriteSlice<'a, &mut Self>;
    fn write_string(&mut self, s: String) -> WriteString<&mut Self>;
    fn write_string_coll<'a, C, I>(&mut self, coll: C) -> WriteStringColl<'a, &mut Self, I>
    where
        C: CollectionSize + IntoIterator<Item = &'a String, IntoIter = I>,
        I: Iterator<Item = &'a String>;
    fn write_printed<S, I>(&mut self, state: S, item: &I) -> WriteString<&mut Self>
    where
        S: StatePrint<I>;
    fn write_printed_coll<'async_trait, 'item, C, S, IT, I>(
        &mut self,
        state: S,
        coll: C,
    ) -> WriteOwnedStringColl<&mut Self, MapPrintedState<S, IT>>
    where
        'item: 'async_trait,
        S: StatePrint<I> + 'async_trait,
        C: CollectionSize + IntoIterator<Item = &'item I, IntoIter = IT>,
        IT: Iterator<Item = &'item I> + 'async_trait,
        I: 'item;
}

impl<W> AsyncSink for W
where
    W: AsyncWrite,
{
    /*
    fn write_u64(&mut self, value: u64) -> WriteU64<&mut Self> {
        WriteU64::new(self, value)
    }
    */

    fn write_usize(&mut self, value: usize) -> WriteU64<&mut Self> {
        write_u64(self, value as u64)
    }

    fn write_bool(&mut self, value: bool) -> WriteU64<&mut Self> {
        if value {
            write_u64(self, 1)
        } else {
            write_u64(self, 0)
        }
    }

    fn write_enum<V: Into<u64>>(&mut self, value: V) -> WriteU64<&mut Self> {
        write_u64(self, value.into())
    }

    fn write_flag<V: Into<bool>>(&mut self, value: V) -> WriteU64<&mut Self> {
        self.write_bool(value.into())
    }

    fn write_seconds(&mut self, duration: Duration) -> WriteU64<&mut Self> {
        let secs = duration.as_secs();
        write_u64(self, secs)
    }

    fn write_time(
        &mut self,
        time: SystemTime,
    ) -> Either<WriteU64<&mut Self>, Ready<io::Result<()>>> {
        match time.duration_since(SystemTime::UNIX_EPOCH) {
            Ok(duration) => Either::Left(self.write_seconds(duration)),
            Err(_) => Either::Right(future::err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "time is before unix epoch",
            ))),
        }
    }

    fn write_buf<'a>(&mut self, buf: &'a [u8]) -> WriteSlice<'a, &mut Self> {
        write_buf(self, buf)
    }

    fn write_str<'a>(&mut self, s: &'a str) -> WriteSlice<'a, &mut Self> {
        write_str(self, s)
    }

    fn write_string(&mut self, s: String) -> WriteString<&mut Self> {
        write_string(self, s)
    }

    fn write_string_coll<'a, C, I>(&mut self, coll: C) -> WriteStringColl<'a, &mut Self, I>
    where
        C: CollectionSize + IntoIterator<Item = &'a String, IntoIter = I>,
        I: Iterator<Item = &'a String>,
    {
        write_string_coll(self, coll)
    }

    fn write_printed<S, I>(&mut self, state: S, item: &I) -> WriteString<&mut Self>
    where
        S: StatePrint<I>,
    {
        let s = state.print(item);
        write_string(self, s)
    }

    fn write_printed_coll<'async_trait, 'item, C, S, IT, I>(
        &mut self,
        state: S,
        coll: C,
    ) -> WriteOwnedStringColl<&mut Self, MapPrintedState<S, IT>>
    where
        'item: 'async_trait,
        S: StatePrint<I> + 'async_trait,
        C: CollectionSize + IntoIterator<Item = &'item I, IntoIter = IT>,
        IT: Iterator<Item = &'item I> + 'async_trait,
        I: 'item,
    {
        write_owned_string_coll(self, MapPrintedColl { state, coll })
    }
}
