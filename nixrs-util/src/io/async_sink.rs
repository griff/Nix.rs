use std::future::Future;
use std::io;
use std::pin::Pin;
use std::time::{Duration, SystemTime};

use futures::future::{self, Either, Ready};
use tokio::io::AsyncWrite;

use crate::StatePrint;

use super::write_int::WriteU64;
use super::write_string::{write_string, WriteStr};
use super::write_string_coll::{write_string_coll, WriteStringColl};
use super::CollectionSize;

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
    fn write_string<'a>(&mut self, s: &'a str) -> WriteStr<'a, &mut Self>;
    fn write_string_coll<'a, C, I>(&mut self, coll: C) -> WriteStringColl<'a, &mut Self, I>
    where
        C: CollectionSize + IntoIterator<Item = &'a String, IntoIter = I>,
        I: Iterator<Item = &'a String>;
    fn write_printed<'this, 'item, 'async_trait, S, I>(
        &'this mut self,
        state: S,
        item: &'item I,
    ) -> Pin<Box<dyn Future<Output = io::Result<()>> + 'async_trait>>
    where
        'this: 'async_trait,
        'item: 'async_trait,
        S: StatePrint<I> + 'async_trait,
        Self: Unpin;
    fn write_printed_coll<'this, 'async_trait, 'item, C, S, IT, I>(
        &'this mut self,
        state: S,
        coll: C,
    ) -> Pin<Box<dyn Future<Output = io::Result<()>> + 'async_trait>>
    where
        'this: 'async_trait,
        'item: 'async_trait,
        S: StatePrint<I> + 'async_trait,
        C: CollectionSize + IntoIterator<Item = &'item I, IntoIter = IT> + 'async_trait,
        IT: Iterator<Item = &'item I> + 'async_trait,
        I: 'item,
        Self: Unpin;
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

    fn write_string<'a>(&mut self, s: &'a str) -> WriteStr<'a, &mut Self> {
        write_string(self, s)
    }

    fn write_string_coll<'a, C, I>(&mut self, coll: C) -> WriteStringColl<'a, &mut Self, I>
    where
        C: CollectionSize + IntoIterator<Item = &'a String, IntoIter = I>,
        I: Iterator<Item = &'a String>,
    {
        write_string_coll(self, coll)
    }

    fn write_printed<'this, 'item, 'async_trait, S, I>(
        &'this mut self,
        state: S,
        item: &'item I,
    ) -> Pin<Box<dyn Future<Output = io::Result<()>> + 'async_trait>>
    where
        'this: 'async_trait,
        'item: 'async_trait,
        S: StatePrint<I> + 'async_trait,
        Self: Unpin,
    {
        async fn run<W, S, I>(me: &mut W, state: S, item: &I) -> io::Result<()>
        where
            S: StatePrint<I>,
            W: AsyncWrite + Unpin,
        {
            let s = state.print(item);
            me.write_string(&s).await
        }
        Box::pin(run(self, state, item))
    }
    fn write_printed_coll<'this, 'async_trait, 'item, C, S, IT, I>(
        &'this mut self,
        state: S,
        coll: C,
    ) -> Pin<Box<dyn Future<Output = io::Result<()>> + 'async_trait>>
    where
        'this: 'async_trait,
        'item: 'async_trait,
        S: StatePrint<I> + 'async_trait,
        C: CollectionSize + IntoIterator<Item = &'item I, IntoIter = IT> + 'async_trait,
        IT: Iterator<Item = &'item I> + 'async_trait,
        I: 'item,
        Self: Unpin,
    {
        async fn run<'a, W, C, S, IT, I>(me: &mut W, state: S, coll: C) -> io::Result<()>
        where
            S: StatePrint<I>,
            W: AsyncWrite + Unpin,
            C: CollectionSize + IntoIterator<Item = &'a I, IntoIter = IT>,
            I: 'a,
            IT: Iterator<Item = &'a I>,
        {
            let len = coll.len();
            me.write_usize(len).await?;
            for item in coll.into_iter() {
                let s = state.print(item);
                me.write_string(&s).await?;
            }
            Ok(())
        }
        Box::pin(run(self, state, coll))
    }
}
