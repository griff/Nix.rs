use tokio::io::AsyncRead;

use super::drain::{DrainAll, DrainExact};
use super::read_int::{ReadBool, ReadEnum, ReadFlag, ReadSeconds, ReadTime, ReadUsize};
use super::read_padding::ReadPadding;
use super::read_parsed::ReadParsed;
use super::read_parsed_coll::ReadParsedColl;
use super::read_string::ReadString;
use super::read_string_coll::ReadStringColl;
use super::state_parse::StateParse;
use super::CollectionRead;

pub trait AsyncSource {
    //fn read_u64(&mut self) -> ReadU64<&mut Self>;
    fn read_usize(&mut self) -> ReadUsize<&mut Self>;
    fn read_bool(&mut self) -> ReadBool<&mut Self>;
    fn read_enum<T>(&mut self) -> ReadEnum<&mut Self, T>
    where
        T: From<u64>;
    fn read_flag<F>(&mut self) -> ReadFlag<&mut Self, F>
    where
        F: From<bool>;
    fn read_seconds(&mut self) -> ReadSeconds<&mut Self>;
    fn read_time(&mut self) -> ReadTime<&mut Self>;
    fn read_padding(&mut self, size: u64) -> ReadPadding<&mut Self>;
    fn read_string(&mut self) -> ReadString<&mut Self>;
    fn read_limited_string(&mut self, limit: usize) -> ReadString<&mut Self>;
    fn read_parsed<S, T>(&mut self, state: S) -> ReadParsed<&mut Self, S, T>
    where
        S: StateParse<T>;
    fn read_string_coll<C>(&mut self) -> ReadStringColl<&mut Self, C>
    where
        C: CollectionRead<String>;
    fn read_parsed_coll<S, T, C>(&mut self, state: S) -> ReadParsedColl<&mut Self, S, T, C>
    where
        C: CollectionRead<T>,
        S: StateParse<T>;
    fn drain_all(&mut self) -> DrainAll<&mut Self>;
    fn drain_exact(&mut self, len: u64) -> DrainExact<&mut Self>;
}

impl<R> AsyncSource for R
where
    R: AsyncRead,
{
    /*
    fn read_u64(&mut self) -> ReadU64<&mut Self> {
        ReadU64::new(self)
    }
     */

    fn read_usize(&mut self) -> ReadUsize<&mut Self> {
        ReadUsize::new(self)
    }

    fn read_bool(&mut self) -> ReadBool<&mut Self> {
        ReadBool::new(self)
    }

    fn read_enum<T>(&mut self) -> ReadEnum<&mut Self, T>
    where
        T: From<u64>,
    {
        ReadEnum::new(self)
    }

    fn read_flag<F>(&mut self) -> ReadFlag<&mut Self, F>
    where
        F: From<bool>,
    {
        ReadFlag::new(self)
    }

    fn read_seconds(&mut self) -> ReadSeconds<&mut Self> {
        ReadSeconds::new(self)
    }

    fn read_time(&mut self) -> ReadTime<&mut Self> {
        ReadTime::new(self)
    }

    fn read_padding(&mut self, size: u64) -> ReadPadding<&mut Self> {
        ReadPadding::new(self, size)
    }

    fn read_string(&mut self) -> ReadString<&mut Self> {
        ReadString::new(self)
    }

    fn read_limited_string(&mut self, limit: usize) -> ReadString<&mut Self> {
        ReadString::with_limit(self, limit)
    }

    fn read_parsed<S, T>(&mut self, state: S) -> ReadParsed<&mut Self, S, T>
    where
        S: StateParse<T>,
    {
        ReadParsed::new(self, state)
    }
    fn read_string_coll<C>(&mut self) -> ReadStringColl<&mut Self, C>
    where
        C: CollectionRead<String>,
    {
        ReadStringColl::new(self)
    }
    fn read_parsed_coll<S, T, C>(&mut self, state: S) -> ReadParsedColl<&mut Self, S, T, C>
    where
        C: CollectionRead<T>,
        S: StateParse<T>,
    {
        ReadParsedColl::new(self, state)
    }

    fn drain_all(&mut self) -> DrainAll<&mut Self> {
        DrainAll::new(self)
    }

    fn drain_exact(&mut self, len: u64) -> DrainExact<&mut Self> {
        DrainExact::new(self, len)
    }
}
