use bytes::BytesMut;
use tokio::io::AsyncRead;

use super::CollectionRead;
use super::StateParse;

mod drain;
mod read_bytes;
mod read_exact;
mod read_int;
mod read_padding;
mod read_parsed;
mod read_parsed_coll;
mod read_string;
mod read_string_coll;

pub use self::drain::{DrainAll, DrainExact};
pub use self::read_bytes::ReadBytes;
pub use self::read_int::{ReadBool, ReadEnum, ReadFlag, ReadSeconds, ReadTime, ReadUsize};
pub use self::read_padding::ReadPadding;
pub use self::read_parsed::ReadParsed;
pub use self::read_parsed_coll::ReadParsedColl;
pub use self::read_string::ReadString;
pub use self::read_string_coll::ReadStringColl;

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
    fn read_bytes(&mut self) -> ReadBytes<&mut Self>;
    fn read_bytes_buf(&mut self, buf: BytesMut) -> ReadBytes<&mut Self>;
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

    fn read_bytes(&mut self) -> ReadBytes<&mut Self> {
        ReadBytes::new(self, BytesMut::new())
    }

    fn read_bytes_buf(&mut self, buf: BytesMut) -> ReadBytes<&mut Self> {
        ReadBytes::new(self, buf)
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
