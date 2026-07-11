use std::{fmt, str::FromStr};

use bytes::Bytes;
use capnp::Error;

mod collections;
mod text;

#[expect(clippy::len_without_is_empty)]
pub trait SetInto<B> {
    fn set_into(&self, builder: &mut B) -> capnp::Result<()>;

    fn len(&self) -> u32 {
        0
    }
}

pub trait BuildFrom<V> {
    fn build_from(&mut self, input: &V) -> Result<(), Error>;
}

impl<B, V> BuildFrom<V> for B
where
    V: SetInto<B>,
{
    fn build_from(&mut self, input: &V) -> Result<(), Error> {
        input.set_into(self)
    }
}

pub trait ReadFrom<R>: Sized {
    fn read_from(reader: R) -> Result<Self, Error>;
}

pub trait ReadInto<V> {
    fn read_into(self) -> Result<V, Error>;
}
impl<R, V> ReadInto<V> for R
where
    V: ReadFrom<R>,
{
    fn read_into(self) -> Result<V, Error> {
        V::read_from(self)
    }
}

impl<T> ReadFrom<capnp::text::Reader<'_>> for T
where
    T: FromStr,
    <T as FromStr>::Err: fmt::Display,
{
    fn read_from(reader: capnp::text::Reader<'_>) -> Result<Self, Error> {
        reader
            .to_str()?
            .parse::<T>()
            .map_err(|err| Error::failed(err.to_string()))
    }
}

impl ReadFrom<capnp::data::Reader<'_>> for Bytes {
    fn read_from(reader: capnp::data::Reader<'_>) -> Result<Self, Error> {
        Ok(Bytes::copy_from_slice(reader))
    }
}
