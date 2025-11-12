use std::{fmt, str::FromStr};

use bytes::Bytes;
use capnp::Error;
use nixrs::signature::Signature;

use crate::capnp::nix_types_capnp;

mod collections;
mod daemon;
mod derivation;
mod derived_path;
mod hash;
mod log;
mod net;
mod realisation;
mod store_path;

pub trait BuildFrom<V> {
    fn build_from(&mut self, input: &V) -> Result<(), Error>;
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

impl<'b, T> BuildFrom<T> for capnp::text::Builder<'b>
where
    T: fmt::Display,
{
    fn build_from(&mut self, input: &T) -> Result<(), Error> {
        let s = input.to_string();
        self.push_str(&s);
        Ok(())
    }
}

impl<'r, T> ReadFrom<capnp::text::Reader<'r>> for T
where
    T: FromStr,
    <T as FromStr>::Err: fmt::Display,
{
    fn read_from(reader: capnp::text::Reader<'r>) -> Result<Self, Error> {
        reader
            .to_str()?
            .parse::<T>()
            .map_err(|err| Error::failed(err.to_string()))
    }
}

impl<'r> ReadFrom<capnp::data::Reader<'r>> for Bytes {
    fn read_from(reader: capnp::data::Reader<'r>) -> Result<Self, Error> {
        Ok(Bytes::copy_from_slice(reader))
    }
}

impl<'b> BuildFrom<Signature> for nix_types_capnp::signature::Builder<'b> {
    fn build_from(&mut self, input: &Signature) -> Result<(), Error> {
        self.set_key(input.name());
        self.set_hash(input.signature_bytes());
        Ok(())
    }
}

impl<'r> ReadFrom<nix_types_capnp::signature::Reader<'r>> for Signature {
    fn read_from(value: nix_types_capnp::signature::Reader<'r>) -> Result<Self, Error> {
        let c_key = value.get_key()?.to_str()?;
        let c_hash = value.get_hash()?;
        let signature =
            Signature::from_parts(c_key, c_hash).map_err(|err| Error::failed(err.to_string()))?;
        Ok(signature)
    }
}
