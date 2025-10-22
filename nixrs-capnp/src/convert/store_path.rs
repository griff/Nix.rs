use capnp::{
    Error,
    traits::{FromPointerBuilder as _, SetterInput},
};
use nixrs::{
    hash,
    store_path::{
        ContentAddress, ContentAddressMethodAlgorithm, StorePath, StorePathError, StorePathHash,
        StorePathName,
    },
};

use crate::{
    capnp::{nix_daemon_capnp, nix_types_capnp},
    convert::{BuildFrom, ReadFrom, ReadInto as _},
};

impl<'b> BuildFrom<StorePath> for nix_types_capnp::store_path::Builder<'b> {
    fn build_from(&mut self, input: &StorePath) -> Result<(), Error> {
        self.set_hash(input.hash().as_ref());
        self.set_name(input.name().as_ref());
        Ok(())
    }
}

impl SetterInput<nix_types_capnp::store_path::Owned> for &'_ StorePath {
    fn set_pointer_builder(
        builder: capnp::private::layout::PointerBuilder<'_>,
        input: Self,
        _canonicalize: bool,
    ) -> capnp::Result<()> {
        let mut builder = nix_types_capnp::store_path::Builder::init_pointer(builder, 0);
        builder.set_hash(input.hash().as_ref());
        builder.set_name(input.name().as_ref());
        Ok(())
    }
}

impl<'r> ReadFrom<nix_types_capnp::store_path::Reader<'r>> for StorePath {
    fn read_from(value: nix_types_capnp::store_path::Reader<'r>) -> Result<Self, Error> {
        let c_hash = value.get_hash()?;
        let c_name = value.get_name()?.to_str()?;
        let name = c_name
            .parse::<StorePathName>()
            .map_err(|err| Error::failed(err.to_string()))?;
        let hash: StorePathHash = c_hash
            .try_into()
            .map_err(|err: StorePathError| Error::failed(err.to_string()))?;
        Ok((hash, name).into())
    }
}

impl<'r> ReadFrom<capnp::data::Reader<'r>> for StorePathHash {
    fn read_from(value: capnp::data::Reader<'r>) -> Result<Self, Error> {
        let hash: StorePathHash = value
            .try_into()
            .map_err(|err: StorePathError| Error::failed(err.to_string()))?;
        Ok(hash)
    }
}

impl<'b> BuildFrom<ContentAddress> for nix_types_capnp::content_address::Builder<'b> {
    fn build_from(&mut self, input: &ContentAddress) -> Result<(), Error> {
        match input {
            ContentAddress::Text(sha256) => {
                self.set_text(sha256.as_ref());
            }
            ContentAddress::Flat(hash) => {
                self.set_flat(hash)?;
            }
            ContentAddress::Recursive(hash) => {
                self.set_recursive(hash)?;
            }
        }
        Ok(())
    }
}

impl SetterInput<nix_types_capnp::content_address::Owned> for &'_ ContentAddress {
    fn set_pointer_builder(
        builder: capnp::private::layout::PointerBuilder<'_>,
        input: Self,
        _canonicalize: bool,
    ) -> capnp::Result<()> {
        let mut builder = nix_types_capnp::content_address::Builder::init_pointer(builder, 0);
        match input {
            ContentAddress::Text(sha256) => {
                builder.set_text(sha256.as_ref());
            }
            ContentAddress::Flat(hash) => {
                builder.set_flat(hash)?;
            }
            ContentAddress::Recursive(hash) => {
                builder.set_recursive(hash)?;
            }
        }
        Ok(())
    }
}

impl<'r> ReadFrom<nix_types_capnp::content_address::Reader<'r>> for ContentAddress {
    fn read_from(value: nix_types_capnp::content_address::Reader<'r>) -> Result<Self, Error> {
        match value.which()? {
            nix_types_capnp::content_address::Which::Text(hash) => {
                let digest = hash?;
                let hash = hash::Sha256::from_slice(digest)
                    .map_err(|err| Error::failed(err.to_string()))?;
                Ok(ContentAddress::Text(hash))
            }
            nix_types_capnp::content_address::Which::Flat(hash) => {
                let hash = hash?.read_into()?;
                Ok(ContentAddress::Flat(hash))
            }
            nix_types_capnp::content_address::Which::Recursive(hash) => {
                let hash = hash?.read_into()?;
                Ok(ContentAddress::Recursive(hash))
            }
        }
    }
}

impl<'r> ReadFrom<nix_daemon_capnp::content_address_method_algorithm::Reader<'r>>
    for ContentAddressMethodAlgorithm
{
    fn read_from(
        reader: nix_daemon_capnp::content_address_method_algorithm::Reader<'r>,
    ) -> Result<Self, Error> {
        match reader.which()? {
            nix_daemon_capnp::content_address_method_algorithm::Which::Text(_) => {
                Ok(ContentAddressMethodAlgorithm::Text)
            }
            nix_daemon_capnp::content_address_method_algorithm::Which::Flat(hash_algo) => {
                Ok(ContentAddressMethodAlgorithm::Flat(hash_algo?.try_into()?))
            }
            nix_daemon_capnp::content_address_method_algorithm::Which::Recursive(hash_algo) => Ok(
                ContentAddressMethodAlgorithm::Recursive(hash_algo?.try_into()?),
            ),
        }
    }
}
