use capnp::Error;
use capnp_convert::{BuildFrom as _, ReadFrom, ReadInto as _, SetInto};
use nixrs::hash;
use nixrs::store_path::{
    ContentAddress, ContentAddressMethodAlgorithm, StorePath, StorePathHash, StorePathName,
};

use crate::capnp::{nix_daemon_capnp, nix_types_capnp, nixrs_capnp};

impl<'b> SetInto<nix_types_capnp::store_path::Builder<'b>> for StorePath {
    fn set_into(
        &self,
        builder: &mut nix_types_capnp::store_path::Builder<'b>,
    ) -> capnp::Result<()> {
        builder.set_hash(self.hash().as_ref());
        builder.set_name(self.name());
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
        let hash = StorePathHash::try_from(c_hash).map_err(|err| Error::failed(err.to_string()))?;
        Ok((hash, name).into())
    }
}

impl<'r> ReadFrom<nixrs_capnp::remote_store_path::Reader<'r>> for StorePath {
    fn read_from(value: nixrs_capnp::remote_store_path::Reader<'r>) -> Result<Self, Error> {
        if value.has_store_path() {
            value.get_store_path()?.read_into()
        } else {
            Err(Error::failed("No store path was sent".to_string()))
        }
    }
}

impl<'r> ReadFrom<nixrs_capnp::remote_store_path::Reader<'r>> for Option<StorePath> {
    fn read_from(value: nixrs_capnp::remote_store_path::Reader<'r>) -> Result<Self, Error> {
        if value.has_store_path() {
            Ok(Some(value.get_store_path()?.read_into()?))
        } else {
            Ok(None)
        }
    }
}

/*
impl<'r> ReadFrom<capnp::data::Reader<'r>> for StorePathHash {
    fn read_from(value: capnp::data::Reader<'r>) -> Result<Self, Error> {
        let hash: StorePathHash = value
            .try_into()
            .map_err(|err: StorePathError| Error::failed(err.to_string()))?;
        Ok(hash)
    }
}
*/

impl<'b> SetInto<nix_types_capnp::content_address::Builder<'b>> for ContentAddress {
    fn set_into(
        &self,
        builder: &mut nix_types_capnp::content_address::Builder<'b>,
    ) -> capnp::Result<()> {
        match self {
            ContentAddress::Text(sha256) => {
                builder.set_text(sha256.as_ref());
            }
            ContentAddress::Flat(hash) => {
                builder.reborrow().init_flat().build_from(hash)?;
            }
            ContentAddress::Recursive(hash) => {
                builder.reborrow().init_recursive().build_from(hash)?;
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

impl<'b> SetInto<nix_daemon_capnp::content_address_method_algorithm::Builder<'b>>
    for ContentAddressMethodAlgorithm
{
    fn set_into(
        &self,
        builder: &mut nix_daemon_capnp::content_address_method_algorithm::Builder<'b>,
    ) -> capnp::Result<()> {
        match self {
            ContentAddressMethodAlgorithm::Text => {
                builder.set_text(());
            }
            ContentAddressMethodAlgorithm::Flat(algo) => {
                builder.reborrow().set_flat((*algo).into());
            }
            ContentAddressMethodAlgorithm::Recursive(algo) => {
                builder.reborrow().set_recursive((*algo).into());
            }
        }
        Ok(())
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
            nix_daemon_capnp::content_address_method_algorithm::Which::Flat(algo) => {
                Ok(ContentAddressMethodAlgorithm::Flat(algo?.into()))
            }
            nix_daemon_capnp::content_address_method_algorithm::Which::Recursive(algo) => {
                Ok(ContentAddressMethodAlgorithm::Recursive(algo?.into()))
            }
        }
    }
}
