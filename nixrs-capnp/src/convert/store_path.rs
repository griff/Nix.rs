use capnp::Error;
use capnp_convert::{BuildFrom as _, ReadFrom, ReadInto as _, SetInto};
use nixrs::hash;
use nixrs::store_path::{
    ContentAddress, ContentAddressMethodAlgorithm, FixedOutput, FixedOutputMethod,
    FixedOutputMethodAlgorithm, StorePath, StorePathHash, StorePathName,
};

use crate::capnp::{nix_daemon_capnp, nix_types_capnp, nixrs_capnp};

impl From<nix_types_capnp::FixedOutputMethod> for FixedOutputMethod {
    fn from(value: nix_types_capnp::FixedOutputMethod) -> Self {
        match value {
            nix_types_capnp::FixedOutputMethod::Flat => Self::Flat,
            nix_types_capnp::FixedOutputMethod::Recursive => Self::Recursive,
        }
    }
}

impl From<FixedOutputMethod> for nix_types_capnp::FixedOutputMethod {
    fn from(value: FixedOutputMethod) -> Self {
        match value {
            FixedOutputMethod::Flat => nix_types_capnp::FixedOutputMethod::Flat,
            FixedOutputMethod::Recursive => nix_types_capnp::FixedOutputMethod::Recursive,
        }
    }
}

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

impl<'b> SetInto<nix_types_capnp::fixed_output::Builder<'b>> for FixedOutput {
    fn set_into(
        &self,
        builder: &mut nix_types_capnp::fixed_output::Builder<'b>,
    ) -> capnp::Result<()> {
        builder.set_method(self.method.into());
        builder.reborrow().init_hash().build_from(&self.hash)?;
        Ok(())
    }
}

impl<'r> ReadFrom<nix_types_capnp::fixed_output::Reader<'r>> for FixedOutput {
    fn read_from(value: nix_types_capnp::fixed_output::Reader<'r>) -> Result<Self, Error> {
        let method = value.get_method()?.into();
        let hash = value.get_hash()?.read_into()?;
        Ok(FixedOutput { method, hash })
    }
}

impl<'b> SetInto<nix_types_capnp::content_address::Builder<'b>> for ContentAddress {
    fn set_into(
        &self,
        builder: &mut nix_types_capnp::content_address::Builder<'b>,
    ) -> capnp::Result<()> {
        match self {
            ContentAddress::Text(sha256) => {
                builder.set_text(sha256.as_ref());
            }
            ContentAddress::Fixed(fo) => {
                builder.reborrow().init_fixed().build_from(fo)?;
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
            nix_types_capnp::content_address::Which::Fixed(fo) => {
                let fo = fo?.read_into()?;
                Ok(ContentAddress::Fixed(fo))
            }
        }
    }
}

impl<'b> SetInto<nix_types_capnp::fixed_output_method_algorithm::Builder<'b>>
    for FixedOutputMethodAlgorithm
{
    fn set_into(
        &self,
        builder: &mut nix_types_capnp::fixed_output_method_algorithm::Builder<'b>,
    ) -> capnp::Result<()> {
        builder.set_method(self.method.into());
        builder.set_algo(self.algorithm.into());
        Ok(())
    }
}

impl<'r> ReadFrom<nix_types_capnp::fixed_output_method_algorithm::Reader<'r>>
    for FixedOutputMethodAlgorithm
{
    fn read_from(
        reader: nix_types_capnp::fixed_output_method_algorithm::Reader<'r>,
    ) -> Result<Self, Error> {
        let method = reader.get_method()?.into();
        let algorithm = reader.get_algo()?.into();
        Ok(FixedOutputMethodAlgorithm { method, algorithm })
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
            ContentAddressMethodAlgorithm::Fixed(fo) => {
                builder.reborrow().init_fixed().build_from(fo)?;
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
            nix_daemon_capnp::content_address_method_algorithm::Which::Fixed(fo) => {
                Ok(ContentAddressMethodAlgorithm::Fixed(fo?.read_into()?))
            }
        }
    }
}
