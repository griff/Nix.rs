use capnp::Error;
use capnp_convert::{ReadFrom, SetInto};
use nixrs::hash;

use crate::capnp::nix_types_capnp;

impl From<nix_types_capnp::HashAlgo> for hash::Algorithm {
    fn from(value: nix_types_capnp::HashAlgo) -> Self {
        match value {
            nix_types_capnp::HashAlgo::Md5 => hash::Algorithm::MD5,
            nix_types_capnp::HashAlgo::Sha1 => hash::Algorithm::SHA1,
            nix_types_capnp::HashAlgo::Sha256 => hash::Algorithm::SHA256,
            nix_types_capnp::HashAlgo::Sha512 => hash::Algorithm::SHA512,
        }
    }
}
impl From<hash::Algorithm> for nix_types_capnp::HashAlgo {
    fn from(value: hash::Algorithm) -> Self {
        match value {
            hash::Algorithm::MD5 => nix_types_capnp::HashAlgo::Md5,
            hash::Algorithm::SHA1 => nix_types_capnp::HashAlgo::Sha1,
            hash::Algorithm::SHA256 => nix_types_capnp::HashAlgo::Sha256,
            hash::Algorithm::SHA512 => nix_types_capnp::HashAlgo::Sha512,
        }
    }
}

impl<'b> SetInto<nix_types_capnp::hash::Builder<'b>> for hash::Hash {
    fn set_into(&self, builder: &mut nix_types_capnp::hash::Builder<'b>) -> capnp::Result<()> {
        builder.set_algo(self.algorithm().into());
        builder.set_digest(self.digest_bytes());
        Ok(())
    }
}

impl<'r> ReadFrom<nix_types_capnp::hash::Reader<'r>> for hash::Hash {
    fn read_from(value: nix_types_capnp::hash::Reader<'r>) -> Result<Self, Error> {
        let algorithm = value.get_algo()?.into();
        let digest = value.get_digest()?;
        hash::Hash::from_slice(algorithm, digest).map_err(|err| Error::failed(err.to_string()))
    }
}

/*
impl<'r> ReadFrom<capnp::data::Reader<'r>> for hash::NarHash {
    fn read_from(value: capnp::data::Reader<'r>) -> Result<Self, Error> {
        hash::NarHash::from_slice(value).map_err(|err| Error::failed(err.to_string()))
    }
}
*/
