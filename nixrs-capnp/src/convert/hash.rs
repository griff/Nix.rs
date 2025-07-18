use capnp::{
    traits::{FromPointerBuilder as _, SetterInput},
    Error,
};
use nixrs::hash;

use crate::capnp::nix_daemon_capnp;
use crate::convert::ReadFrom;

impl TryFrom<nix_daemon_capnp::HashAlgo> for hash::Algorithm {
    type Error = Error;

    fn try_from(value: nix_daemon_capnp::HashAlgo) -> Result<Self, Self::Error> {
        match value {
            nix_daemon_capnp::HashAlgo::Md5 => Ok(hash::Algorithm::MD5),
            nix_daemon_capnp::HashAlgo::Sha1 => Ok(hash::Algorithm::SHA1),
            nix_daemon_capnp::HashAlgo::Sha256 => Ok(hash::Algorithm::SHA256),
            nix_daemon_capnp::HashAlgo::Sha512 => Ok(hash::Algorithm::SHA512),
        }
    }
}
impl From<hash::Algorithm> for nix_daemon_capnp::HashAlgo {
    fn from(value: hash::Algorithm) -> Self {
        match value {
            hash::Algorithm::MD5 => nix_daemon_capnp::HashAlgo::Md5,
            hash::Algorithm::SHA1 => nix_daemon_capnp::HashAlgo::Sha1,
            hash::Algorithm::SHA256 => nix_daemon_capnp::HashAlgo::Sha256,
            hash::Algorithm::SHA512 => nix_daemon_capnp::HashAlgo::Sha512,
        }
    }
}
/*
impl<'b> BuildFrom<hash::Hash> for nix_daemon_capnp::hash::Builder<'b> {
    fn build_from(&mut self, input: &hash::Hash) -> capnp::Result<()> {
        self.set_algo(input.algorithm().into());
        self.set_digest(input.data());
        Ok(())
    }
}
*/
impl SetterInput<nix_daemon_capnp::hash::Owned> for &'_ hash::Hash {
    fn set_pointer_builder(
        builder: capnp::private::layout::PointerBuilder<'_>,
        input: Self,
        _canonicalize: bool,
    ) -> capnp::Result<()> {
        let mut builder = nix_daemon_capnp::hash::Builder::init_pointer(builder, 0);
        builder.set_algo(input.algorithm().into());
        builder.set_digest(input.data());
        Ok(())
    }
}

impl<'r> ReadFrom<nix_daemon_capnp::hash::Reader<'r>> for hash::Hash {
    fn read_from(value: nix_daemon_capnp::hash::Reader<'r>) -> Result<Self, Error> {
        let algorithm = value.get_algo()?.try_into()?;
        let digest = value.get_digest()?;
        hash::Hash::from_slice(algorithm, digest).map_err(|err| Error::failed(err.to_string()))
    }
}

impl<'r> ReadFrom<capnp::data::Reader<'r>> for hash::NarHash {
    fn read_from(value: capnp::data::Reader<'r>) -> Result<Self, Error> {
        hash::NarHash::from_slice(value).map_err(|err| Error::failed(err.to_string()))
    }
}
