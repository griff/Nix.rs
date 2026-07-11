use capnp::Error;
use capnp_convert::{ReadFrom, SetInto};
use nixrs::signature::Signature;

use crate::capnp::nix_types_capnp;

impl SetInto<nix_types_capnp::signature::Builder<'_>> for Signature {
    fn set_into(&self, builder: &mut nix_types_capnp::signature::Builder<'_>) -> capnp::Result<()> {
        builder.set_key(self.name());
        builder.set_hash(self.signature_bytes());
        Ok(())
    }
}

impl ReadFrom<nix_types_capnp::signature::Reader<'_>> for Signature {
    fn read_from(value: nix_types_capnp::signature::Reader<'_>) -> Result<Self, Error> {
        let c_key = value.get_key()?.to_str()?;
        let c_hash = value.get_hash()?;
        let signature =
            Signature::from_parts(c_key, c_hash).map_err(|err| Error::failed(err.to_string()))?;
        Ok(signature)
    }
}
