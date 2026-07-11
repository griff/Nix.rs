use std::collections::BTreeMap;

use capnp::Error;
use capnp::traits::Owned;
use capnp_convert::{BuildFrom as _, ReadFrom, ReadInto as _, SetInto};

use crate::capnp::nix_types_capnp;

impl<'d, K, V, KR, VR> SetInto<nix_types_capnp::map::entry::Builder<'_, KR, VR>> for (&'d K, &'d V)
where
    KR: Owned,
    for<'kb> K: SetInto<<KR as Owned>::Builder<'kb>>,
    VR: Owned,
    for<'vb> V: SetInto<<VR as Owned>::Builder<'vb>>,
{
    fn set_into(
        &self,
        builder: &mut nix_types_capnp::map::entry::Builder<'_, KR, VR>,
    ) -> capnp::Result<()> {
        builder
            .reborrow()
            .initn_key(self.0.len())
            .build_from(self.0)?;
        builder
            .reborrow()
            .initn_value(self.0.len())
            .build_from(self.1)?;
        Ok(())
    }
}

impl<'r, K, V, KR, VR> ReadFrom<nix_types_capnp::map::entry::Reader<'r, KR, VR>> for (K, V)
where
    K: ReadFrom<<KR as Owned>::Reader<'r>> + Ord,
    KR: Owned,
    V: ReadFrom<<VR as Owned>::Reader<'r>>,
    VR: Owned,
{
    fn read_from(reader: nix_types_capnp::map::entry::Reader<'r, KR, VR>) -> Result<Self, Error> {
        let key = reader.get_key()?.read_into()?;
        let value = reader.get_value()?.read_into()?;
        Ok((key, value))
    }
}

impl<'b, K, V, KR, VR> SetInto<nix_types_capnp::map::Builder<'b, KR, VR>> for BTreeMap<K, V>
where
    KR: Owned,
    for<'kb> K: SetInto<<KR as Owned>::Builder<'kb>>,
    VR: Owned,
    for<'vb> V: SetInto<<VR as Owned>::Builder<'vb>>,
{
    fn set_into(
        &self,
        builder: &mut nix_types_capnp::map::Builder<'b, KR, VR>,
    ) -> capnp::Result<()> {
        builder
            .reborrow()
            .init_entries(self.len() as u32)
            .build_from(&self)
    }
}

impl<'r, K, V, KR, VR> ReadFrom<nix_types_capnp::map::Reader<'r, KR, VR>> for BTreeMap<K, V>
where
    K: ReadFrom<<KR as Owned>::Reader<'r>> + Ord,
    KR: Owned,
    V: ReadFrom<<VR as Owned>::Reader<'r>>,
    VR: Owned,
{
    fn read_from(reader: nix_types_capnp::map::Reader<'r, KR, VR>) -> Result<Self, Error> {
        reader.get_entries()?.read_into()
    }
}
