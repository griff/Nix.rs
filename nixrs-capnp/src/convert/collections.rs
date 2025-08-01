use std::collections::BTreeSet;
use std::fmt;
use std::{collections::BTreeMap, str::FromStr};

use capnp::traits::{Owned, OwnedStruct, SetterInput};
use capnp::Error;

use crate::capnp::nix_types_capnp;
use crate::convert::{BuildFrom, ReadFrom, ReadInto};

impl<'b, T> BuildFrom<BTreeSet<T>> for capnp::text_list::Builder<'b>
where
    T: AsRef<str>,
{
    fn build_from(&mut self, input: &BTreeSet<T>) -> capnp::Result<()> {
        for (index, name) in input.iter().enumerate() {
            self.set(index as u32, name.as_ref());
        }
        Ok(())
    }
}

impl<'r, T> ReadFrom<capnp::text_list::Reader<'r>> for BTreeSet<T>
where
    T: FromStr + Ord,
    <T as FromStr>::Err: fmt::Display,
{
    fn read_from(reader: capnp::text_list::Reader<'r>) -> Result<Self, Error> {
        let mut ret = BTreeSet::new();
        for item_r in reader.iter() {
            let item = item_r?
                .to_str()?
                .parse::<T>()
                .map_err(|err| Error::failed(err.to_string()))?;
            ret.insert(item);
        }
        Ok(ret)
    }
}

impl<'b, T> BuildFrom<Vec<T>> for capnp::data_list::Builder<'b>
where
    T: AsRef<[u8]>,
{
    fn build_from(&mut self, input: &Vec<T>) -> Result<(), Error> {
        for (index, item) in input.iter().enumerate() {
            self.set(index as u32, item.as_ref());
        }
        Ok(())
    }
}

impl<'r, T> ReadFrom<capnp::data_list::Reader<'r>> for Vec<T>
where
    for<'tr> T: ReadFrom<capnp::data::Reader<'tr>>,
{
    fn read_from(reader: capnp::data_list::Reader<'r>) -> Result<Self, Error> {
        let mut ret = Vec::with_capacity(reader.len() as usize);
        for item_r in reader.iter() {
            let item = item_r?.read_into()?;
            ret.push(item);
        }
        Ok(ret)
    }
}

impl<'r, T, R> ReadFrom<capnp::struct_list::Reader<'r, R>> for BTreeSet<T>
where
    T: ReadFrom<<R as OwnedStruct>::Reader<'r>> + Ord,
    R: OwnedStruct,
{
    fn read_from(reader: capnp::struct_list::Reader<'r, R>) -> Result<Self, Error> {
        let mut ret = BTreeSet::new();
        for item_r in reader.iter() {
            let item = T::read_from(item_r)?;
            ret.insert(item);
        }
        Ok(ret)
    }
}

impl<'b, T, B> BuildFrom<BTreeSet<T>> for capnp::struct_list::Builder<'b, B>
where
    B: OwnedStruct,
    for<'b2> <B as OwnedStruct>::Builder<'b2>: BuildFrom<T>,
{
    fn build_from(&mut self, input: &BTreeSet<T>) -> capnp::Result<()> {
        for (index, item) in input.iter().enumerate() {
            self.reborrow().get(index as u32).build_from(item)?;
        }
        Ok(())
    }
}

impl<'r, T, R> ReadFrom<capnp::struct_list::Reader<'r, R>> for Vec<T>
where
    T: ReadFrom<<R as OwnedStruct>::Reader<'r>>,
    R: OwnedStruct,
{
    fn read_from(reader: capnp::struct_list::Reader<'r, R>) -> Result<Self, Error> {
        let mut ret = Vec::with_capacity(reader.len() as usize);
        for item_r in reader.iter() {
            let item = T::read_from(item_r)?;
            ret.push(item);
        }
        Ok(ret)
    }
}

impl<'b, T, B> BuildFrom<Vec<T>> for capnp::struct_list::Builder<'b, B>
where
    B: OwnedStruct,
    for<'b2> <B as OwnedStruct>::Builder<'b2>: BuildFrom<T>,
{
    fn build_from(&mut self, input: &Vec<T>) -> capnp::Result<()> {
        for (index, item) in input.iter().enumerate() {
            self.reborrow().get(index as u32).build_from(item)?;
        }
        Ok(())
    }
}

impl<'d, 'b, I, IB> BuildFrom<&'d [I]> for capnp::struct_list::Builder<'b, IB>
where
    IB: OwnedStruct,
    for<'ib> <IB as OwnedStruct>::Builder<'ib>: BuildFrom<I>,
{
    fn build_from(&mut self, input: &&'d [I]) -> capnp::Result<()> {
        for (index, path) in input.iter().enumerate() {
            let mut c_path = self.reborrow().get(index as u32);
            c_path.build_from(path)?;
        }
        Ok(())
    }
}

impl<'b, K, V, KR, VR> BuildFrom<BTreeMap<K, V>>
    for capnp::struct_list::Builder<'b, nix_types_capnp::map::entry::Owned<KR, VR>>
where
    KR: Owned,
    for<'kr> &'kr K: SetterInput<KR>,
    for<'vb> <VR as Owned>::Builder<'vb>: BuildFrom<V>,
    VR: Owned,
{
    fn build_from(&mut self, input: &BTreeMap<K, V>) -> Result<(), Error> {
        for (index, (key, value)) in input.iter().enumerate() {
            let mut b = self.reborrow().get(index as u32);
            b.reborrow().set_key(key)?;
            b.init_value().build_from(value)?;
        }
        Ok(())
    }
}

impl<'r, K, V, KR, VR>
    ReadFrom<capnp::struct_list::Reader<'r, nix_types_capnp::map::entry::Owned<KR, VR>>>
    for BTreeMap<K, V>
where
    K: ReadFrom<<KR as Owned>::Reader<'r>> + Ord,
    KR: Owned,
    V: ReadFrom<<VR as Owned>::Reader<'r>>,
    VR: Owned,
{
    fn read_from(
        reader: capnp::struct_list::Reader<'r, nix_types_capnp::map::entry::Owned<KR, VR>>,
    ) -> Result<Self, Error> {
        let mut ret = BTreeMap::new();
        for item_r in reader.iter() {
            let key = item_r.get_key()?.read_into()?;
            let value = item_r.get_value()?.read_into()?;
            ret.insert(key, value);
        }
        Ok(ret)
    }
}

impl<'b, K, V, KR, VR> BuildFrom<BTreeMap<K, V>> for nix_types_capnp::map::Builder<'b, KR, VR>
where
    for<'kb> &'kb K: SetterInput<KR>,
    KR: Owned,
    for<'vb> <VR as Owned>::Builder<'vb>: BuildFrom<V>,
    VR: Owned,
{
    fn build_from(&mut self, input: &BTreeMap<K, V>) -> Result<(), Error> {
        self.reborrow()
            .init_entries(input.len() as u32)
            .build_from(input)
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
