use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::str::FromStr;

use capnp::Error;
use capnp::traits::OwnedStruct;

use crate::{ReadFrom, ReadInto as _, SetInto};

impl<'b, T> SetInto<capnp::text_list::Builder<'b>> for BTreeSet<T>
where
    T: AsRef<str>,
{
    fn set_into(&self, builder: &mut capnp::text_list::Builder<'b>) -> capnp::Result<()> {
        for (index, name) in self.iter().enumerate() {
            builder.set(index as u32, name.as_ref());
        }
        Ok(())
    }

    fn len(&self) -> u32 {
        self.len() as u32
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

impl<'b, T> SetInto<capnp::text_list::Builder<'b>> for Vec<T>
where
    T: AsRef<str>,
{
    fn set_into(&self, builder: &mut capnp::text_list::Builder<'b>) -> capnp::Result<()> {
        for (index, name) in self.iter().enumerate() {
            builder.set(index as u32, name.as_ref());
        }
        Ok(())
    }

    fn len(&self) -> u32 {
        self.len() as u32
    }
}

impl<'r, T> ReadFrom<capnp::text_list::Reader<'r>> for Vec<T>
where
    T: FromStr,
    <T as FromStr>::Err: fmt::Display,
{
    fn read_from(reader: capnp::text_list::Reader<'r>) -> Result<Self, Error> {
        let mut ret = Vec::with_capacity(reader.len() as usize);
        for item_r in reader.iter() {
            let item = item_r?
                .to_str()?
                .parse::<T>()
                .map_err(|err| Error::failed(err.to_string()))?;
            ret.push(item);
        }
        Ok(ret)
    }
}

impl<'b, T> SetInto<capnp::data_list::Builder<'b>> for Vec<T>
where
    T: AsRef<[u8]>,
{
    fn set_into(&self, builder: &mut capnp::data_list::Builder<'b>) -> capnp::Result<()> {
        for (index, item) in self.iter().enumerate() {
            builder.set(index as u32, item.as_ref());
        }
        Ok(())
    }

    fn len(&self) -> u32 {
        self.len() as u32
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

impl<'b, T, B> SetInto<capnp::struct_list::Builder<'b, B>> for BTreeSet<T>
where
    B: OwnedStruct,
    for<'b2> T: SetInto<<B as OwnedStruct>::Builder<'b2>>,
{
    fn set_into(&self, builder: &mut capnp::struct_list::Builder<'b, B>) -> capnp::Result<()> {
        for (index, item) in self.iter().enumerate() {
            let mut b = builder.reborrow().get(index as u32);
            item.set_into(&mut b)?;
        }
        Ok(())
    }

    fn len(&self) -> u32 {
        self.len() as u32
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

impl<'b, T, B> SetInto<capnp::struct_list::Builder<'b, B>> for Vec<T>
where
    B: OwnedStruct,
    for<'b2> T: SetInto<<B as OwnedStruct>::Builder<'b2>>,
{
    fn set_into(&self, builder: &mut capnp::struct_list::Builder<'b, B>) -> capnp::Result<()> {
        for (index, item) in self.iter().enumerate() {
            let mut b = builder.reborrow().get(index as u32);
            item.set_into(&mut b)?;
        }
        Ok(())
    }

    fn len(&self) -> u32 {
        self.len() as u32
    }
}

impl<'b, I, IB> SetInto<capnp::struct_list::Builder<'b, IB>> for &[I]
where
    IB: OwnedStruct,
    for<'b2> I: SetInto<<IB as OwnedStruct>::Builder<'b2>>,
{
    fn set_into(&self, builder: &mut capnp::struct_list::Builder<'b, IB>) -> capnp::Result<()> {
        for (index, item) in self.iter().enumerate() {
            let mut b = builder.reborrow().get(index as u32);
            item.set_into(&mut b)?;
        }
        Ok(())
    }

    fn len(&self) -> u32 {
        <[I]>::len(self) as u32
    }
}

impl<'d, 'b, KV, K, V> SetInto<capnp::struct_list::Builder<'b, KV>> for &'d BTreeMap<K, V>
where
    KV: OwnedStruct,
    for<'ib> (&'d K, &'d V): SetInto<<KV as OwnedStruct>::Builder<'ib>>,
{
    fn set_into(&self, builder: &mut capnp::struct_list::Builder<'b, KV>) -> capnp::Result<()> {
        for (index, item) in self.iter().enumerate() {
            let mut b = builder.reborrow().get(index as u32);
            item.set_into(&mut b)?;
        }
        Ok(())
    }

    fn len(&self) -> u32 {
        (*self).len() as u32
    }
}

impl<'r, K, V, O> ReadFrom<capnp::struct_list::Reader<'r, O>> for BTreeMap<K, V>
where
    (K, V): ReadFrom<<O as OwnedStruct>::Reader<'r>>,
    K: Ord,
    O: OwnedStruct,
{
    fn read_from(reader: capnp::struct_list::Reader<'r, O>) -> Result<Self, Error> {
        let mut ret = BTreeMap::new();
        for item_r in reader.iter() {
            let (key, value) = item_r.read_into()?;
            ret.insert(key, value);
        }
        Ok(ret)
    }
}
