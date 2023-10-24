use std::collections::{BTreeSet, HashSet};

pub trait CollectionSize {
    fn len(&self) -> usize;
}

impl<'a, C> CollectionSize for &'a [C]
{
    fn len(&self) -> usize {
        (*self).len()
    }
}


impl<'a, C> CollectionSize for &'a C
where
    C: CollectionSize,
{
    fn len(&self) -> usize {
        CollectionSize::len(*self)
    }
}

impl<I> CollectionSize for HashSet<I> {
    fn len(&self) -> usize {
        HashSet::len(self)
    }
}

impl<I> CollectionSize for BTreeSet<I> {
    fn len(&self) -> usize {
        BTreeSet::len(self)
    }
}

impl<I> CollectionSize for Vec<I> {
    fn len(&self) -> usize {
        Vec::len(self)
    }
}
