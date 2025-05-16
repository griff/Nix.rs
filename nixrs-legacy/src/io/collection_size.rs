use std::collections::{BTreeSet, HashSet};

#[allow(clippy::len_without_is_empty)]
pub trait CollectionSize {
    fn len(&self) -> usize;
}

impl<C> CollectionSize for &[C] {
    fn len(&self) -> usize {
        (*self).len()
    }
}

impl<C> CollectionSize for &C
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
