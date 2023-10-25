use std::collections::{BTreeSet, HashSet};

pub trait CollectionSize {
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl<'a, C> CollectionSize for &'a [C] {
    fn len(&self) -> usize {
        (*self).len()
    }
    fn is_empty(&self) -> bool {
        (*self).is_empty()
    }
}

impl<'a, C> CollectionSize for &'a C
where
    C: CollectionSize,
{
    fn len(&self) -> usize {
        CollectionSize::len(*self)
    }
    fn is_empty(&self) -> bool {
        CollectionSize::is_empty(*self)
    }
}

impl<I> CollectionSize for HashSet<I> {
    fn len(&self) -> usize {
        HashSet::len(self)
    }
    fn is_empty(&self) -> bool {
        HashSet::is_empty(self)
    }
}

impl<I> CollectionSize for BTreeSet<I> {
    fn len(&self) -> usize {
        BTreeSet::len(self)
    }
    fn is_empty(&self) -> bool {
        BTreeSet::is_empty(self)
    }
}

impl<I> CollectionSize for Vec<I> {
    fn len(&self) -> usize {
        Vec::len(self)
    }
    fn is_empty(&self) -> bool {
        Vec::is_empty(self)
    }
}
