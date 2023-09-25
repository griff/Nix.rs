use std::collections::{BTreeSet, HashSet};
use std::hash::Hash;

use super::CollectionSize;

pub trait CollectionRead<T>: CollectionSize {
    fn with_capacity(len: usize) -> Self;
    fn push(&mut self, item: T);
}

impl<T> CollectionRead<T> for Vec<T> {
    fn with_capacity(len: usize) -> Self {
        Vec::with_capacity(len)
    }

    fn push(&mut self, item: T) {
        self.push(item);
    }
}

impl<T> CollectionRead<T> for HashSet<T>
where
    T: Eq + Hash,
{
    fn with_capacity(len: usize) -> Self {
        HashSet::with_capacity(len)
    }

    fn push(&mut self, item: T) {
        self.insert(item);
    }
}

impl<T> CollectionRead<T> for BTreeSet<T>
where
    T: Ord + PartialOrd,
{
    fn with_capacity(_len: usize) -> Self {
        BTreeSet::new()
    }

    fn push(&mut self, item: T) {
        self.insert(item);
    }
}
