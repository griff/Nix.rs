use std::collections::{BTreeSet, HashSet};
use std::hash::Hash;

pub trait CollectionRead<T> {
    fn make(len: usize) -> Self;
    fn push(&mut self, item: T);
    fn len(&self) -> usize;
}

impl<T> CollectionRead<T> for Vec<T> {
    fn make(len: usize) -> Self {
        Vec::with_capacity(len)
    }

    fn push(&mut self, item: T) {
        self.push(item);
    }

    fn len(&self) -> usize {
        self.len()
    }
}

impl<T> CollectionRead<T> for HashSet<T>
    where T: Eq + Hash,
{
    fn make(len: usize) -> Self {
        HashSet::with_capacity(len)
    }

    fn push(&mut self, item: T) {
        self.insert(item);
    }

    fn len(&self) -> usize {
        self.len()
    }
}


impl<T> CollectionRead<T> for BTreeSet<T>
    where T: Ord + PartialOrd,
{
    fn make(_len: usize) -> Self {
        BTreeSet::new()
    }

    fn push(&mut self, item: T) {
        self.insert(item);
    }

    fn len(&self) -> usize {
        self.len()
    }
}