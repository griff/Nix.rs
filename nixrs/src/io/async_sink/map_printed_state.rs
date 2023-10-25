use super::{CollectionSize, StatePrint};

pub struct MapPrintedColl<S, C> {
    pub state: S,
    pub coll: C,
}

impl<S, C> CollectionSize for MapPrintedColl<S, C>
where
    C: CollectionSize,
{
    fn len(&self) -> usize {
        self.coll.len()
    }
}

impl<'a, S, C, I, IntoIter> IntoIterator for MapPrintedColl<S, C>
where
    C: IntoIterator<Item = &'a I, IntoIter = IntoIter>,
    S: StatePrint<I>,
    IntoIter: Iterator<Item = &'a I>,
    I: 'a,
{
    type Item = String;

    type IntoIter = MapPrintedState<S, IntoIter>;

    fn into_iter(self) -> Self::IntoIter {
        MapPrintedState {
            state: self.state,
            it: self.coll.into_iter(),
        }
    }
}

pub struct MapPrintedState<S, IT> {
    state: S,
    it: IT,
}

impl<'a, S, I, IT> Iterator for MapPrintedState<S, IT>
where
    S: StatePrint<I>,
    IT: Iterator<Item = &'a I>,
    I: 'a,
{
    type Item = String;

    fn next(&mut self) -> Option<Self::Item> {
        self.it.next().map(|item| self.state.print(item))
    }
}
