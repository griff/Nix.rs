pub trait StatePrint<I> {
    fn print(&self, item: &I) -> String;
}

impl<T, I> StatePrint<I> for &T
where
    T: StatePrint<I>,
{
    fn print(&self, item: &I) -> String {
        StatePrint::print(*self, item)
    }
}
