pub trait StatePrint<I> {
    fn print(&self, item: &I) -> String;
}

impl<'t, T, I> StatePrint<I> for &'t T
where
    T: StatePrint<I>,
{
    fn print(&self, item: &I) -> String {
        StatePrint::print(*self, item)
    }
}
