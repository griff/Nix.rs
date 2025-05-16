pub trait StateParse<I> {
    type Err;
    fn parse(&self, s: &str) -> Result<I, Self::Err>;
}

impl<T, I> StateParse<I> for &T
where
    T: StateParse<I>,
{
    type Err = T::Err;

    fn parse(&self, s: &str) -> Result<I, Self::Err> {
        StateParse::parse(*self, s)
    }
}
