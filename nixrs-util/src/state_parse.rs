pub trait StateParse<I> {
    type Err;
    fn parse(&self, s: &str) -> Result<I, Self::Err>;
}

impl<'t, T, I> StateParse<I> for &'t T
where
    T: StateParse<I>,
{
    type Err = T::Err;

    fn parse(&self, s: &str) -> Result<I, Self::Err> {
        StateParse::parse(*self, s)
    }
}
