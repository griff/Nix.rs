use std::fmt;

pub trait StateDisplay<'a, I> {
    type Output : fmt::Display + 'a;
    fn display(&'a self, item: &'a I) -> Self::Output;
}

impl<'t, T, I> StateDisplay<'t, I> for &'t T
where
    T: StateDisplay<'t, I> + 't,
{
    type Output = T::Output;
    fn display(&'t self, item: &'t I) -> Self::Output {
        StateDisplay::display(*self, item)
    }
}
