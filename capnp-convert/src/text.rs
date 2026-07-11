use crate::SetInto;

impl<'b, S> SetInto<capnp::text::Builder<'b>> for S
where
    S: AsRef<str>,
{
    fn set_into(&self, builder: &mut capnp::text::Builder<'b>) -> capnp::Result<()> {
        builder.push_str(self.as_ref());
        Ok(())
    }

    fn len(&self) -> u32 {
        self.as_ref().len() as u32
    }
}
