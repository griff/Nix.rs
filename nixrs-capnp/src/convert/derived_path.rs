use capnp::traits::{FromPointerBuilder as _, SetterInput};
use capnp::Error;
use nixrs::derived_path::{DerivedPath, OutputSpec, SingleDerivedPath};

use crate::capnp::nix_daemon_capnp;
use crate::convert::{BuildFrom, ReadFrom, ReadInto as _};

impl<'b> BuildFrom<OutputSpec> for nix_daemon_capnp::output_spec::Builder<'b> {
    fn build_from(&mut self, input: &OutputSpec) -> Result<(), Error> {
        match input {
            OutputSpec::All => {
                self.set_all(());
            }
            OutputSpec::Named(names) => {
                self.reborrow()
                    .init_named(names.len() as u32)
                    .build_from(names)?;
            }
        }
        Ok(())
    }
}

impl SetterInput<nix_daemon_capnp::output_spec::Owned> for &'_ OutputSpec {
    fn set_pointer_builder(
        builder: capnp::private::layout::PointerBuilder<'_>,
        input: Self,
        _canonicalize: bool,
    ) -> capnp::Result<()> {
        let mut builder = nix_daemon_capnp::output_spec::Builder::init_pointer(builder, 0);
        match input {
            OutputSpec::All => {
                builder.set_all(());
            }
            OutputSpec::Named(names) => {
                builder
                    .reborrow()
                    .init_named(names.len() as u32)
                    .build_from(names)?;
            }
        }
        Ok(())
    }
}

impl<'r> ReadFrom<nix_daemon_capnp::output_spec::Reader<'r>> for OutputSpec {
    fn read_from(reader: nix_daemon_capnp::output_spec::Reader<'r>) -> Result<Self, Error> {
        match reader.which()? {
            nix_daemon_capnp::output_spec::Which::All(_) => Ok(OutputSpec::All),
            nix_daemon_capnp::output_spec::Which::Named(names) => {
                Ok(OutputSpec::Named(names?.read_into()?))
            }
        }
    }
}

impl<'b> BuildFrom<SingleDerivedPath> for nix_daemon_capnp::single_derived_path::Builder<'b> {
    fn build_from(&mut self, input: &SingleDerivedPath) -> Result<(), Error> {
        match input {
            SingleDerivedPath::Opaque(store_path) => {
                self.set_opaque(store_path)?;
            }
            SingleDerivedPath::Built { drv_path, output } => {
                let mut built = self.reborrow().init_built();
                built.set_drv_path(drv_path.as_ref())?;
                built.set_output(output);
            }
        }
        Ok(())
    }
}

impl SetterInput<nix_daemon_capnp::single_derived_path::Owned> for &'_ SingleDerivedPath {
    fn set_pointer_builder(
        builder: capnp::private::layout::PointerBuilder<'_>,
        input: Self,
        _canonicalize: bool,
    ) -> capnp::Result<()> {
        let mut builder = nix_daemon_capnp::single_derived_path::Builder::init_pointer(builder, 0);
        match input {
            SingleDerivedPath::Opaque(store_path) => {
                builder.set_opaque(store_path)?;
            }
            SingleDerivedPath::Built { drv_path, output } => {
                let mut built = builder.init_built();
                built.set_drv_path(drv_path.as_ref())?;
                built.set_output(output);
            }
        }
        Ok(())
    }
}

impl<'r> ReadFrom<nix_daemon_capnp::single_derived_path::Reader<'r>> for SingleDerivedPath {
    fn read_from(reader: nix_daemon_capnp::single_derived_path::Reader<'r>) -> Result<Self, Error> {
        match reader.which()? {
            nix_daemon_capnp::single_derived_path::Which::Opaque(path) => {
                let path = path?.read_into()?;
                Ok(SingleDerivedPath::Opaque(path))
            }
            nix_daemon_capnp::single_derived_path::Which::Built(built) => {
                let drv_path = Box::new(built.reborrow().get_drv_path()?.read_into()?);
                let output = built.get_output()?.read_into()?;
                Ok(SingleDerivedPath::Built { drv_path, output })
            }
        }
    }
}

impl<'b> BuildFrom<DerivedPath> for nix_daemon_capnp::derived_path::Builder<'b> {
    fn build_from(&mut self, input: &DerivedPath) -> capnp::Result<()> {
        match input {
            DerivedPath::Opaque(store_path) => {
                self.set_opaque(store_path)?;
            }
            DerivedPath::Built { drv_path, outputs } => {
                let mut built = self.reborrow().init_built();
                built.set_drv_path(drv_path)?;
                built.set_outputs(outputs)?;
            }
        }
        Ok(())
    }
}

impl SetterInput<nix_daemon_capnp::derived_path::Owned> for &'_ DerivedPath {
    fn set_pointer_builder(
        builder: capnp::private::layout::PointerBuilder<'_>,
        input: Self,
        _canonicalize: bool,
    ) -> capnp::Result<()> {
        let mut builder = nix_daemon_capnp::derived_path::Builder::init_pointer(builder, 0);
        match input {
            DerivedPath::Opaque(store_path) => {
                builder.set_opaque(store_path)?;
            }
            DerivedPath::Built { drv_path, outputs } => {
                let mut built = builder.init_built();
                built.set_drv_path(drv_path)?;
                built.set_outputs(outputs)?;
            }
        }
        Ok(())
    }
}

impl<'r> ReadFrom<nix_daemon_capnp::derived_path::Reader<'r>> for DerivedPath {
    fn read_from(reader: nix_daemon_capnp::derived_path::Reader<'r>) -> Result<Self, Error> {
        match reader.which()? {
            nix_daemon_capnp::derived_path::Which::Opaque(path) => {
                let path = path?.read_into()?;
                Ok(DerivedPath::Opaque(path))
            }
            nix_daemon_capnp::derived_path::Which::Built(built) => {
                let drv_path = built.reborrow().get_drv_path()?.read_into()?;
                let outputs = built.get_outputs()?.read_into()?;
                Ok(DerivedPath::Built { drv_path, outputs })
            }
        }
    }
}
