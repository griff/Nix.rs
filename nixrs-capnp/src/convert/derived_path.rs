use capnp::Error;
use capnp_convert::{BuildFrom as _, ReadFrom, ReadInto as _, SetInto};
use nixrs::derived_path::{DerivedPath, OutputSpec, SingleDerivedPath};

use crate::capnp::nix_daemon_capnp;

impl<'b> SetInto<nix_daemon_capnp::output_spec::Builder<'b>> for OutputSpec {
    fn set_into(
        &self,
        builder: &mut nix_daemon_capnp::output_spec::Builder<'b>,
    ) -> capnp::Result<()> {
        match self {
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

impl<'b> SetInto<nix_daemon_capnp::single_derived_path::Builder<'b>> for SingleDerivedPath {
    fn set_into(
        &self,
        builder: &mut nix_daemon_capnp::single_derived_path::Builder<'b>,
    ) -> capnp::Result<()> {
        match self {
            SingleDerivedPath::Opaque(store_path) => {
                builder.reborrow().init_opaque().build_from(store_path)?;
            }
            SingleDerivedPath::Built { drv_path, output } => {
                let mut built = builder.reborrow().init_built();
                built
                    .reborrow()
                    .init_drv_path()
                    .build_from(drv_path.as_ref())?;
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

impl<'b> SetInto<nix_daemon_capnp::derived_path::Builder<'b>> for DerivedPath {
    fn set_into(
        &self,
        builder: &mut nix_daemon_capnp::derived_path::Builder<'b>,
    ) -> capnp::Result<()> {
        match self {
            DerivedPath::Opaque(store_path) => {
                builder.reborrow().init_opaque().build_from(store_path)?;
            }
            DerivedPath::Built { drv_path, outputs } => {
                let mut built = builder.reborrow().init_built();
                built.reborrow().init_drv_path().build_from(drv_path)?;
                built.init_outputs().build_from(outputs)?;
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
