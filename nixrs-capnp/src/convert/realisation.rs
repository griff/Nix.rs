use capnp::traits::{FromPointerBuilder, SetterInput};
use capnp::Error;
use nixrs::realisation::{DrvOutput, Realisation};

use crate::capnp::nix_daemon_capnp;
use crate::convert::{BuildFrom, ReadFrom, ReadInto};

impl<'b> BuildFrom<DrvOutput> for nix_daemon_capnp::drv_output::Builder<'b> {
    fn build_from(&mut self, input: &DrvOutput) -> Result<(), Error> {
        self.set_drv_hash(&input.drv_hash)?;
        self.set_output_name(&input.output_name);
        Ok(())
    }
}

impl SetterInput<nix_daemon_capnp::drv_output::Owned> for &'_ DrvOutput {
    fn set_pointer_builder(
        builder: capnp::private::layout::PointerBuilder<'_>,
        input: Self,
        _canonicalize: bool,
    ) -> capnp::Result<()> {
        let mut builder = nix_daemon_capnp::drv_output::Builder::init_pointer(builder, 0);
        builder.set_drv_hash(&input.drv_hash)?;
        builder.set_output_name(&input.output_name);
        Ok(())
    }
}

impl<'r> ReadFrom<nix_daemon_capnp::drv_output::Reader<'r>> for DrvOutput {
    fn read_from(reader: nix_daemon_capnp::drv_output::Reader<'r>) -> Result<Self, Error> {
        let drv_hash = reader.get_drv_hash()?.read_into()?;
        let output_name = reader.get_output_name()?.read_into()?;
        Ok(DrvOutput {
            drv_hash,
            output_name,
        })
    }
}

impl<'b> BuildFrom<Realisation> for nix_daemon_capnp::realisation::Builder<'b> {
    fn build_from(&mut self, input: &Realisation) -> Result<(), Error> {
        self.set_id(&input.id)?;
        self.set_out_path(&input.out_path)?;
        self.reborrow()
            .init_signatures(input.signatures.len() as u32)
            .build_from(&input.signatures)?;
        self.reborrow()
            .init_dependent_realisations()
            .build_from(&input.dependent_realisations)?;
        Ok(())
    }
}

impl SetterInput<nix_daemon_capnp::realisation::Owned> for &'_ Realisation {
    fn set_pointer_builder(
        builder: capnp::private::layout::PointerBuilder<'_>,
        input: Self,
        _canonicalize: bool,
    ) -> capnp::Result<()> {
        let mut builder = nix_daemon_capnp::realisation::Builder::init_pointer(builder, 0);
        builder.set_id(&input.id)?;
        builder.set_out_path(&input.out_path)?;
        builder
            .reborrow()
            .init_signatures(input.signatures.len() as u32)
            .build_from(&input.signatures)?;
        builder
            .reborrow()
            .init_dependent_realisations()
            .build_from(&input.dependent_realisations)?;
        Ok(())
    }
}

impl<'r> ReadFrom<nix_daemon_capnp::realisation::Reader<'r>> for Realisation {
    fn read_from(reader: nix_daemon_capnp::realisation::Reader<'r>) -> Result<Self, Error> {
        let id = reader.get_id()?.read_into()?;
        let out_path = reader.get_out_path()?.read_into()?;
        let signatures = reader.get_signatures()?.read_into()?;
        let dependent_realisations = reader.get_dependent_realisations()?.read_into()?;
        Ok(Realisation {
            id,
            out_path,
            signatures,
            dependent_realisations,
        })
    }
}
