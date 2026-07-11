use capnp::Error;
use capnp_convert::{BuildFrom as _, ReadFrom, ReadInto as _, SetInto};
use nixrs::realisation::{DrvOutput, Realisation};

use crate::capnp::nix_daemon_capnp;

impl<'b> SetInto<nix_daemon_capnp::drv_output::Builder<'b>> for DrvOutput {
    fn set_into(
        &self,
        builder: &mut nix_daemon_capnp::drv_output::Builder<'b>,
    ) -> capnp::Result<()> {
        builder
            .reborrow()
            .init_drv_hash()
            .build_from(&self.drv_hash)?;
        builder.set_output_name(&self.output_name);
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

impl<'b> SetInto<nix_daemon_capnp::realisation::Builder<'b>> for Realisation {
    fn set_into(
        &self,
        builder: &mut nix_daemon_capnp::realisation::Builder<'b>,
    ) -> capnp::Result<()> {
        builder.reborrow().init_id().build_from(&self.id)?;
        builder
            .reborrow()
            .init_out_path()
            .build_from(&self.out_path)?;
        builder
            .reborrow()
            .init_signatures(self.signatures.len() as u32)
            .build_from(&self.signatures)?;
        builder
            .reborrow()
            .init_dependent_realisations()
            .build_from(&self.dependent_realisations)?;
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
