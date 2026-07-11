use capnp::Error;
use capnp_convert::{BuildFrom as _, ReadFrom, ReadInto as _, SetInto};
use nixrs::derivation::{BasicDerivation, DerivationOutput};

use crate::capnp::nix_daemon_capnp;

impl<'b> SetInto<nix_daemon_capnp::derivation_output::Builder<'b>> for DerivationOutput {
    fn set_into(
        &self,
        builder: &mut nix_daemon_capnp::derivation_output::Builder<'b>,
    ) -> capnp::Result<()> {
        match self {
            DerivationOutput::InputAddressed(path) => {
                builder.reborrow().init_input_addressed().build_from(path)?;
            }
            DerivationOutput::CAFixed(ca) => {
                builder.reborrow().init_ca_fixed().build_from(ca)?;
            }
            DerivationOutput::Deferred => {
                builder.set_deferred(());
            }
            #[cfg(feature = "xp-ca-derivations")]
            DerivationOutput::CAFloating(cama) => {
                builder.reborrow().init_ca_floating().build_from(cama)?;
            }
            #[cfg(feature = "xp-impure-derivations")]
            DerivationOutput::Impure(cama) => {
                builder.reborrow().init_impure().build_from(cama)?;
            }
        }
        Ok(())
    }
}

impl<'r> ReadFrom<nix_daemon_capnp::derivation_output::Reader<'r>> for DerivationOutput {
    fn read_from(reader: nix_daemon_capnp::derivation_output::Reader<'r>) -> Result<Self, Error> {
        match reader.which()? {
            nix_daemon_capnp::derivation_output::Which::InputAddressed(r) => {
                let path = r?.read_into()?;
                Ok(DerivationOutput::InputAddressed(path))
            }
            nix_daemon_capnp::derivation_output::Which::CaFixed(r) => {
                let ca = r?.read_into()?;
                Ok(DerivationOutput::CAFixed(ca))
            }
            nix_daemon_capnp::derivation_output::Which::Deferred(_) => {
                Ok(DerivationOutput::Deferred)
            }
            #[cfg(feature = "xp-ca-derivations")]
            nix_daemon_capnp::derivation_output::Which::CaFloating(r) => {
                let cama: nixrs::store_path::ContentAddressMethodAlgorithm = r?.read_into()?;
                Ok(DerivationOutput::CAFloating(cama))
            }
            #[cfg(not(feature = "xp-ca-derivations"))]
            nix_daemon_capnp::derivation_output::Which::CaFloating(r) => {
                r?;
                Err(Error::unimplemented(
                    "xp-ca-derivations feature is not enabled in build".into(),
                ))
            }
            #[cfg(feature = "xp-impure-derivations")]
            nix_daemon_capnp::derivation_output::Which::Impure(r) => {
                let cama: nixrs::store_path::ContentAddressMethodAlgorithm = r?.read_into()?;
                Ok(DerivationOutput::Impure(cama))
            }
            #[cfg(not(feature = "xp-impure-derivations"))]
            nix_daemon_capnp::derivation_output::Which::Impure(r) => {
                r?;
                Err(Error::unimplemented(
                    "xp-impure-derivations feature is not enabled in build".into(),
                ))
            }
        }
    }
}

impl<'b> SetInto<nix_daemon_capnp::basic_derivation::Builder<'b>> for BasicDerivation {
    fn set_into(
        &self,
        builder: &mut nix_daemon_capnp::basic_derivation::Builder<'b>,
    ) -> capnp::Result<()> {
        builder
            .reborrow()
            .init_drv_path()
            .build_from(&self.drv_path)?;
        builder
            .reborrow()
            .init_outputs()
            .build_from(&self.outputs)?;
        builder
            .reborrow()
            .init_input_srcs(self.input_srcs.len() as u32)
            .build_from(&self.input_srcs)?;
        builder.set_platform(self.platform.as_ref());
        builder.set_builder(self.builder.as_ref());
        builder
            .reborrow()
            .init_args(self.args.len() as u32)
            .build_from(&self.args)?;
        let mut entries = builder
            .reborrow()
            .init_env()
            .init_entries(self.env.len() as u32);
        for (index, (key, value)) in self.env.iter().enumerate() {
            let mut entry = entries.reborrow().get(index as u32);
            entry.set_key(key.as_ref())?;
            entry.set_value(value.as_ref())?;
        }
        Ok(())
    }
}

impl<'r> ReadFrom<nix_daemon_capnp::basic_derivation::Reader<'r>> for BasicDerivation {
    fn read_from(reader: nix_daemon_capnp::basic_derivation::Reader<'r>) -> Result<Self, Error> {
        let drv_path = reader.get_drv_path()?.read_into()?;
        let outputs = reader.get_outputs()?.read_into()?;
        let input_srcs = reader.get_input_srcs()?.read_into()?;
        let platform = reader.get_platform()?.read_into()?;
        let builder = reader.get_builder()?.read_into()?;
        let args = reader.get_args()?.read_into()?;
        let env = reader.get_env()?.read_into()?;
        Ok(BasicDerivation {
            drv_path,
            outputs,
            input_srcs,
            platform,
            builder,
            args,
            env,
        })
    }
}
