use capnp::Error;
use nixrs::derivation::{BasicDerivation, DerivationOutput};

use crate::capnp::nix_daemon_capnp;
use crate::convert::{BuildFrom, ReadFrom, ReadInto};

impl<'b> BuildFrom<DerivationOutput> for nix_daemon_capnp::derivation_output::Builder<'b> {
    fn build_from(&mut self, input: &DerivationOutput) -> Result<(), Error> {
        match input {
            DerivationOutput::InputAddressed(path) => {
                self.reborrow().set_input_addressed(path)?;
            }
            DerivationOutput::CAFixed(ca) => {
                self.reborrow().init_ca_fixed().build_from(ca)?;
            }
            DerivationOutput::Deferred => {
                self.set_deferred(());
            }
            #[cfg(feature = "xp-ca-derivations")]
            DerivationOutput::CAFloating(cama) => {
                self.reborrow().init_ca_floating().build_from(cama)?;
            }
            #[cfg(feature = "xp-impure-derivations")]
            DerivationOutput::Impure(cama) => {
                self.reborrow().init_impure(cama).build_from(cama)?;
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
                Ok(DerivationOutput::CaFloating(cama))
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

impl<'b> BuildFrom<BasicDerivation> for nix_daemon_capnp::basic_derivation::Builder<'b> {
    fn build_from(&mut self, input: &BasicDerivation) -> Result<(), Error> {
        self.reborrow().set_drv_path(&input.drv_path)?;
        self.reborrow().init_outputs().build_from(&input.outputs)?;
        self.reborrow()
            .init_input_srcs(input.input_srcs.len() as u32)
            .build_from(&input.input_srcs)?;
        self.reborrow().set_platform(input.platform.as_ref());
        self.reborrow().set_builder(input.builder.as_ref());
        self.reborrow()
            .init_args(input.args.len() as u32)
            .build_from(&input.args)?;
        let mut entries = self
            .reborrow()
            .init_env()
            .init_entries(input.env.len() as u32);
        for (index, (key, value)) in input.env.iter().enumerate() {
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
