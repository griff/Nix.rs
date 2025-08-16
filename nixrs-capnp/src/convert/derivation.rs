use capnp::Error;
use capnp::traits::{FromPointerBuilder as _, SetterInput};
use nixrs::derivation::{BasicDerivation, DerivationOutput};

use crate::capnp::nix_daemon_capnp;
use crate::convert::{BuildFrom, ReadFrom, ReadInto};

impl<'b> BuildFrom<DerivationOutput> for nix_daemon_capnp::derivation_output::Builder<'b> {
    fn build_from(&mut self, input: &DerivationOutput) -> Result<(), Error> {
        match input {
            DerivationOutput::InputAddressed(path) => {
                self.set_input_addressed(path)?;
            }
            DerivationOutput::CAFixed(ca) => {
                self.set_ca_fixed(ca)?;
            }
            DerivationOutput::Deferred => {
                self.set_deferred(());
            }
            #[cfg(feature = "xp-ca-derivations")]
            DerivationOutput::CAFloating(cama) => {
                self.set_ca_floating(cama)?;
            }
            #[cfg(feature = "xp-impure-derivations")]
            DerivationOutput::Impure(cama) => {
                self.set_impure(cama)?;
            }
        }
        Ok(())
    }
}

impl SetterInput<nix_daemon_capnp::derivation_output::Owned> for &'_ DerivationOutput {
    fn set_pointer_builder(
        builder: capnp::private::layout::PointerBuilder<'_>,
        input: Self,
        _canonicalize: bool,
    ) -> capnp::Result<()> {
        let mut builder = nix_daemon_capnp::derivation_output::Builder::init_pointer(builder, 0);
        match input {
            DerivationOutput::InputAddressed(path) => {
                builder.set_input_addressed(path)?;
            }
            DerivationOutput::CAFixed(ca) => {
                builder.set_ca_fixed(ca)?;
            }
            DerivationOutput::Deferred => {
                builder.set_deferred(());
            }
            #[cfg(feature = "xp-ca-derivations")]
            DerivationOutput::CAFloating(cama) => {
                builder.set_ca_floating(cama)?;
            }
            #[cfg(feature = "xp-impure-derivations")]
            DerivationOutput::Impure(cama) => {
                builder.set_impure(cama)?;
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
        self.set_drv_path(&input.drv_path)?;
        self.reborrow().init_outputs().build_from(&input.outputs)?;
        self.reborrow()
            .init_input_srcs(input.input_srcs.len() as u32)
            .build_from(&input.input_srcs)?;
        self.set_platform(input.platform.as_ref());
        self.set_builder(input.builder.as_ref());
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

impl SetterInput<nix_daemon_capnp::basic_derivation::Owned> for &'_ BasicDerivation {
    fn set_pointer_builder(
        builder: capnp::private::layout::PointerBuilder<'_>,
        input: Self,
        _canonicalize: bool,
    ) -> capnp::Result<()> {
        let mut builder = nix_daemon_capnp::basic_derivation::Builder::init_pointer(builder, 0);
        builder.set_drv_path(&input.drv_path)?;
        builder
            .reborrow()
            .init_outputs()
            .build_from(&input.outputs)?;
        builder
            .reborrow()
            .init_input_srcs(input.input_srcs.len() as u32)
            .build_from(&input.input_srcs)?;
        builder.set_platform(input.platform.as_ref());
        builder.set_builder(input.builder.as_ref());
        builder
            .reborrow()
            .init_args(input.args.len() as u32)
            .build_from(&input.args)?;
        let mut entries = builder
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
