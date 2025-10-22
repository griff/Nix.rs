use bytes::Bytes;
use capnp::Error;
use capnp::traits::{FromPointerBuilder as _, SetterInput};
use nixrs::daemon::wire::types2::{
    BuildMode, BuildResult, BuildStatus, KeyedBuildResult, QueryMissingResult, ValidPathInfo,
};
use nixrs::daemon::{ClientOptions, UnkeyedValidPathInfo};

use crate::capnp::nix_daemon_capnp;
use crate::capnp::nixrs_capnp::store_path_info;
use crate::convert::{BuildFrom, ReadFrom, ReadInto as _};

impl From<nix_daemon_capnp::BuildStatus> for BuildStatus {
    fn from(value: nix_daemon_capnp::BuildStatus) -> Self {
        match value {
            nix_daemon_capnp::BuildStatus::Built => BuildStatus::Built,
            nix_daemon_capnp::BuildStatus::Substituted => BuildStatus::Substituted,
            nix_daemon_capnp::BuildStatus::AlreadyValid => BuildStatus::AlreadyValid,
            nix_daemon_capnp::BuildStatus::PermanentFailure => BuildStatus::PermanentFailure,
            nix_daemon_capnp::BuildStatus::InputRejected => BuildStatus::InputRejected,
            nix_daemon_capnp::BuildStatus::OutputRejected => BuildStatus::OutputRejected,
            nix_daemon_capnp::BuildStatus::TransientFailure => BuildStatus::TransientFailure,
            nix_daemon_capnp::BuildStatus::CachedFailure => BuildStatus::CachedFailure,
            nix_daemon_capnp::BuildStatus::TimedOut => BuildStatus::TimedOut,
            nix_daemon_capnp::BuildStatus::MiscFailure => BuildStatus::MiscFailure,
            nix_daemon_capnp::BuildStatus::DependencyFailed => BuildStatus::DependencyFailed,
            nix_daemon_capnp::BuildStatus::LogLimitExceeded => BuildStatus::LogLimitExceeded,
            nix_daemon_capnp::BuildStatus::NotDeterministic => BuildStatus::NotDeterministic,
            nix_daemon_capnp::BuildStatus::ResolvesToAlreadyValid => {
                BuildStatus::ResolvesToAlreadyValid
            }
            nix_daemon_capnp::BuildStatus::NoSubstituters => BuildStatus::NoSubstituters,
        }
    }
}

impl From<BuildStatus> for nix_daemon_capnp::BuildStatus {
    fn from(value: BuildStatus) -> Self {
        match value {
            BuildStatus::Built => nix_daemon_capnp::BuildStatus::Built,
            BuildStatus::Substituted => nix_daemon_capnp::BuildStatus::Substituted,
            BuildStatus::AlreadyValid => nix_daemon_capnp::BuildStatus::AlreadyValid,
            BuildStatus::PermanentFailure => nix_daemon_capnp::BuildStatus::PermanentFailure,
            BuildStatus::InputRejected => nix_daemon_capnp::BuildStatus::InputRejected,
            BuildStatus::OutputRejected => nix_daemon_capnp::BuildStatus::OutputRejected,
            BuildStatus::TransientFailure => nix_daemon_capnp::BuildStatus::TransientFailure,
            BuildStatus::CachedFailure => nix_daemon_capnp::BuildStatus::CachedFailure,
            BuildStatus::TimedOut => nix_daemon_capnp::BuildStatus::TimedOut,
            BuildStatus::MiscFailure => nix_daemon_capnp::BuildStatus::MiscFailure,
            BuildStatus::DependencyFailed => nix_daemon_capnp::BuildStatus::DependencyFailed,
            BuildStatus::LogLimitExceeded => nix_daemon_capnp::BuildStatus::LogLimitExceeded,
            BuildStatus::NotDeterministic => nix_daemon_capnp::BuildStatus::NotDeterministic,
            BuildStatus::ResolvesToAlreadyValid => {
                nix_daemon_capnp::BuildStatus::ResolvesToAlreadyValid
            }
            BuildStatus::NoSubstituters => nix_daemon_capnp::BuildStatus::NoSubstituters,
        }
    }
}

impl From<BuildMode> for nix_daemon_capnp::BuildMode {
    fn from(value: BuildMode) -> Self {
        match value {
            BuildMode::Normal => nix_daemon_capnp::BuildMode::Normal,
            BuildMode::Repair => nix_daemon_capnp::BuildMode::Repair,
            BuildMode::Check => nix_daemon_capnp::BuildMode::Check,
        }
    }
}

impl From<nix_daemon_capnp::BuildMode> for BuildMode {
    fn from(value: nix_daemon_capnp::BuildMode) -> Self {
        match value {
            nix_daemon_capnp::BuildMode::Normal => BuildMode::Normal,
            nix_daemon_capnp::BuildMode::Repair => BuildMode::Repair,
            nix_daemon_capnp::BuildMode::Check => BuildMode::Check,
        }
    }
}

impl<'b> BuildFrom<ClientOptions> for nix_daemon_capnp::client_options::Builder<'b> {
    fn build_from(&mut self, options: &ClientOptions) -> Result<(), Error> {
        self.set_keep_failed(options.keep_failed);
        self.set_keep_going(options.keep_going);
        self.set_try_fallback(options.try_fallback);
        self.set_verbosity(options.verbosity.into());
        self.set_max_build_jobs(options.max_build_jobs);
        self.set_max_silent_time(options.max_silent_time);
        self.set_verbose_build(options.verbose_build.into());
        self.set_build_cores(options.build_cores);
        self.set_use_substitutes(options.use_substitutes);
        if !options.other_settings.is_empty() {
            let other = self.reborrow().init_other_settings();
            let mut entries = other.init_entries(options.other_settings.len() as u32);
            for (index, (k, v)) in options.other_settings.iter().enumerate() {
                let mut entry = entries.reborrow().get(index as u32);
                entry.set_key(k)?;
                entry.set_value(&v[..])?;
            }
        }
        Ok(())
    }
}

impl SetterInput<nix_daemon_capnp::client_options::Owned> for &'_ ClientOptions {
    fn set_pointer_builder(
        builder: capnp::private::layout::PointerBuilder<'_>,
        input: Self,
        _canonicalize: bool,
    ) -> capnp::Result<()> {
        let mut builder = nix_daemon_capnp::client_options::Builder::init_pointer(builder, 0);
        builder.set_keep_failed(input.keep_failed);
        builder.set_keep_going(input.keep_going);
        builder.set_try_fallback(input.try_fallback);
        builder.set_verbosity(input.verbosity.into());
        builder.set_max_build_jobs(input.max_build_jobs);
        builder.set_max_silent_time(input.max_silent_time);
        builder.set_verbose_build(input.verbose_build.into());
        builder.set_build_cores(input.build_cores);
        builder.set_use_substitutes(input.use_substitutes);
        if !input.other_settings.is_empty() {
            let other = builder.reborrow().init_other_settings();
            let mut entries = other.init_entries(input.other_settings.len() as u32);
            for (index, (k, v)) in input.other_settings.iter().enumerate() {
                let mut entry = entries.reborrow().get(index as u32);
                entry.set_key(k)?;
                entry.set_value(&v[..])?;
            }
        }
        Ok(())
    }
}

impl<'r> ReadFrom<nix_daemon_capnp::client_options::Reader<'r>> for ClientOptions {
    fn read_from(reader: nix_daemon_capnp::client_options::Reader<'r>) -> Result<Self, Error> {
        let mut options = ClientOptions::default();
        options.keep_failed = reader.get_keep_failed();
        options.keep_going = reader.get_keep_going();
        options.try_fallback = reader.get_try_fallback();
        options.verbosity = reader.get_verbosity()?.into();
        options.max_build_jobs = reader.get_max_build_jobs();
        options.max_silent_time = reader.get_max_silent_time();
        options.verbose_build = reader.get_verbose_build()?.into();
        options.build_cores = reader.get_build_cores();
        options.use_substitutes = reader.get_use_substitutes();
        options.other_settings = reader.get_other_settings()?.read_into()?;
        Ok(options)
    }
}

impl<'b> BuildFrom<BuildResult> for nix_daemon_capnp::build_result::Builder<'b> {
    fn build_from(&mut self, input: &BuildResult) -> Result<(), Error> {
        self.set_status(input.status.into());
        self.set_error_msg(input.error_msg.as_ref());
        self.set_times_built(input.times_built);
        self.set_is_non_deterministic(input.is_non_deterministic);
        self.set_start_time(input.start_time);
        self.set_stop_time(input.stop_time);
        if let Some(cpu_user) = input.cpu_user.as_ref() {
            self.set_cpu_user((*cpu_user).into());
        }
        if let Some(cpu_system) = input.cpu_system.as_ref() {
            self.set_cpu_system((*cpu_system).into());
        }
        self.reborrow()
            .init_built_outputs()
            .build_from(&input.built_outputs)?;
        Ok(())
    }
}

impl SetterInput<nix_daemon_capnp::build_result::Owned> for &'_ BuildResult {
    fn set_pointer_builder(
        builder: capnp::private::layout::PointerBuilder<'_>,
        input: Self,
        _canonicalize: bool,
    ) -> capnp::Result<()> {
        let mut builder = nix_daemon_capnp::build_result::Builder::init_pointer(builder, 0);
        builder.set_status(input.status.into());
        builder.set_error_msg(input.error_msg.as_ref());
        builder.set_times_built(input.times_built);
        builder.set_is_non_deterministic(input.is_non_deterministic);
        builder.set_start_time(input.start_time);
        builder.set_stop_time(input.stop_time);
        if let Some(cpu_user) = input.cpu_user.as_ref() {
            builder.set_cpu_user((*cpu_user).into());
        }
        if let Some(cpu_system) = input.cpu_system.as_ref() {
            builder.set_cpu_system((*cpu_system).into());
        }
        builder
            .reborrow()
            .init_built_outputs()
            .build_from(&input.built_outputs)?;
        Ok(())
    }
}

impl<'r> ReadFrom<nix_daemon_capnp::build_result::Reader<'r>> for BuildResult {
    fn read_from(value: nix_daemon_capnp::build_result::Reader<'r>) -> Result<Self, Error> {
        let status = value.get_status()?.into();
        let error_msg = Bytes::copy_from_slice(value.get_error_msg()?);
        let times_built = value.get_times_built();
        let is_non_deterministic = value.get_is_non_deterministic();
        let start_time = value.get_start_time();
        let stop_time = value.get_stop_time();
        let cpu_user = if value.get_cpu_user() < 0 {
            None
        } else {
            Some(value.get_cpu_user().into())
        };
        let cpu_system = if value.get_cpu_system() < 0 {
            None
        } else {
            Some(value.get_cpu_system().into())
        };
        let built_outputs = value.get_built_outputs()?.read_into()?;
        Ok(BuildResult {
            status,
            error_msg,
            times_built,
            is_non_deterministic,
            start_time,
            stop_time,
            cpu_user,
            cpu_system,
            built_outputs,
        })
    }
}

impl<'b> BuildFrom<KeyedBuildResult> for nix_daemon_capnp::keyed_build_result::Builder<'b> {
    fn build_from(&mut self, input: &KeyedBuildResult) -> Result<(), Error> {
        self.set_path(&input.path)?;
        self.set_result(&input.result)?;
        Ok(())
    }
}

impl SetterInput<nix_daemon_capnp::keyed_build_result::Owned> for &'_ KeyedBuildResult {
    fn set_pointer_builder(
        builder: capnp::private::layout::PointerBuilder<'_>,
        input: Self,
        _canonicalize: bool,
    ) -> capnp::Result<()> {
        let mut builder = nix_daemon_capnp::keyed_build_result::Builder::init_pointer(builder, 0);
        builder.set_path(&input.path)?;
        builder.set_result(&input.result)?;
        Ok(())
    }
}

impl<'r> ReadFrom<nix_daemon_capnp::keyed_build_result::Reader<'r>> for KeyedBuildResult {
    fn read_from(value: nix_daemon_capnp::keyed_build_result::Reader<'r>) -> Result<Self, Error> {
        let path = value.get_path()?.read_into()?;
        let result = value.get_result()?.read_into()?;
        Ok(KeyedBuildResult { path, result })
    }
}

impl<'b> BuildFrom<QueryMissingResult> for nix_daemon_capnp::query_missing_result::Builder<'b> {
    fn build_from(&mut self, input: &QueryMissingResult) -> Result<(), Error> {
        self.reborrow()
            .init_unknown(input.unknown.len() as u32)
            .build_from(&input.unknown)?;
        self.reborrow()
            .init_will_build(input.will_build.len() as u32)
            .build_from(&input.will_build)?;
        self.reborrow()
            .init_will_substitute(input.will_substitute.len() as u32)
            .build_from(&input.will_substitute)?;
        self.set_download_size(input.download_size);
        self.set_nar_size(input.nar_size);
        Ok(())
    }
}

impl SetterInput<nix_daemon_capnp::query_missing_result::Owned> for &'_ QueryMissingResult {
    fn set_pointer_builder(
        builder: capnp::private::layout::PointerBuilder<'_>,
        input: Self,
        _canonicalize: bool,
    ) -> capnp::Result<()> {
        let mut builder = nix_daemon_capnp::query_missing_result::Builder::init_pointer(builder, 0);
        builder
            .reborrow()
            .init_unknown(input.unknown.len() as u32)
            .build_from(&input.unknown)?;
        builder
            .reborrow()
            .init_will_build(input.will_build.len() as u32)
            .build_from(&input.will_build)?;
        builder
            .reborrow()
            .init_will_substitute(input.will_substitute.len() as u32)
            .build_from(&input.will_substitute)?;
        builder.set_download_size(input.download_size);
        builder.set_nar_size(input.nar_size);
        Ok(())
    }
}

impl<'r> ReadFrom<nix_daemon_capnp::query_missing_result::Reader<'r>> for QueryMissingResult {
    fn read_from(
        reader: nix_daemon_capnp::query_missing_result::Reader<'r>,
    ) -> Result<Self, Error> {
        let will_build = reader.get_will_build()?.read_into()?;
        let will_substitute = reader.get_will_substitute()?.read_into()?;
        let unknown = reader.get_unknown()?.read_into()?;
        let download_size = reader.get_download_size();
        let nar_size = reader.get_nar_size();
        Ok(QueryMissingResult {
            will_build,
            will_substitute,
            unknown,
            download_size,
            nar_size,
        })
    }
}

impl<'b> BuildFrom<UnkeyedValidPathInfo>
    for nix_daemon_capnp::unkeyed_valid_path_info::Builder<'b>
{
    fn build_from(&mut self, input: &UnkeyedValidPathInfo) -> Result<(), Error> {
        if let Some(deriver) = input.deriver.as_ref() {
            self.set_deriver(deriver)?;
        }
        self.set_nar_hash(input.nar_hash.digest_bytes());
        self.reborrow()
            .init_references(input.references.len() as u32)
            .build_from(&input.references)?;
        self.set_registration_time(input.registration_time);
        self.set_nar_size(input.nar_size);
        self.set_ultimate(input.ultimate);
        self.reborrow()
            .init_signatures(input.signatures.len() as u32)
            .build_from(&input.signatures)?;
        if let Some(ca) = input.ca.as_ref() {
            self.set_ca(ca)?;
        }
        Ok(())
    }
}

impl SetterInput<nix_daemon_capnp::unkeyed_valid_path_info::Owned> for &'_ UnkeyedValidPathInfo {
    fn set_pointer_builder(
        builder: capnp::private::layout::PointerBuilder<'_>,
        input: Self,
        _canonicalize: bool,
    ) -> capnp::Result<()> {
        let mut builder =
            nix_daemon_capnp::unkeyed_valid_path_info::Builder::init_pointer(builder, 0);
        if let Some(deriver) = input.deriver.as_ref() {
            builder.set_deriver(deriver)?;
        }
        builder.set_nar_hash(input.nar_hash.digest_bytes());
        builder
            .reborrow()
            .init_references(input.references.len() as u32)
            .build_from(&input.references)?;
        builder.set_registration_time(input.registration_time);
        builder.set_nar_size(input.nar_size);
        builder.set_ultimate(input.ultimate);
        builder
            .reborrow()
            .init_signatures(input.signatures.len() as u32)
            .build_from(&input.signatures)?;
        if let Some(ca) = input.ca.as_ref() {
            builder.set_ca(ca)?;
        }
        Ok(())
    }
}

impl<'r> ReadFrom<nix_daemon_capnp::unkeyed_valid_path_info::Reader<'r>> for UnkeyedValidPathInfo {
    fn read_from(
        reader: nix_daemon_capnp::unkeyed_valid_path_info::Reader<'r>,
    ) -> Result<Self, Error> {
        let deriver = if reader.has_deriver() {
            Some(reader.get_deriver()?.read_into()?)
        } else {
            None
        };
        let nar_hash = reader.get_nar_hash()?.read_into()?;
        let references = reader.get_references()?.read_into()?;
        let registration_time = reader.get_registration_time();
        let nar_size = reader.get_nar_size();
        let ultimate = reader.get_ultimate();
        let signatures = reader.get_signatures()?.read_into()?;
        let ca = if reader.has_ca() {
            Some(reader.get_ca()?.read_into()?)
        } else {
            None
        };
        Ok(UnkeyedValidPathInfo {
            deriver,
            nar_hash,
            references,
            registration_time,
            nar_size,
            ultimate,
            signatures,
            ca,
        })
    }
}

impl<'b> BuildFrom<ValidPathInfo> for nix_daemon_capnp::valid_path_info::Builder<'b> {
    fn build_from(&mut self, input: &ValidPathInfo) -> Result<(), Error> {
        self.set_path(&input.path)?;
        self.set_info(&input.info)?;
        Ok(())
    }
}

impl SetterInput<nix_daemon_capnp::valid_path_info::Owned> for &'_ ValidPathInfo {
    fn set_pointer_builder(
        builder: capnp::private::layout::PointerBuilder<'_>,
        input: Self,
        _canonicalize: bool,
    ) -> capnp::Result<()> {
        let mut builder = nix_daemon_capnp::valid_path_info::Builder::init_pointer(builder, 0);
        builder.set_path(&input.path)?;
        builder.set_info(&input.info)?;
        Ok(())
    }
}

impl<'r> ReadFrom<nix_daemon_capnp::valid_path_info::Reader<'r>> for ValidPathInfo {
    fn read_from(reader: nix_daemon_capnp::valid_path_info::Reader<'r>) -> Result<Self, Error> {
        let path = reader.get_path()?.read_into()?;
        let info = reader.get_info()?.read_into()?;
        Ok(ValidPathInfo { path, info })
    }
}

impl<'r> ReadFrom<store_path_info::Reader<'r>> for UnkeyedValidPathInfo {
    fn read_from(reader: store_path_info::Reader<'r>) -> Result<Self, Error> {
        let deriver = if reader.has_deriver() {
            Some(reader.get_deriver()?.read_into()?)
        } else {
            None
        };
        let nar_hash = reader.get_nar_hash()?.read_into()?;
        let references = reader.get_references()?.read_into()?;
        let registration_time = reader.get_registration_time();
        let nar_size = reader.get_nar_size();
        let ultimate = reader.get_ultimate();
        let signatures = reader.get_signatures()?.read_into()?;
        let ca = if reader.has_ca() {
            Some(reader.get_ca()?.read_into()?)
        } else {
            None
        };
        Ok(UnkeyedValidPathInfo {
            deriver,
            nar_hash,
            references,
            registration_time,
            nar_size,
            ultimate,
            signatures,
            ca,
        })
    }
}

impl<'r> ReadFrom<store_path_info::Reader<'r>> for ValidPathInfo {
    fn read_from(reader: store_path_info::Reader<'r>) -> Result<Self, Error> {
        let path = reader.get_store_path()?.read_into()?;
        let info = reader.read_into()?;
        Ok(ValidPathInfo { path, info })
    }
}
