use bytes::Bytes;
use capnp::Error;
use nixrs::daemon::wire::types2::{
    BuildMode, BuildResult, BuildStatus, KeyedBuildResult, QueryMissingResult, ValidPathInfo,
};
use nixrs::daemon::{
    Activity, ActivityResult, ActivityType, ClientOptions, Field, LogMessage, ResultType,
    UnkeyedValidPathInfo, Verbosity,
};

use crate::capnp::nix_daemon_capnp;
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

impl From<Verbosity> for nix_daemon_capnp::Verbosity {
    fn from(value: Verbosity) -> Self {
        match value {
            Verbosity::Error => nix_daemon_capnp::Verbosity::Error,
            Verbosity::Warn => nix_daemon_capnp::Verbosity::Warn,
            Verbosity::Notice => nix_daemon_capnp::Verbosity::Notice,
            Verbosity::Info => nix_daemon_capnp::Verbosity::Info,
            Verbosity::Talkative => nix_daemon_capnp::Verbosity::Talkative,
            Verbosity::Chatty => nix_daemon_capnp::Verbosity::Chatty,
            Verbosity::Debug => nix_daemon_capnp::Verbosity::Debug,
            Verbosity::Vomit => nix_daemon_capnp::Verbosity::Vomit,
        }
    }
}

impl From<nix_daemon_capnp::Verbosity> for Verbosity {
    fn from(value: nix_daemon_capnp::Verbosity) -> Self {
        match value {
            nix_daemon_capnp::Verbosity::Error => Verbosity::Error,
            nix_daemon_capnp::Verbosity::Warn => Verbosity::Warn,
            nix_daemon_capnp::Verbosity::Notice => Verbosity::Notice,
            nix_daemon_capnp::Verbosity::Info => Verbosity::Info,
            nix_daemon_capnp::Verbosity::Talkative => Verbosity::Talkative,
            nix_daemon_capnp::Verbosity::Chatty => Verbosity::Chatty,
            nix_daemon_capnp::Verbosity::Debug => Verbosity::Debug,
            nix_daemon_capnp::Verbosity::Vomit => Verbosity::Vomit,
        }
    }
}

impl From<ActivityType> for nix_daemon_capnp::ActivityType {
    fn from(value: ActivityType) -> Self {
        match value {
            ActivityType::Unknown => nix_daemon_capnp::ActivityType::Unknown,
            ActivityType::CopyPath => nix_daemon_capnp::ActivityType::CopyPath,
            ActivityType::FileTransfer => nix_daemon_capnp::ActivityType::FileTransfer,
            ActivityType::Realise => nix_daemon_capnp::ActivityType::Realise,
            ActivityType::CopyPaths => nix_daemon_capnp::ActivityType::CopyPath,
            ActivityType::Builds => nix_daemon_capnp::ActivityType::Builds,
            ActivityType::Build => nix_daemon_capnp::ActivityType::Build,
            ActivityType::OptimiseStore => nix_daemon_capnp::ActivityType::OptimiseStore,
            ActivityType::VerifyPaths => nix_daemon_capnp::ActivityType::VerifyPaths,
            ActivityType::Substitute => nix_daemon_capnp::ActivityType::Substitute,
            ActivityType::QueryPathInfo => nix_daemon_capnp::ActivityType::QueryPathInfo,
            ActivityType::PostBuildHook => nix_daemon_capnp::ActivityType::PostBuildHook,
            ActivityType::BuildWaiting => nix_daemon_capnp::ActivityType::BuildWaiting,
            ActivityType::FetchTree => nix_daemon_capnp::ActivityType::FetchTree,
        }
    }
}

impl From<nix_daemon_capnp::ActivityType> for ActivityType {
    fn from(value: nix_daemon_capnp::ActivityType) -> Self {
        match value {
            nix_daemon_capnp::ActivityType::Unknown => ActivityType::Unknown,
            nix_daemon_capnp::ActivityType::CopyPath => ActivityType::CopyPath,
            nix_daemon_capnp::ActivityType::FileTransfer => ActivityType::FileTransfer,
            nix_daemon_capnp::ActivityType::Realise => ActivityType::Realise,
            nix_daemon_capnp::ActivityType::CopyPaths => ActivityType::CopyPaths,
            nix_daemon_capnp::ActivityType::Builds => ActivityType::Builds,
            nix_daemon_capnp::ActivityType::Build => ActivityType::Build,
            nix_daemon_capnp::ActivityType::OptimiseStore => ActivityType::OptimiseStore,
            nix_daemon_capnp::ActivityType::VerifyPaths => ActivityType::VerifyPaths,
            nix_daemon_capnp::ActivityType::Substitute => ActivityType::Substitute,
            nix_daemon_capnp::ActivityType::QueryPathInfo => ActivityType::QueryPathInfo,
            nix_daemon_capnp::ActivityType::PostBuildHook => ActivityType::PostBuildHook,
            nix_daemon_capnp::ActivityType::BuildWaiting => ActivityType::BuildWaiting,
            nix_daemon_capnp::ActivityType::FetchTree => ActivityType::FetchTree,
        }
    }
}

impl From<ResultType> for nix_daemon_capnp::ResultType {
    fn from(value: ResultType) -> Self {
        match value {
            ResultType::FileLinked => nix_daemon_capnp::ResultType::FileLinked,
            ResultType::BuildLogLine => nix_daemon_capnp::ResultType::BuildLogLine,
            ResultType::UntrustedPath => nix_daemon_capnp::ResultType::UntrustedPath,
            ResultType::CorruptedPath => nix_daemon_capnp::ResultType::CorruptedPath,
            ResultType::SetPhase => nix_daemon_capnp::ResultType::SetPhase,
            ResultType::Progress => nix_daemon_capnp::ResultType::Progress,
            ResultType::SetExpected => nix_daemon_capnp::ResultType::SetExpected,
            ResultType::PostBuildLogLine => nix_daemon_capnp::ResultType::PostBuildLogLine,
            ResultType::FetchStatus => nix_daemon_capnp::ResultType::FetchStatus,
        }
    }
}

impl From<nix_daemon_capnp::ResultType> for ResultType {
    fn from(value: nix_daemon_capnp::ResultType) -> Self {
        match value {
            nix_daemon_capnp::ResultType::FileLinked => ResultType::FileLinked,
            nix_daemon_capnp::ResultType::BuildLogLine => ResultType::BuildLogLine,
            nix_daemon_capnp::ResultType::UntrustedPath => ResultType::UntrustedPath,
            nix_daemon_capnp::ResultType::CorruptedPath => ResultType::CorruptedPath,
            nix_daemon_capnp::ResultType::SetPhase => ResultType::SetPhase,
            nix_daemon_capnp::ResultType::Progress => ResultType::Progress,
            nix_daemon_capnp::ResultType::SetExpected => ResultType::SetExpected,
            nix_daemon_capnp::ResultType::PostBuildLogLine => ResultType::PostBuildLogLine,
            nix_daemon_capnp::ResultType::FetchStatus => ResultType::FetchStatus,
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
        self.reborrow().init_path().build_from(&input.path)?;
        self.reborrow().init_result().build_from(&input.result)?;
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
            self.reborrow().set_deriver(deriver)?;
        }
        self.set_nar_hash(input.nar_hash.as_ref());
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
            self.reborrow().init_ca().build_from(ca)?;
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
        self.reborrow().set_path(&input.path)?;
        self.reborrow().init_info().build_from(&input.info)?;
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

impl<'b> BuildFrom<LogMessage> for nix_daemon_capnp::log_message::Builder<'b> {
    fn build_from(&mut self, input: &LogMessage) -> Result<(), Error> {
        match input {
            LogMessage::Next(msg) => {
                self.set_next(msg.as_ref());
            }
            LogMessage::StartActivity(activity) => {
                self.reborrow().init_start_activity().build_from(activity)?;
            }
            LogMessage::StopActivity(act) => {
                self.reborrow().init_stop_activity().set_act(*act);
            }
            LogMessage::Result(result) => {
                self.reborrow().init_result().build_from(result)?;
            }
        }
        Ok(())
    }
}

impl<'b> BuildFrom<Activity> for nix_daemon_capnp::log_message::start_activity::Builder<'b> {
    fn build_from(&mut self, input: &Activity) -> Result<(), Error> {
        self.set_act(input.act);
        self.set_activity_type(input.activity_type.into());
        self.set_level(input.level.into());
        self.set_text(input.text.as_ref());
        self.set_parent(input.parent);
        self.reborrow()
            .init_fields(input.fields.len() as u32)
            .build_from(&input.fields)?;
        Ok(())
    }
}

impl<'b> BuildFrom<ActivityResult> for nix_daemon_capnp::log_message::result::Builder<'b> {
    fn build_from(&mut self, input: &ActivityResult) -> Result<(), Error> {
        self.set_act(input.act);
        self.set_result_type(input.result_type.into());
        self.reborrow()
            .init_fields(input.fields.len() as u32)
            .build_from(&input.fields)?;
        Ok(())
    }
}

impl<'b> BuildFrom<Field> for nix_daemon_capnp::field::Builder<'b> {
    fn build_from(&mut self, input: &Field) -> Result<(), Error> {
        match input {
            Field::Int(value) => self.set_int(*value),
            Field::String(value) => self.set_string(value.as_ref()),
        }
        Ok(())
    }
}
