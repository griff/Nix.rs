use capnp::{
    traits::{FromPointerBuilder as _, SetterInput},
    Error,
};
use nixrs::{
    log::{
        Activity, ActivityResult, ActivityType, Field, LogMessage, Message, ResultType,
        StopActivity, Verbosity,
    },
    ByteString,
};

use crate::{
    capnp::nix_daemon_capnp,
    convert::{BuildFrom, ReadFrom, ReadInto as _},
};

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
            ActivityType::CopyPaths => nix_daemon_capnp::ActivityType::CopyPaths,
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

impl<'b> BuildFrom<LogMessage> for nix_daemon_capnp::log_message::Builder<'b> {
    fn build_from(&mut self, input: &LogMessage) -> Result<(), Error> {
        match input {
            LogMessage::Message(msg) => {
                self.reborrow().init_message().build_from(msg)?;
            }
            LogMessage::StartActivity(activity) => {
                self.reborrow().init_start_activity().build_from(activity)?;
            }
            LogMessage::StopActivity(act) => {
                self.reborrow().init_stop_activity().set_id(act.id);
            }
            LogMessage::Result(result) => {
                self.reborrow().init_result().build_from(result)?;
            }
        }
        Ok(())
    }
}

impl SetterInput<nix_daemon_capnp::log_message::Owned> for &'_ LogMessage {
    fn set_pointer_builder(
        builder: capnp::private::layout::PointerBuilder<'_>,
        input: Self,
        _canonicalize: bool,
    ) -> capnp::Result<()> {
        let builder = nix_daemon_capnp::log_message::Builder::init_pointer(builder, 0);
        match input {
            LogMessage::Message(msg) => {
                builder.init_message().build_from(msg)?;
            }
            LogMessage::StartActivity(activity) => {
                builder.init_start_activity().build_from(activity)?;
            }
            LogMessage::StopActivity(act) => {
                builder.init_stop_activity().set_id(act.id);
            }
            LogMessage::Result(result) => {
                builder.init_result().build_from(result)?;
            }
        }
        Ok(())
    }
}

impl<'r> ReadFrom<nix_daemon_capnp::log_message::Reader<'r>> for LogMessage {
    fn read_from(reader: nix_daemon_capnp::log_message::Reader<'r>) -> Result<Self, Error> {
        match reader.which()? {
            nix_daemon_capnp::log_message::Which::Message(msg) => {
                Ok(LogMessage::Message(msg.read_into()?))
            }
            nix_daemon_capnp::log_message::Which::StartActivity(act) => {
                Ok(LogMessage::StartActivity(act.read_into()?))
            }
            nix_daemon_capnp::log_message::Which::StopActivity(act) => {
                Ok(LogMessage::StopActivity(StopActivity { id: act.get_id() }))
            }
            nix_daemon_capnp::log_message::Which::Result(res) => {
                Ok(LogMessage::Result(res.read_into()?))
            }
        }
    }
}

impl<'b> BuildFrom<Message> for nix_daemon_capnp::log_message::message::Builder<'b> {
    fn build_from(&mut self, input: &Message) -> Result<(), Error> {
        self.set_level(input.level.into());
        self.set_text(input.text.as_ref());
        Ok(())
    }
}

impl SetterInput<nix_daemon_capnp::log_message::message::Owned> for &'_ Message {
    fn set_pointer_builder(
        builder: capnp::private::layout::PointerBuilder<'_>,
        input: Self,
        _canonicalize: bool,
    ) -> capnp::Result<()> {
        let mut builder = nix_daemon_capnp::log_message::message::Builder::init_pointer(builder, 0);
        builder.set_level(input.level.into());
        builder.set_text(input.text.as_ref());
        Ok(())
    }
}

impl<'r> ReadFrom<nix_daemon_capnp::log_message::message::Reader<'r>> for Message {
    fn read_from(
        reader: nix_daemon_capnp::log_message::message::Reader<'r>,
    ) -> Result<Self, Error> {
        let level = reader.get_level()?.into();
        let text = ByteString::copy_from_slice(reader.get_text()?);
        Ok(Message { level, text })
    }
}

impl<'b> BuildFrom<Activity> for nix_daemon_capnp::log_message::start_activity::Builder<'b> {
    fn build_from(&mut self, input: &Activity) -> Result<(), Error> {
        self.set_id(input.id);
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

impl SetterInput<nix_daemon_capnp::log_message::start_activity::Owned> for &'_ Activity {
    fn set_pointer_builder(
        builder: capnp::private::layout::PointerBuilder<'_>,
        input: Self,
        _canonicalize: bool,
    ) -> capnp::Result<()> {
        let mut builder =
            nix_daemon_capnp::log_message::start_activity::Builder::init_pointer(builder, 0);
        builder.set_id(input.id);
        builder.set_activity_type(input.activity_type.into());
        builder.set_level(input.level.into());
        builder.set_text(input.text.as_ref());
        builder.set_parent(input.parent);
        builder
            .init_fields(input.fields.len() as u32)
            .build_from(&input.fields)?;
        Ok(())
    }
}

impl<'r> ReadFrom<nix_daemon_capnp::log_message::start_activity::Reader<'r>> for Activity {
    fn read_from(
        reader: nix_daemon_capnp::log_message::start_activity::Reader<'r>,
    ) -> Result<Self, Error> {
        let id = reader.get_id();
        let activity_type = reader.get_activity_type()?.into();
        let level = reader.get_level()?.into();
        let text = ByteString::copy_from_slice(reader.get_text()?);
        let fields = reader.get_fields()?.read_into()?;
        let parent = reader.get_parent();
        Ok(Activity {
            id,
            activity_type,
            level,
            text,
            fields,
            parent,
        })
    }
}

impl<'b> BuildFrom<ActivityResult> for nix_daemon_capnp::log_message::result::Builder<'b> {
    fn build_from(&mut self, input: &ActivityResult) -> Result<(), Error> {
        self.set_id(input.id);
        self.set_result_type(input.result_type.into());
        self.reborrow()
            .init_fields(input.fields.len() as u32)
            .build_from(&input.fields)?;
        Ok(())
    }
}

impl SetterInput<nix_daemon_capnp::log_message::result::Owned> for &'_ ActivityResult {
    fn set_pointer_builder(
        builder: capnp::private::layout::PointerBuilder<'_>,
        input: Self,
        _canonicalize: bool,
    ) -> capnp::Result<()> {
        let mut builder = nix_daemon_capnp::log_message::result::Builder::init_pointer(builder, 0);
        builder.set_id(input.id);
        builder.set_result_type(input.result_type.into());
        builder
            .init_fields(input.fields.len() as u32)
            .build_from(&input.fields)?;
        Ok(())
    }
}

impl<'r> ReadFrom<nix_daemon_capnp::log_message::result::Reader<'r>> for ActivityResult {
    fn read_from(reader: nix_daemon_capnp::log_message::result::Reader<'r>) -> Result<Self, Error> {
        let id = reader.get_id();
        let result_type = reader.get_result_type()?.into();
        let fields = reader.get_fields()?.read_into()?;
        Ok(ActivityResult {
            id,
            result_type,
            fields,
        })
    }
}

impl SetterInput<nix_daemon_capnp::log_message::stop_activity::Owned> for &'_ StopActivity {
    fn set_pointer_builder(
        builder: capnp::private::layout::PointerBuilder<'_>,
        input: Self,
        _canonicalize: bool,
    ) -> capnp::Result<()> {
        let mut builder =
            nix_daemon_capnp::log_message::stop_activity::Builder::init_pointer(builder, 0);
        builder.set_id(input.id);
        Ok(())
    }
}

impl<'r> ReadFrom<nix_daemon_capnp::log_message::stop_activity::Reader<'r>> for StopActivity {
    fn read_from(
        reader: nix_daemon_capnp::log_message::stop_activity::Reader<'r>,
    ) -> Result<Self, Error> {
        let id = reader.get_id();
        Ok(StopActivity { id })
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

impl SetterInput<nix_daemon_capnp::field::Owned> for &'_ Field {
    fn set_pointer_builder(
        builder: capnp::private::layout::PointerBuilder<'_>,
        input: Self,
        _canonicalize: bool,
    ) -> capnp::Result<()> {
        let mut builder = nix_daemon_capnp::field::Builder::init_pointer(builder, 0);
        match input {
            Field::Int(value) => builder.set_int(*value),
            Field::String(value) => builder.set_string(value.as_ref()),
        }
        Ok(())
    }
}

impl<'r> ReadFrom<nix_daemon_capnp::field::Reader<'r>> for Field {
    fn read_from(reader: nix_daemon_capnp::field::Reader<'r>) -> Result<Self, Error> {
        match reader.which()? {
            nix_daemon_capnp::field::Which::Int(value) => Ok(Field::Int(value)),
            nix_daemon_capnp::field::Which::String(value) => {
                Ok(Field::String(ByteString::copy_from_slice(value?)))
            }
        }
    }
}
