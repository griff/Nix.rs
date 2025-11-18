use proptest::prelude::*;

#[cfg(feature = "daemon")]
use crate::{daemon::ProtocolVersion, log::LogMessage};
use crate::{
    log::{
        Activity, ActivityResult, ActivityType, Field, Message, ResultType, StopActivity, Verbosity,
    },
    test::arbitrary::arb_byte_string,
};

impl Arbitrary for Verbosity {
    type Parameters = ();
    type Strategy = BoxedStrategy<Self>;

    fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
        prop_oneof![
            Just(Self::Error),
            Just(Self::Warn),
            Just(Self::Notice),
            Just(Self::Info),
            Just(Self::Talkative),
            Just(Self::Chatty),
            Just(Self::Debug),
            Just(Self::Vomit),
        ]
        .boxed()
    }
}

impl Arbitrary for ActivityType {
    type Parameters = ();
    type Strategy = BoxedStrategy<Self>;

    fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
        prop_oneof![
            Just(Self::Unknown),
            Just(Self::CopyPath),
            Just(Self::FileTransfer),
            Just(Self::Realise),
            Just(Self::CopyPaths),
            Just(Self::Builds),
            Just(Self::Build),
            Just(Self::OptimiseStore),
            Just(Self::VerifyPaths),
            Just(Self::Substitute),
            Just(Self::QueryPathInfo),
            Just(Self::PostBuildHook),
            Just(Self::BuildWaiting),
            Just(Self::FetchTree),
        ]
        .boxed()
    }
}

impl Arbitrary for ResultType {
    type Parameters = ();
    type Strategy = BoxedStrategy<Self>;

    fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
        prop_oneof![
            Just(Self::FileLinked),
            Just(Self::BuildLogLine),
            Just(Self::UntrustedPath),
            Just(Self::CorruptedPath),
            Just(Self::SetPhase),
            Just(Self::Progress),
            Just(Self::SetExpected),
            Just(Self::PostBuildLogLine),
            Just(Self::FetchStatus),
        ]
        .boxed()
    }
}

#[cfg(feature = "daemon")]
impl Arbitrary for LogMessage {
    type Parameters = ProtocolVersion;
    type Strategy = BoxedStrategy<Self>;

    fn arbitrary_with(args: Self::Parameters) -> Self::Strategy {
        use crate::log::{Activity, ActivityResult, Message, StopActivity};
        if args.minor() >= 20 {
            prop_oneof![
                any::<Message>().prop_map(LogMessage::Message),
                any::<Activity>().prop_map(LogMessage::StartActivity),
                any::<ActivityResult>().prop_map(LogMessage::Result),
                any::<StopActivity>().prop_map(LogMessage::StopActivity)
            ]
            .boxed()
        } else {
            any::<Message>().prop_map(LogMessage::Message).boxed()
        }
    }
}

impl Arbitrary for Message {
    type Parameters = ();
    type Strategy = BoxedStrategy<Self>;

    fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
        (any::<Verbosity>(), arb_byte_string())
            .prop_map(|(level, text)| Message { level, text })
            .boxed()
    }
}

impl Arbitrary for Activity {
    type Parameters = ();
    type Strategy = BoxedStrategy<Self>;

    fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
        (
            any::<Vec<Field>>(),
            any::<u64>(),
            any::<Verbosity>(),
            any::<u64>(),
            arb_byte_string(),
            any::<ActivityType>(),
        )
            .prop_map(
                |(fields, id, level, parent, text, activity_type)| Activity {
                    fields,
                    id,
                    level,
                    parent,
                    text,
                    activity_type,
                },
            )
            .boxed()
    }
}

impl Arbitrary for StopActivity {
    type Parameters = ();
    type Strategy = BoxedStrategy<Self>;

    fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
        any::<u64>().prop_map(|id| StopActivity { id }).boxed()
    }
}

impl Arbitrary for ActivityResult {
    type Parameters = ();
    type Strategy = BoxedStrategy<Self>;

    fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
        (any::<Vec<Field>>(), any::<u64>(), any::<ResultType>())
            .prop_map(|(fields, id, result_type)| ActivityResult {
                fields,
                id,
                result_type,
            })
            .boxed()
    }
}

impl Arbitrary for Field {
    type Parameters = ();
    type Strategy = BoxedStrategy<Self>;

    fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
        prop_oneof![
            any::<u64>().prop_map(Field::Int),
            arb_byte_string().prop_map(Field::String),
        ]
        .boxed()
    }
}
