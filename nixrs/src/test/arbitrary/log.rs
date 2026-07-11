use std::num::NonZeroU64;

use proptest::prelude::*;

#[cfg(feature = "daemon")]
use crate::daemon::ProtocolVersion;
use crate::log::ParsedLogMessage;
use crate::log::{
    LogMessage, ParsedActivity, ParsedActivityResult, ParsedActivityType, ParsedResultType,
    StoreUri,
};
use crate::test::arbitrary::arb_http_uri;
use crate::{
    log::{
        Activity, ActivityId, ActivityResult, ActivityType, Field, Message, ResultType,
        StopActivity, Verbosity,
    },
    test::arbitrary::arb_byte_string,
};

impl Arbitrary for ActivityId {
    type Parameters = ();
    type Strategy = BoxedStrategy<Self>;

    fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
        any::<NonZeroU64>().prop_map(From::from).boxed()
    }
}

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

impl Arbitrary for StoreUri {
    type Parameters = ();
    type Strategy = BoxedStrategy<Self>;

    fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
        prop_oneof![
            Just(Self::Local),
            Just(Self::Daemon),
            arb_http_uri().prop_map(Self::Uri),
        ]
        .boxed()
    }
}

#[expect(clippy::large_enum_variant)]
#[derive(Debug, Clone)]
enum ActivityTree {
    ActivityResult(ParsedActivityResult),
    Message(Message),
    Activity {
        activity: ParsedActivity,
        children: Vec<ActivityTree>,
        closed: bool,
    },
}

impl ActivityTree {
    fn events(
        self,
        parent: Option<ActivityId>,
        ignore_closed: bool,
        out: &mut Vec<ParsedLogMessage>,
    ) {
        match self {
            ActivityTree::Message(msg) => {
                out.push(ParsedLogMessage::Message(msg));
            }
            ActivityTree::ActivityResult(mut result) => {
                if let Some(parent) = parent {
                    result.id = parent;
                    out.push(ParsedLogMessage::Result(result));
                }
            }
            ActivityTree::Activity {
                mut activity,
                children,
                closed,
            } => {
                let id = activity.id;
                activity.parent = parent;
                out.push(ParsedLogMessage::StartActivity(activity));
                for child in children {
                    child.events(Some(id), ignore_closed, out);
                }
                if ignore_closed || closed {
                    out.push(ParsedLogMessage::StopActivity(StopActivity { id }));
                }
            }
        }
    }

    fn into_parsed_events(self) -> Vec<ParsedLogMessage> {
        let mut out = Vec::new();
        self.events(None, false, &mut out);
        out
    }

    fn into_consistent_parsed_events(self) -> Vec<ParsedLogMessage> {
        let mut out = Vec::new();
        self.events(None, true, &mut out);
        out
    }

    fn into_events(self) -> Vec<LogMessage> {
        self.into_parsed_events()
            .into_iter()
            .map(From::from)
            .collect()
    }

    fn into_consistent_events(self) -> Vec<LogMessage> {
        self.into_consistent_parsed_events()
            .into_iter()
            .map(From::from)
            .collect()
    }
}

fn log_activities(
    depth: u32,
    desired_size: u32,
    expected_branch_size: u32,
    inconsistent: bool,
) -> impl Strategy<Value = ActivityTree> {
    let leaf = prop_oneof![
        any::<Message>().prop_map(ActivityTree::Message),
        any::<ParsedActivityResult>().prop_map(ActivityTree::ActivityResult),
    ];
    leaf.prop_recursive(depth, desired_size, expected_branch_size, move |inner| {
        let closed = if inconsistent {
            any::<bool>().boxed()
        } else {
            Just(true).boxed()
        };
        (
            any::<ParsedActivity>(),
            prop::collection::vec(inner, 0..expected_branch_size as usize),
            closed,
        )
            .prop_map(|(activity, children, closed)| ActivityTree::Activity {
                activity,
                children,
                closed,
            })
    })
}

pub fn inconsistent_log_messages(
    depth: u32,
    desired_size: u32,
    expected_branch_size: u32,
) -> impl Strategy<Value = (Vec<LogMessage>, Vec<LogMessage>)> {
    log_activities(depth, desired_size, expected_branch_size, true)
        .prop_map(|tree| (tree.clone().into_events(), tree.into_consistent_events()))
}
pub fn log_messages(
    depth: u32,
    desired_size: u32,
    expected_branch_size: u32,
) -> impl Strategy<Value = Vec<LogMessage>> {
    log_activities(depth, desired_size, expected_branch_size, false)
        .prop_map(|tree| tree.into_events())
}

#[cfg(feature = "daemon")]
pub fn inconsistent_protocol_log_messages(
    version: ProtocolVersion,
    depth: u32,
    desired_size: u32,
    expected_branch_size: u32,
) -> impl Strategy<Value = (Vec<LogMessage>, Vec<LogMessage>)> {
    if version.minor() >= 20 {
        inconsistent_log_messages(depth, desired_size, expected_branch_size).boxed()
    } else {
        let element = any::<Message>().prop_map(LogMessage::Message);
        prop::collection::vec(element, 0..desired_size as usize)
            .prop_map(|log| (log.clone(), log))
            .boxed()
    }
}

#[cfg(feature = "daemon")]
pub fn protocol_log_messages(
    version: ProtocolVersion,
    depth: u32,
    desired_size: u32,
    expected_branch_size: u32,
) -> impl Strategy<Value = Vec<LogMessage>> {
    if version.minor() >= 20 {
        log_messages(depth, desired_size, expected_branch_size).boxed()
    } else {
        let element = any::<Message>().prop_map(LogMessage::Message);
        prop::collection::vec(element, 0..desired_size as usize).boxed()
    }
}

pub fn inconsistent_parsed_log_messages(
    depth: u32,
    desired_size: u32,
    expected_branch_size: u32,
) -> impl Strategy<Value = (Vec<ParsedLogMessage>, Vec<ParsedLogMessage>)> {
    log_activities(depth, desired_size, expected_branch_size, true).prop_map(|tree| {
        (
            tree.clone().into_parsed_events(),
            tree.into_consistent_parsed_events(),
        )
    })
}
pub fn parsed_log_messages(
    depth: u32,
    desired_size: u32,
    expected_branch_size: u32,
) -> impl Strategy<Value = Vec<ParsedLogMessage>> {
    log_activities(depth, desired_size, expected_branch_size, false)
        .prop_map(|tree| tree.into_parsed_events())
}

#[cfg(feature = "daemon")]
pub fn inconsistent_protocol_parsed_log_messages(
    version: ProtocolVersion,
    depth: u32,
    desired_size: u32,
    expected_branch_size: u32,
) -> impl Strategy<Value = (Vec<ParsedLogMessage>, Vec<ParsedLogMessage>)> {
    if version.minor() >= 20 {
        inconsistent_parsed_log_messages(depth, desired_size, expected_branch_size).boxed()
    } else {
        let element = any::<Message>().prop_map(ParsedLogMessage::Message);
        prop::collection::vec(element, 0..desired_size as usize)
            .prop_map(|log| (log.clone(), log))
            .boxed()
    }
}

#[cfg(feature = "daemon")]
pub fn protocol_parsed_log_messages(
    version: ProtocolVersion,
    depth: u32,
    desired_size: u32,
    expected_branch_size: u32,
) -> impl Strategy<Value = Vec<ParsedLogMessage>> {
    if version.minor() >= 20 {
        parsed_log_messages(depth, desired_size, expected_branch_size).boxed()
    } else {
        let element = any::<Message>().prop_map(ParsedLogMessage::Message);
        prop::collection::vec(element, 0..desired_size as usize).boxed()
    }
}

#[cfg(feature = "daemon")]
impl Arbitrary for ParsedLogMessage {
    type Parameters = ProtocolVersion;
    type Strategy = BoxedStrategy<Self>;

    fn arbitrary_with(args: Self::Parameters) -> Self::Strategy {
        if args.minor() >= 20 {
            prop_oneof![
                any::<Message>().prop_map(Self::Message),
                any::<ParsedActivity>().prop_map(Self::StartActivity),
                any::<ParsedActivityResult>().prop_map(Self::Result),
                any::<StopActivity>().prop_map(Self::StopActivity)
            ]
            .boxed()
        } else {
            any::<Message>().prop_map(Self::Message).boxed()
        }
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
            any::<ActivityId>().no_shrink(),
            any::<Verbosity>(),
            arb_byte_string(),
            any::<ActivityType>(),
        )
            .prop_map(|(fields, id, level, text, activity_type)| Activity {
                fields,
                id,
                level,
                parent: None,
                text,
                activity_type,
            })
            .boxed()
    }
}

impl Arbitrary for ParsedActivity {
    type Parameters = ();
    type Strategy = BoxedStrategy<Self>;

    fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
        (
            any::<ActivityId>().no_shrink(),
            any::<Verbosity>(),
            arb_byte_string(),
            any::<ParsedActivityType>(),
        )
            .prop_map(|(id, level, text, activity_type)| ParsedActivity {
                id,
                level,
                parent: None,
                text,
                activity_type,
            })
            .boxed()
    }
}

pub mod parsed_activity_type {
    use proptest::{
        prelude::{Arbitrary, BoxedStrategy, Just, Strategy, any},
        prop_oneof,
    };

    use crate::log::{ActivityType, Field, ParsedActivityType, StoreUri};
    use crate::store_path::FullStorePath;
    use crate::test::arbitrary::arb_http_uri;
    use crate::test::arbitrary::store_path::arb_full_drv_store_path;

    pub fn unknown() -> impl Strategy<Value = ParsedActivityType> {
        Just(ParsedActivityType::Unknown)
    }

    pub fn copy_path() -> impl Strategy<Value = ParsedActivityType> {
        (any::<FullStorePath>(), any::<StoreUri>(), any::<StoreUri>()).prop_map(
            |(store_path, source_uri, dest_uri)| ParsedActivityType::CopyPath {
                store_path,
                source_uri,
                dest_uri,
            },
        )
    }

    pub fn file_transfer() -> impl Strategy<Value = ParsedActivityType> {
        arb_http_uri().prop_map(|request_uri| ParsedActivityType::FileTransfer { request_uri })
    }

    pub fn realise() -> impl Strategy<Value = ParsedActivityType> {
        Just(ParsedActivityType::Realise)
    }

    pub fn copy_paths() -> impl Strategy<Value = ParsedActivityType> {
        Just(ParsedActivityType::CopyPaths)
    }

    pub fn builds() -> impl Strategy<Value = ParsedActivityType> {
        Just(ParsedActivityType::Builds)
    }

    pub fn build() -> impl Strategy<Value = ParsedActivityType> {
        (
            arb_full_drv_store_path(),
            any::<String>(),
            any::<u64>(),
            any::<u64>(),
        )
            .prop_map(|(drv_path, remote_machine, current_round, total_rounds)| {
                ParsedActivityType::Build {
                    drv_path,
                    remote_machine: (!remote_machine.is_empty()).then_some(remote_machine),
                    current_round,
                    total_rounds,
                }
            })
    }

    pub fn optimise_store() -> impl Strategy<Value = ParsedActivityType> {
        Just(ParsedActivityType::OptimiseStore)
    }

    pub fn verify_paths() -> impl Strategy<Value = ParsedActivityType> {
        Just(ParsedActivityType::VerifyPaths)
    }

    pub fn substitute() -> impl Strategy<Value = ParsedActivityType> {
        (any::<FullStorePath>(), any::<StoreUri>()).prop_map(|(store_path, store_uri)| {
            ParsedActivityType::Substitute {
                store_path,
                store_uri,
            }
        })
    }

    pub fn query_path_info() -> impl Strategy<Value = ParsedActivityType> {
        (any::<FullStorePath>(), any::<StoreUri>()).prop_map(|(store_path, store_uri)| {
            ParsedActivityType::QueryPathInfo {
                store_path,
                store_uri,
            }
        })
    }

    pub fn post_build_hook() -> impl Strategy<Value = ParsedActivityType> {
        arb_full_drv_store_path()
            .prop_map(|drv_path| ParsedActivityType::PostBuildHook { drv_path })
    }

    pub fn build_waiting() -> impl Strategy<Value = ParsedActivityType> {
        Just(ParsedActivityType::BuildWaiting)
    }

    pub fn build_waiting_ca_resolved() -> impl Strategy<Value = ParsedActivityType> {
        (arb_full_drv_store_path(), any::<FullStorePath>()).prop_map(|(drv_path, path_resolved)| {
            ParsedActivityType::BuildWaitingCAResolved {
                drv_path,
                path_resolved,
            }
        })
    }

    pub fn fetch_tree() -> impl Strategy<Value = ParsedActivityType> {
        Just(ParsedActivityType::FetchTree)
    }

    pub fn unparsable() -> impl Strategy<Value = ParsedActivityType> {
        (any::<Vec<Field>>(), any::<ActivityType>()).prop_filter_map(
            "parsable",
            |(fields, activity_type)| {
                let ret = ParsedActivityType::parse(activity_type, fields);
                (!matches!(
                    ret,
                    ParsedActivityType::Unparsable {
                        fields: _,
                        activity_type: _
                    }
                ))
                .then_some(ret)
            },
        )
    }

    impl Arbitrary for ParsedActivityType {
        type Parameters = ();
        type Strategy = BoxedStrategy<Self>;

        fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
            prop_oneof![
                100 => unknown(),
                100 => copy_path(),
                100 => file_transfer(),
                100 => realise(),
                100 => copy_paths(),
                100 => builds(),
                100 => build(),
                100 => optimise_store(),
                100 => verify_paths(),
                100 => substitute(),
                100 => query_path_info(),
                100 => post_build_hook(),
                100 => build_waiting(),
                100 => fetch_tree(),
                1 => unparsable(),
            ]
            .boxed()
        }
    }
}

impl Arbitrary for StopActivity {
    type Parameters = ();
    type Strategy = BoxedStrategy<Self>;

    fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
        any::<ActivityId>()
            .no_shrink()
            .prop_map(|id| StopActivity { id })
            .boxed()
    }
}

pub mod parsed_result_type {
    use proptest::prelude::{Arbitrary, BoxedStrategy, Strategy, any};
    use proptest::prop_oneof;

    use crate::log::{ActivityType, Field, ParsedResultType, ResultType};
    use crate::store_path::FullStorePath;
    use crate::test::arbitrary::arb_byte_string;

    pub fn file_linked() -> impl Strategy<Value = ParsedResultType> {
        (any::<u64>(), any::<Option<u64>>())
            .prop_map(|(size, blocks)| ParsedResultType::FileLinked { size, blocks })
    }

    pub fn build_log_line() -> impl Strategy<Value = ParsedResultType> {
        arb_byte_string().prop_map(ParsedResultType::BuildLogLine)
    }

    pub fn untrusted_path() -> impl Strategy<Value = ParsedResultType> {
        any::<FullStorePath>().prop_map(ParsedResultType::UntrustedPath)
    }

    pub fn corrupted_path() -> impl Strategy<Value = ParsedResultType> {
        any::<FullStorePath>().prop_map(ParsedResultType::CorruptedPath)
    }

    pub fn set_phase() -> impl Strategy<Value = ParsedResultType> {
        arb_byte_string().prop_map(ParsedResultType::SetPhase)
    }

    pub fn progress() -> impl Strategy<Value = ParsedResultType> {
        (any::<u64>(), any::<u64>(), any::<u64>(), any::<u64>()).prop_map(
            |(done, expected, running, failed)| ParsedResultType::Progress {
                done,
                expected,
                running,
                failed,
            },
        )
    }
    pub fn set_expected_result() -> impl Strategy<Value = ParsedResultType> {
        (any::<ActivityType>(), any::<u64>()).prop_map(|(activity_type, expected)| {
            ParsedResultType::SetExpected {
                activity_type,
                expected,
            }
        })
    }

    pub fn post_build_log_line() -> impl Strategy<Value = ParsedResultType> {
        arb_byte_string().prop_map(ParsedResultType::PostBuildLogLine)
    }

    pub fn fetch_status() -> impl Strategy<Value = ParsedResultType> {
        arb_byte_string().prop_map(ParsedResultType::FetchStatus)
    }

    pub fn unparsable() -> impl Strategy<Value = ParsedResultType> {
        (any::<Vec<Field>>(), any::<ResultType>()).prop_filter_map(
            "parsable",
            |(fields, result_type)| {
                let ret = ParsedResultType::parse(result_type, fields);
                (!matches!(
                    ret,
                    ParsedResultType::Unparsable {
                        fields: _,
                        result_type: _
                    }
                ))
                .then_some(ret)
            },
        )
    }

    impl Arbitrary for ParsedResultType {
        type Parameters = ();
        type Strategy = BoxedStrategy<Self>;

        fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
            prop_oneof![
                100 => file_linked(),
                100 => build_log_line(),
                100 => untrusted_path(),
                100 => corrupted_path(),
                100 => set_phase(),
                100 => progress(),
                100 => set_expected_result(),
                100 => post_build_log_line(),
                100 => fetch_status(),
                1 => unparsable()
            ]
            .boxed()
        }
    }
}

impl Arbitrary for ParsedActivityResult {
    type Parameters = ();
    type Strategy = BoxedStrategy<Self>;

    fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
        (any::<ActivityId>().no_shrink(), any::<ParsedResultType>())
            .prop_map(|(id, result_type)| ParsedActivityResult { id, result_type })
            .boxed()
    }
}

impl Arbitrary for ActivityResult {
    type Parameters = ();
    type Strategy = BoxedStrategy<Self>;

    fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
        (
            any::<Vec<Field>>(),
            any::<ActivityId>().no_shrink(),
            any::<ResultType>(),
        )
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
