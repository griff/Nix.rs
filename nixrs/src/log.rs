use std::fmt;
use std::num::{NonZeroU64, TryFromIntError};
use std::str::FromStr;

use bstr::ByteSlice;
use http::uri::InvalidUri;
use num_enum::{FromPrimitive, IntoPrimitive, TryFromPrimitive};
use serde::{Deserialize, Serialize};

use crate::ByteString;
use crate::store_path::FullStorePath;

#[derive(
    Debug,
    derive_more::Display,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
)]
#[repr(transparent)]
pub struct ActivityId(NonZeroU64);

impl ActivityId {
    /// Constructs an `ActivityId` from the given `u64`
    ///
    /// # Panics
    /// - If the provided `u64` is 0.
    pub fn from_u64(u: u64) -> Self {
        ActivityId(NonZeroU64::new(u).expect("ActivityId must be > 0"))
    }

    pub fn as_u64(&self) -> u64 {
        self.0.into()
    }
}

impl From<NonZeroU64> for ActivityId {
    fn from(value: NonZeroU64) -> Self {
        ActivityId(value)
    }
}
impl TryFrom<u64> for ActivityId {
    type Error = TryFromIntError;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        Ok(ActivityId(value.try_into()?))
    }
}

impl From<ActivityId> for u64 {
    fn from(value: ActivityId) -> Self {
        value.as_u64()
    }
}

mod optional_activity_id {
    use serde::{Deserializer, Serialize};

    pub(crate) fn serialize<S>(
        value: &Option<super::ActivityId>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match value {
            Some(s) => s.as_u64().serialize(serializer),
            None => 0u64.serialize(serializer),
        }
    }

    pub(crate) fn deserialize<'de, D>(
        deserializer: D,
    ) -> Result<Option<super::ActivityId>, D::Error>
    where
        D: Deserializer<'de>,
    {
        use serde::Deserialize;
        let value = u64::deserialize(deserializer)?;
        Ok(value.try_into().ok())
    }
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    FromPrimitive,
    IntoPrimitive,
    Default,
    Serialize,
    Deserialize,
)]
#[serde(try_from = "u16", into = "u16")]
#[repr(u16)]
pub enum Verbosity {
    #[default]
    Error = 0,
    Warn = 1,
    Notice = 2,
    Info = 3,
    Talkative = 4,
    Chatty = 5,
    Debug = 6,
    #[catch_all]
    Vomit = 7,
}
impl Verbosity {
    pub fn as_u16(&self) -> u16 {
        (*self).into()
    }
    pub fn as_str(&self) -> &'static str {
        use Verbosity::*;
        match self {
            Error => "ERROR",
            Warn => "WARN",
            Notice => "NOTICE",
            Info => "INFO",
            Talkative => "TALKATIVE",
            Chatty => "CHATTY",
            Debug => "DEBUG",
            Vomit => "VOMIT",
        }
    }
}

impl fmt::Display for Verbosity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    TryFromPrimitive,
    IntoPrimitive,
    Serialize,
    Deserialize,
)]
#[serde(try_from = "u16", into = "u16")]
#[repr(u16)]
pub enum ActivityType {
    Unknown = 0,
    CopyPath = 100,
    FileTransfer = 101,
    Realise = 102,
    CopyPaths = 103,
    Builds = 104,
    Build = 105,
    OptimiseStore = 106,
    VerifyPaths = 107,
    Substitute = 108,
    QueryPathInfo = 109,
    PostBuildHook = 110,
    BuildWaiting = 111,
    FetchTree = 112,
}
impl ActivityType {
    pub fn as_u16(&self) -> u16 {
        (*self).into()
    }
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    TryFromPrimitive,
    IntoPrimitive,
    Serialize,
    Deserialize,
)]
#[serde(try_from = "u16", into = "u16")]
#[repr(u16)]
pub enum ResultType {
    FileLinked = 100,
    BuildLogLine = 101,
    UntrustedPath = 102,
    CorruptedPath = 103,
    SetPhase = 104,
    Progress = 105,
    SetExpected = 106,
    PostBuildLogLine = 107,
    FetchStatus = 108,
}
impl ResultType {
    pub fn as_u16(&self) -> u16 {
        (*self).into()
    }
}

#[expect(clippy::large_enum_variant)]
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "action")]
pub enum ParsedLogMessage {
    #[serde(rename = "msg")]
    Message(Message),
    #[serde(rename = "start")]
    StartActivity(ParsedActivity),
    #[serde(rename = "stop")]
    StopActivity(StopActivity),
    #[serde(rename = "result")]
    Result(ParsedActivityResult),
}

impl ParsedLogMessage {
    pub fn message<T: Into<ByteString>>(text: T) -> Self {
        Self::Message(Message {
            level: Verbosity::Error,
            text: text.into(),
        })
    }
}

impl From<LogMessage> for ParsedLogMessage {
    fn from(value: LogMessage) -> Self {
        match value {
            LogMessage::Message(message) => ParsedLogMessage::Message(message),
            LogMessage::StartActivity(activity) => ParsedLogMessage::StartActivity(activity.into()),
            LogMessage::StopActivity(activity) => ParsedLogMessage::StopActivity(activity),
            LogMessage::Result(result) => ParsedLogMessage::Result(result.into()),
        }
    }
}

impl From<ParsedLogMessage> for LogMessage {
    fn from(value: ParsedLogMessage) -> Self {
        match value {
            ParsedLogMessage::Message(message) => LogMessage::Message(message),
            ParsedLogMessage::StartActivity(activity) => LogMessage::StartActivity(activity.into()),
            ParsedLogMessage::StopActivity(activity) => LogMessage::StopActivity(activity),
            ParsedLogMessage::Result(result) => LogMessage::Result(result.into()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "action")]
pub enum LogMessage {
    #[serde(rename = "msg")]
    Message(Message),
    #[serde(rename = "start")]
    StartActivity(Activity),
    #[serde(rename = "stop")]
    StopActivity(StopActivity),
    #[serde(rename = "result")]
    Result(ActivityResult),
}

impl LogMessage {
    pub fn message<T: Into<ByteString>>(text: T) -> Self {
        Self::Message(Message {
            level: Verbosity::Error,
            text: text.into(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Message {
    pub level: Verbosity,
    #[serde(rename = "msg", serialize_with = "crate::serialize_byte_string")]
    pub text: ByteString,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum StoreUri {
    Local,
    Daemon,
    Uri(http::Uri),
}

impl FromStr for StoreUri {
    type Err = InvalidUri;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s == "local" {
            Ok(Self::Local)
        } else if s == "daemon" {
            Ok(Self::Daemon)
        } else {
            s.parse::<http::Uri>().map(Self::Uri)
        }
    }
}

impl fmt::Display for StoreUri {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StoreUri::Local => write!(f, "local"),
            StoreUri::Daemon => write!(f, "daemon"),
            StoreUri::Uri(uri) => write!(f, "{uri}"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(from = "Activity", into = "Activity")]
pub struct ParsedActivity {
    pub id: ActivityId,
    pub level: Verbosity,
    pub parent: Option<ActivityId>,
    pub text: ByteString, // If logger is JSON, invalid UTF-8 is replaced with U+FFFD
    pub activity_type: ParsedActivityType,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ParsedActivityType {
    Unknown,
    CopyPath {
        store_path: FullStorePath,
        source_uri: StoreUri,
        dest_uri: StoreUri,
    },
    FileTransfer {
        request_uri: http::Uri,
    },
    Realise,
    CopyPaths,
    Builds,
    Build {
        drv_path: FullStorePath,
        remote_machine: Option<String>,
        current_round: u64,
        total_rounds: u64,
    },
    OptimiseStore,
    VerifyPaths,
    Substitute {
        store_path: FullStorePath,
        store_uri: StoreUri,
    },
    QueryPathInfo {
        store_path: FullStorePath,
        store_uri: StoreUri,
    },
    PostBuildHook {
        drv_path: FullStorePath,
    },
    BuildWaiting,
    BuildWaitingCAResolved {
        drv_path: FullStorePath,
        path_resolved: FullStorePath,
    },
    FetchTree,
    Unparsable {
        fields: Vec<Field>,
        activity_type: ActivityType,
    },
}

impl ParsedActivityType {
    fn try_parse(activity_type: ActivityType, fields: &[Field]) -> Option<Self> {
        match (activity_type, fields.len()) {
            (ActivityType::Unknown, 0) => Some(ParsedActivityType::Unknown),
            (ActivityType::Realise, 0) => Some(ParsedActivityType::Realise),
            (ActivityType::CopyPaths, 0) => Some(ParsedActivityType::CopyPaths),
            (ActivityType::Builds, 0) => Some(ParsedActivityType::Builds),
            (ActivityType::OptimiseStore, 0) => Some(ParsedActivityType::OptimiseStore),
            (ActivityType::VerifyPaths, 0) => Some(ParsedActivityType::VerifyPaths),
            (ActivityType::BuildWaiting, 0) => Some(ParsedActivityType::BuildWaiting),
            (ActivityType::FetchTree, 0) => Some(ParsedActivityType::FetchTree),
            (ActivityType::CopyPath, 3) => {
                let store_path = fields[0].try_parse()?;
                let source_uri = fields[1].try_parse()?;
                let dest_uri = fields[2].try_parse()?;
                Some(ParsedActivityType::CopyPath {
                    store_path,
                    source_uri,
                    dest_uri,
                })
            }
            (ActivityType::FileTransfer, 1) => {
                let request_uri = fields[0].try_parse()?;
                Some(ParsedActivityType::FileTransfer { request_uri })
            }
            (ActivityType::Build, 4) => {
                let drv_path = fields[0].try_parse()?;
                let remote_machine_s = fields[1].try_parse::<String>()?;
                let remote_machine = (!remote_machine_s.is_empty()).then_some(remote_machine_s);
                let current_round = fields[2].as_int()?;
                let total_rounds = fields[3].as_int()?;
                Some(ParsedActivityType::Build {
                    drv_path,
                    remote_machine,
                    current_round,
                    total_rounds,
                })
            }
            (ActivityType::Substitute, 2) => {
                let store_path = fields[0].try_parse()?;
                let store_uri = fields[1].try_parse()?;
                Some(ParsedActivityType::Substitute {
                    store_path,
                    store_uri,
                })
            }
            (ActivityType::QueryPathInfo, 2) => {
                let store_path = fields[0].try_parse()?;
                let store_uri = fields[1].try_parse()?;
                Some(ParsedActivityType::QueryPathInfo {
                    store_path,
                    store_uri,
                })
            }
            (ActivityType::PostBuildHook, 1) => {
                let drv_path = fields[0].try_parse()?;
                Some(ParsedActivityType::PostBuildHook { drv_path })
            }
            (ActivityType::BuildWaiting, 2) => {
                let drv_path = fields[0].try_parse()?;
                let path_resolved = fields[1].try_parse()?;
                Some(ParsedActivityType::BuildWaitingCAResolved {
                    drv_path,
                    path_resolved,
                })
            }
            _ => None,
        }
    }

    pub(crate) fn parse(activity_type: ActivityType, fields: Vec<Field>) -> Self {
        Self::try_parse(activity_type, &fields).unwrap_or_else(|| ParsedActivityType::Unparsable {
            fields,
            activity_type,
        })
    }

    pub fn into_fields(self) -> Vec<Field> {
        match self {
            ParsedActivityType::Unknown => Vec::new(),
            ParsedActivityType::Realise => Vec::new(),
            ParsedActivityType::CopyPaths => Vec::new(),
            ParsedActivityType::Builds => Vec::new(),
            ParsedActivityType::OptimiseStore => Vec::new(),
            ParsedActivityType::VerifyPaths => Vec::new(),
            ParsedActivityType::BuildWaiting => Vec::new(),
            ParsedActivityType::FetchTree => Vec::new(),
            ParsedActivityType::CopyPath {
                store_path,
                source_uri,
                dest_uri,
            } => {
                vec![
                    Field::from_string(store_path),
                    Field::from_string(source_uri),
                    Field::from_string(dest_uri),
                ]
            }
            ParsedActivityType::FileTransfer { request_uri } => {
                vec![Field::from_string(request_uri)]
            }
            ParsedActivityType::Build {
                drv_path,
                remote_machine,
                current_round,
                total_rounds,
            } => {
                vec![
                    Field::from_string(drv_path),
                    Field::from_string(remote_machine.unwrap_or(String::new())),
                    Field::Int(current_round),
                    Field::Int(total_rounds),
                ]
            }
            ParsedActivityType::Substitute {
                store_path,
                store_uri,
            } => {
                vec![
                    Field::from_string(store_path),
                    Field::from_string(store_uri),
                ]
            }
            ParsedActivityType::QueryPathInfo {
                store_path,
                store_uri,
            } => {
                vec![
                    Field::from_string(store_path),
                    Field::from_string(store_uri),
                ]
            }
            ParsedActivityType::PostBuildHook { drv_path } => vec![Field::from_string(drv_path)],
            ParsedActivityType::BuildWaitingCAResolved {
                drv_path,
                path_resolved,
            } => {
                vec![
                    Field::from_string(drv_path),
                    Field::from_string(path_resolved),
                ]
            }
            ParsedActivityType::Unparsable {
                fields,
                activity_type: _,
            } => fields,
        }
    }

    pub fn as_activity_type(&self) -> ActivityType {
        match self {
            ParsedActivityType::Unknown => ActivityType::Unknown,
            ParsedActivityType::CopyPath {
                store_path: _,
                source_uri: _,
                dest_uri: _,
            } => ActivityType::CopyPath,
            ParsedActivityType::FileTransfer { request_uri: _ } => ActivityType::FileTransfer,
            ParsedActivityType::Realise => ActivityType::Realise,
            ParsedActivityType::CopyPaths => ActivityType::CopyPaths,
            ParsedActivityType::Builds => ActivityType::Builds,
            ParsedActivityType::Build {
                drv_path: _,
                remote_machine: _,
                current_round: _,
                total_rounds: _,
            } => ActivityType::Build,
            ParsedActivityType::OptimiseStore => ActivityType::OptimiseStore,
            ParsedActivityType::VerifyPaths => ActivityType::VerifyPaths,
            ParsedActivityType::Substitute {
                store_path: _,
                store_uri: _,
            } => ActivityType::Substitute,
            ParsedActivityType::QueryPathInfo {
                store_path: _,
                store_uri: _,
            } => ActivityType::QueryPathInfo,
            ParsedActivityType::PostBuildHook { drv_path: _ } => ActivityType::PostBuildHook,
            ParsedActivityType::BuildWaiting => ActivityType::BuildWaiting,
            ParsedActivityType::BuildWaitingCAResolved {
                drv_path: _,
                path_resolved: _,
            } => ActivityType::BuildWaiting,
            ParsedActivityType::FetchTree => ActivityType::FetchTree,
            ParsedActivityType::Unparsable {
                fields: _,
                activity_type,
            } => *activity_type,
        }
    }
}

impl From<Activity> for ParsedActivity {
    fn from(value: Activity) -> Self {
        let activity_type = ParsedActivityType::parse(value.activity_type, value.fields);
        ParsedActivity {
            id: value.id,
            level: value.level,
            parent: value.parent,
            text: value.text,
            activity_type,
        }
    }
}

impl From<ParsedActivity> for Activity {
    fn from(value: ParsedActivity) -> Self {
        let activity_type = value.activity_type.as_activity_type();
        let fields = value.activity_type.into_fields();
        Self {
            id: value.id,
            level: value.level,
            parent: value.parent,
            text: value.text,
            fields,
            activity_type,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Activity {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub fields: Vec<Field>,
    pub id: ActivityId,
    pub level: Verbosity,
    #[serde(with = "optional_activity_id")]
    pub parent: Option<ActivityId>,
    #[serde(serialize_with = "crate::serialize_byte_string")]
    pub text: ByteString, // If logger is JSON, invalid UTF-8 is replaced with U+FFFD
    #[serde(rename = "type")]
    pub activity_type: ActivityType,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct StopActivity {
    pub id: ActivityId,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(from = "ActivityResult", into = "ActivityResult")]
pub struct ParsedActivityResult {
    pub id: ActivityId,
    pub result_type: ParsedResultType,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ParsedResultType {
    FileLinked {
        size: u64,
        blocks: Option<u64>,
    },
    BuildLogLine(ByteString),
    UntrustedPath(FullStorePath),
    CorruptedPath(FullStorePath),
    SetPhase(ByteString),
    Progress {
        done: u64,
        expected: u64,
        running: u64,
        failed: u64,
    },
    SetExpected {
        activity_type: ActivityType,
        expected: u64,
    },
    PostBuildLogLine(ByteString),
    FetchStatus(ByteString),
    Unparsable {
        result_type: ResultType,
        fields: Vec<Field>,
    },
}

impl ParsedResultType {
    fn try_parse(result_type: ResultType, fields: &[Field]) -> Option<Self> {
        match (result_type, fields.len()) {
            (ResultType::FileLinked, 1) => Some(Self::FileLinked {
                size: fields[0].as_int()?,
                blocks: None,
            }),
            (ResultType::FileLinked, 2) => Some(Self::FileLinked {
                size: fields[0].as_int()?,
                blocks: Some(fields[1].as_int()?),
            }),
            (ResultType::BuildLogLine, 1) => {
                Some(Self::BuildLogLine(fields[0].as_string()?.clone()))
            }
            (ResultType::UntrustedPath, 1) => {
                let store_path = fields[0].try_parse()?;
                Some(Self::UntrustedPath(store_path))
            }
            (ResultType::CorruptedPath, 1) => {
                let store_path = fields[0].try_parse()?;
                Some(Self::CorruptedPath(store_path))
            }
            (ResultType::SetPhase, 1) => Some(Self::SetPhase(fields[0].as_string()?.clone())),
            (ResultType::Progress, 4) => Some(Self::Progress {
                done: fields[0].as_int()?,
                expected: fields[1].as_int()?,
                running: fields[2].as_int()?,
                failed: fields[3].as_int()?,
            }),
            (ResultType::SetExpected, 2) => {
                let value: u16 = fields[0].as_int()?.try_into().ok()?;
                Some(Self::SetExpected {
                    activity_type: value.try_into().ok()?,
                    expected: fields[1].as_int()?,
                })
            }
            (ResultType::PostBuildLogLine, 1) => {
                Some(Self::PostBuildLogLine(fields[0].as_string()?.clone()))
            }
            (ResultType::FetchStatus, 1) => Some(Self::FetchStatus(fields[0].as_string()?.clone())),
            _ => None,
        }
    }

    pub(crate) fn parse(result_type: ResultType, fields: Vec<Field>) -> Self {
        Self::try_parse(result_type, &fields).unwrap_or_else(|| Self::Unparsable {
            fields,
            result_type,
        })
    }

    pub fn into_fields(self) -> Vec<Field> {
        match self {
            ParsedResultType::FileLinked { size, blocks } => {
                if let Some(blocks) = blocks {
                    vec![Field::Int(size), Field::Int(blocks)]
                } else {
                    vec![Field::Int(size)]
                }
            }
            ParsedResultType::BuildLogLine(bytes) => vec![Field::String(bytes)],
            ParsedResultType::UntrustedPath(store_path) => vec![Field::from_string(store_path)],
            ParsedResultType::CorruptedPath(store_path) => vec![Field::from_string(store_path)],
            ParsedResultType::SetPhase(bytes) => vec![Field::String(bytes)],
            ParsedResultType::Progress {
                done,
                expected,
                running,
                failed,
            } => {
                vec![
                    Field::Int(done),
                    Field::Int(expected),
                    Field::Int(running),
                    Field::Int(failed),
                ]
            }
            ParsedResultType::SetExpected {
                activity_type,
                expected,
            } => {
                vec![
                    Field::Int(activity_type.as_u16() as u64),
                    Field::Int(expected),
                ]
            }
            ParsedResultType::PostBuildLogLine(bytes) => vec![Field::String(bytes)],
            ParsedResultType::FetchStatus(bytes) => vec![Field::String(bytes)],
            ParsedResultType::Unparsable {
                result_type: _,
                fields,
            } => fields,
        }
    }

    pub fn as_result_type(&self) -> ResultType {
        match self {
            ParsedResultType::FileLinked { size: _, blocks: _ } => ResultType::FileLinked,
            ParsedResultType::BuildLogLine(_) => ResultType::BuildLogLine,
            ParsedResultType::UntrustedPath(_) => ResultType::UntrustedPath,
            ParsedResultType::CorruptedPath(_) => ResultType::CorruptedPath,
            ParsedResultType::SetPhase(_) => ResultType::SetPhase,
            ParsedResultType::Progress {
                done: _,
                expected: _,
                running: _,
                failed: _,
            } => ResultType::Progress,
            ParsedResultType::SetExpected {
                activity_type: _,
                expected: _,
            } => ResultType::SetExpected,
            ParsedResultType::PostBuildLogLine(_) => ResultType::PostBuildLogLine,
            ParsedResultType::FetchStatus(_) => ResultType::FetchStatus,
            ParsedResultType::Unparsable {
                result_type,
                fields: _,
            } => *result_type,
        }
    }
}

impl From<ActivityResult> for ParsedActivityResult {
    fn from(value: ActivityResult) -> Self {
        let result_type = ParsedResultType::parse(value.result_type, value.fields);
        Self {
            id: value.id,
            result_type,
        }
    }
}

impl From<ParsedActivityResult> for ActivityResult {
    fn from(value: ParsedActivityResult) -> Self {
        let result_type = value.result_type.as_result_type();
        let fields = value.result_type.into_fields();
        let id = value.id;
        Self {
            fields,
            id,
            result_type,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ActivityResult {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub fields: Vec<Field>,
    pub id: ActivityId,
    #[serde(rename = "type")]
    pub result_type: ResultType,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Field {
    Int(u64),
    String(#[serde(serialize_with = "crate::serialize_byte_string")] ByteString),
}

impl Field {
    pub fn from_string<D>(value: D) -> Self
    where
        D: ToString,
    {
        Field::String(value.to_string().into_bytes().into())
    }

    fn try_parse<P>(&self) -> Option<P>
    where
        P: FromStr,
    {
        let s = self.as_string()?.to_str().ok()?;
        s.parse::<P>().ok()
    }

    pub fn as_int(&self) -> Option<u64> {
        if let Field::Int(ret) = self {
            Some(*ret)
        } else {
            None
        }
    }

    pub fn as_string(&self) -> Option<&ByteString> {
        if let Field::String(ret) = self {
            Some(ret)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod unittests {
    use rstest::rstest;
    use rstest_reuse::{apply, template};

    use super::*;

    #[template]
    #[rstest]
    #[case::message(
        r#"{"action":"msg","level":3,"msg":"these 501 derivations will be built:"}"#,
        LogMessage::Message(Message{level: Verbosity::Info, text: "these 501 derivations will be built:".into()}),
        ParsedLogMessage::Message(Message{level: Verbosity::Info, text: "these 501 derivations will be built:".into()})
    )]
    #[case::result(
        r#"{"action":"result","fields":[3,6,2,1],"id":342850059370512,"type":105}"#,
        LogMessage::Result(ActivityResult { id: ActivityId::from_u64(342850059370512), result_type: ResultType::Progress, fields: vec![Field::Int(3), Field::Int(6), Field::Int(2), Field::Int(1)] }),
        ParsedLogMessage::Result(ParsedActivityResult {
            id: ActivityId::from_u64(342850059370512),
            result_type: ParsedResultType::Progress {
                done: 3,
                expected: 6,
                running: 2,
                failed: 1
            }})
    )]
    #[case::start(
        r#"{"action":"start","fields":["/nix/store/rpd4ahsq8kk6i6ji31yww38466zxsmnx-cargo-vendor-dir","https://cache.nixos.org/"],"id":342850059370553,"level":4,"parent":0,"text":"querying info about '/nix/store/rpd4ahsq8kk6i6ji31yww38466zxsmnx-cargo-vendor-dir' on 'https://cache.nixos.org'","type":109}"#,
        LogMessage::StartActivity(Activity {
            id: ActivityId::from_u64(342850059370553),
            level: Verbosity::Talkative,
            activity_type: ActivityType::QueryPathInfo,
            text: "querying info about '/nix/store/rpd4ahsq8kk6i6ji31yww38466zxsmnx-cargo-vendor-dir' on 'https://cache.nixos.org'".into(),
            fields: vec![
                Field::String("/nix/store/rpd4ahsq8kk6i6ji31yww38466zxsmnx-cargo-vendor-dir".into()),
                Field::String("https://cache.nixos.org/".into()),
            ],
            parent: None,
        }),
        ParsedLogMessage::StartActivity(ParsedActivity {
            id: ActivityId::from_u64(342850059370553),
            level: Verbosity::Talkative,
            text: "querying info about '/nix/store/rpd4ahsq8kk6i6ji31yww38466zxsmnx-cargo-vendor-dir' on 'https://cache.nixos.org'".into(),
            parent: None,
            activity_type: ParsedActivityType::QueryPathInfo {
                store_path: "/nix/store/rpd4ahsq8kk6i6ji31yww38466zxsmnx-cargo-vendor-dir".parse().unwrap(),
                store_uri: "https://cache.nixos.org/".parse().unwrap(),
            },
        })
    )]
    #[case::start_no_fields(
        r#"{"action":"start","id":342631016038421,"level":5,"parent":0,"text":"copying '/home/myself/nixpkgs/pkgs/build-support/fetchurl/write-mirror-list.sh' to the store","type":0}"#,
        LogMessage::StartActivity(Activity {
            id: ActivityId::from_u64(342631016038421),
            level: Verbosity::Chatty,
            activity_type: ActivityType::Unknown,
            text: "copying '/home/myself/nixpkgs/pkgs/build-support/fetchurl/write-mirror-list.sh' to the store".into(),
            fields: vec![],
            parent: None,
        }),
        ParsedLogMessage::StartActivity(ParsedActivity {
            id: ActivityId::from_u64(342631016038421),
            level: Verbosity::Chatty,
            text: "copying '/home/myself/nixpkgs/pkgs/build-support/fetchurl/write-mirror-list.sh' to the store".into(),
            parent: None,
            activity_type: ParsedActivityType::Unknown,
        })
    )]
    #[case::stop(
        r#"{"action":"stop","id":342850059370518}"#,
        LogMessage::StopActivity(StopActivity { id: ActivityId::from_u64(342850059370518) }),
        ParsedLogMessage::StopActivity(StopActivity { id: ActivityId::from_u64(342850059370518) })
    )]
    #[case::start_weird_copy_path(
        r#"{"action":"start","fields":["/nix/store/00000000000000000000000000000000-=","local","daemon"],"id":17172761494985901226,"level":0,"parent":0,"text":"","type":100}"#,
        LogMessage::StartActivity(Activity {
            id: ActivityId::from_u64(17172761494985901226),
            level: Verbosity::Error,
            activity_type: ActivityType::CopyPath,
            text: "".into(),
            fields: vec![
                Field::String("/nix/store/00000000000000000000000000000000-=".into()),
                Field::String("local".into()),
                Field::String("daemon".into()),
            ],
            parent: None,
        }),
        ParsedLogMessage::StartActivity(ParsedActivity {
            id: ActivityId::from_u64(17172761494985901226),
            level: nixrs::log::Verbosity::Error,
            parent: None,
            text: "".into(),
            activity_type: nixrs::log::ParsedActivityType::CopyPath {
                store_path: "/nix/store/00000000000000000000000000000000-=".parse().unwrap(),
                source_uri: nixrs::log::StoreUri::Local,
                dest_uri: nixrs::log::StoreUri::Daemon,
            },
        })
    )]
    fn log_cases(
        #[case] json: &str,
        #[case] expected: LogMessage,
        #[case] expected_parsed: ParsedLogMessage,
    ) {
    }

    #[apply(log_cases)]
    fn serialize_deserialize(
        #[case] json: &str,
        #[case] expected: LogMessage,
        #[case] _expected_parsed: ParsedLogMessage,
    ) {
        let actual: LogMessage = serde_json::from_str(json).unwrap();
        pretty_assertions::assert_eq!(actual, expected);
        let actual_s = serde_json::to_string(&expected).unwrap();
        pretty_assertions::assert_eq!(actual_s, json);
    }

    #[apply(log_cases)]
    fn serialize_deserialize_parsed(
        #[case] json: &str,
        #[case] _expected: LogMessage,
        #[case] expected_parsed: ParsedLogMessage,
    ) {
        let actual: ParsedLogMessage = serde_json::from_str(json).unwrap();
        pretty_assertions::assert_eq!(actual, expected_parsed);
        let actual_s = serde_json::to_string(&expected_parsed).unwrap();
        pretty_assertions::assert_eq!(actual_s, json);
    }

    #[apply(log_cases)]
    fn conversion(
        #[case] _json: &str,
        #[case] expected: LogMessage,
        #[case] expected_parsed: ParsedLogMessage,
    ) {
        let actual: LogMessage = expected_parsed.clone().into();
        pretty_assertions::assert_eq!(actual, expected);
        let actual_parsed: ParsedLogMessage = expected.into();
        pretty_assertions::assert_eq!(actual_parsed, expected_parsed);
    }
}

#[cfg(all(test, feature = "daemon"))]
mod proptests {
    use proptest::test_runner::TestCaseResult;

    use super::*;
    use crate::pretty_prop_assert_eq;

    #[test_strategy::proptest(async = "tokio")]
    async fn conversion(expected: ParsedLogMessage) -> TestCaseResult {
        let msg: LogMessage = expected.clone().into();
        let actual: ParsedLogMessage = msg.into();
        pretty_prop_assert_eq!(expected, actual);
        Ok(())
    }
}
