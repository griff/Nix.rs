use std::fmt;
use std::str::FromStr;

use bytes::Bytes;
#[cfg(feature = "nixrs-derive")]
use nixrs_derive::{NixDeserialize, NixSerialize};
use num_enum::{IntoPrimitive, TryFromPrimitive};

#[cfg(feature = "nixrs-derive")]
use crate::daemon::ser::{NixSerialize, NixWrite};
use crate::daemon::wire::IgnoredZero;
use crate::daemon::DaemonString;
use crate::daemon::{DaemonError, DaemonErrorKind, DaemonInt, RemoteError};
use crate::log::{Activity, ActivityResult, StopActivity, Verbosity};

pub const STDERR_LAST: u64 = 0x616c7473; // 'alts' in ASCII
pub const STDERR_ERROR: u64 = 0x63787470; // 'cxtp' in ASCII
pub const STDERR_NEXT: u64 = 0x6f6c6d67; // 'olmg' in ASCII
pub const STDERR_READ: u64 = 0x64617461; // 'data' in ASCII
pub const STDERR_WRITE: u64 = 0x64617416;
pub const STDERR_START_ACTIVITY: u64 = 0x53545254; // 'STRT' in ASCII
pub const STDERR_STOP_ACTIVITY: u64 = 0x53544f50; // 'STOP' in ASCII
pub const STDERR_RESULT: u64 = 0x52534c54; // 'RSLT' in ASCII

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, TryFromPrimitive, IntoPrimitive,
)]
#[cfg_attr(feature = "nixrs-derive", derive(NixDeserialize, NixSerialize))]
#[cfg_attr(feature = "nixrs-derive", nix(try_from = "u64", into = "u64"))]
#[repr(u64)]
pub enum RawLogMessageType {
    Last = STDERR_LAST,
    Error = STDERR_ERROR,
    Next = STDERR_NEXT,
    Read = STDERR_READ,
    Write = STDERR_WRITE,
    StartActivity = STDERR_START_ACTIVITY,
    StopActivity = STDERR_STOP_ACTIVITY,
    Result = STDERR_RESULT,
}

#[derive(Debug)]
//#[cfg_attr(feature = "nixrs-derive", nix(tag = "RawLogMessageType"))]
pub enum RawLogMessage {
    Last,
    Error(LogError),
    Next(DaemonString), // If logger is JSON, invalid UTF-8 is replaced with U+FFFD
    Read(usize),
    Write(Bytes),
    StartActivity(Activity),
    StopActivity(StopActivity),
    Result(ActivityResult),
}

#[cfg(feature = "nixrs-derive")]
impl crate::daemon::de::NixDeserialize for RawLogMessage {
    async fn try_deserialize<R>(reader: &mut R) -> Result<Option<Self>, R::Error>
    where
        R: ?Sized + crate::daemon::de::NixRead + Send,
    {
        use tracing::trace;

        if let Some(tag) = reader.try_read_value::<RawLogMessageType>().await? {
            trace!(?tag, "Read log message tag");
            match tag {
                RawLogMessageType::Last => Ok(Some(RawLogMessage::Last)),
                RawLogMessageType::Error => {
                    Ok(Some(RawLogMessage::Error(reader.read_value().await?)))
                }
                RawLogMessageType::Next => {
                    Ok(Some(RawLogMessage::Next(reader.read_value().await?)))
                }
                RawLogMessageType::Read => {
                    Ok(Some(RawLogMessage::Read(reader.read_value().await?)))
                }
                RawLogMessageType::Write => {
                    Ok(Some(RawLogMessage::Write(reader.read_value().await?)))
                }
                RawLogMessageType::StartActivity => Ok(Some(RawLogMessage::StartActivity(
                    reader.read_value().await?,
                ))),
                RawLogMessageType::StopActivity => Ok(Some(RawLogMessage::StopActivity(
                    reader.read_value().await?,
                ))),
                RawLogMessageType::Result => {
                    Ok(Some(RawLogMessage::Result(reader.read_value().await?)))
                }
            }
        } else {
            Ok(None)
        }
    }
}

#[cfg(feature = "nixrs-derive")]
impl NixSerialize for RawLogMessage {
    async fn serialize<W>(&self, writer: &mut W) -> Result<(), W::Error>
    where
        W: NixWrite,
    {
        use RawLogMessageType::*;
        match self {
            RawLogMessage::Last => writer.write_value(&Last).await?,
            RawLogMessage::Error(err) => {
                writer.write_value(&Error).await?;
                writer.write_value(err).await?;
            }
            RawLogMessage::Next(msg) => {
                writer.write_value(&Next).await?;
                writer.write_value(msg).await?;
            }
            RawLogMessage::Read(len) => {
                writer.write_value(&Read).await?;
                writer.write_value(len).await?;
            }
            RawLogMessage::Write(buf) => {
                writer.write_value(&Write).await?;
                writer.write_value(buf).await?;
            }
            RawLogMessage::StartActivity(act) => {
                if writer.version().minor() >= 20 {
                    writer.write_value(&StartActivity).await?;
                    writer.write_value(act).await?;
                } else {
                    writer.write_value(&Next).await?;
                    writer.write_value(&act.text).await?;
                }
            }
            RawLogMessage::StopActivity(act) => {
                if writer.version().minor() >= 20 {
                    writer.write_value(&StopActivity).await?;
                    writer.write_value(act).await?;
                }
            }
            RawLogMessage::Result(result) => {
                if writer.version().minor() >= 20 {
                    writer.write_value(&Result).await?;
                    writer.write_value(result).await?;
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "nixrs-derive", derive(NixDeserialize, NixSerialize))]
pub struct TraceLine {
    _have_pos: IgnoredZero,
    pub hint: DaemonString, // If logger is JSON, invalid UTF-8 is replaced with U+FFFD
}

fn default_exit_status() -> DaemonInt {
    1
}

#[derive(Debug)]
#[cfg_attr(feature = "nixrs-derive", derive(NixDeserialize, NixSerialize))]
pub struct LogError {
    #[cfg_attr(feature = "nixrs-derive", nix(version = "26.."))]
    _ty: IgnoredErrorType,
    #[cfg_attr(feature = "nixrs-derive", nix(version = "26.."))]
    pub level: Verbosity,
    #[cfg_attr(feature = "nixrs-derive", nix(version = "26.."))]
    _name: IgnoredErrorType,
    pub msg: DaemonString, // If logger is JSON, invalid UTF-8 is replaced with U+FFFD
    #[cfg_attr(
        feature = "nixrs-derive",
        nix(version = "..=25", default = "default_exit_status")
    )]
    pub exit_status: DaemonInt,
    #[cfg_attr(feature = "nixrs-derive", nix(version = "26.."))]
    _have_pos: IgnoredZero,
    #[cfg_attr(feature = "nixrs-derive", nix(version = "26.."))]
    pub traces: Vec<TraceLine>,
}

impl From<RemoteError> for LogError {
    fn from(value: RemoteError) -> Self {
        LogError {
            level: value.level,
            msg: value.msg,
            exit_status: value.exit_status,
            traces: value.traces,
            _ty: IgnoredErrorType,
            _name: IgnoredErrorType,
            _have_pos: IgnoredZero,
        }
    }
}

impl From<DaemonError> for LogError {
    fn from(value: DaemonError) -> Self {
        match value.kind().clone() {
            DaemonErrorKind::Remote(remote_error) => remote_error.into(),
            _ => {
                let msg = value.to_string().into_bytes().into();
                LogError {
                    msg,
                    level: Verbosity::Error,
                    exit_status: 1,
                    traces: Vec::new(),
                    _ty: IgnoredErrorType,
                    _name: IgnoredErrorType,
                    _have_pos: IgnoredZero,
                }
            }
        }
    }
}

impl From<LogError> for RemoteError {
    fn from(err: LogError) -> Self {
        RemoteError {
            level: err.level,
            msg: err.msg,
            traces: err.traces,
            exit_status: err.exit_status,
        }
    }
}

#[derive(Debug, Default)]
#[cfg_attr(feature = "nixrs-derive", derive(NixDeserialize, NixSerialize))]
#[cfg_attr(feature = "nixrs-derive", nix(from_str, display))]
pub struct IgnoredErrorType;

impl fmt::Display for IgnoredErrorType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Error")
    }
}

impl FromStr for IgnoredErrorType {
    type Err = String;
    fn from_str(_: &str) -> Result<Self, Self::Err> {
        Ok(IgnoredErrorType)
    }
}
