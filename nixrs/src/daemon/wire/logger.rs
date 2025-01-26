use std::fmt;
use std::str::FromStr;

use crate::daemon::logger::{Activity, ActivityResult, LogError};
#[cfg(feature = "nixrs-derive")]
use crate::daemon::ser::{NixSerialize, NixWrite};
use crate::daemon::DaemonString;
use bytes::Bytes;
#[cfg(feature = "nixrs-derive")]
use nixrs_derive::{NixDeserialize, NixSerialize};
use num_enum::{IntoPrimitive, TryFromPrimitive};

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
#[cfg_attr(feature = "nixrs-derive", derive(NixDeserialize))]
#[cfg_attr(feature = "nixrs-derive", nix(tag = "RawLogMessageType"))]
pub enum RawLogMessage {
    Last,
    Error(LogError),
    Next(DaemonString), // If logger is JSON, invalid UTF-8 is replaced with U+FFFD
    Read(usize),
    Write(Bytes),
    StartActivity(Activity),
    StopActivity(u64),
    Result(ActivityResult),
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
