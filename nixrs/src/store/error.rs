use std::backtrace::Backtrace;
use std::io;

use thiserror::Error;
use tokio::io::{AsyncWrite, AsyncWriteExt};

use super::daemon::WorkerProtoOp;
use super::derived_path::ReadDerivedPathError;
use super::legacy_worker::ServeCommand;
use super::settings::ParseSettingError;
use super::{
    DerivationOutputsError, ParseDrvOutputError, ReadDerivationError, WriteDerivationError,
};
use crate::hash;
use crate::io::AsyncSink;
use crate::num_enum::num_enum;
use crate::path_info::Compression;
use crate::signature;
use crate::store_path::ParseContentAddressError;
use crate::store_path::{ParseStorePathError, ReadStorePathError};

num_enum! {
    #[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
    pub enum Verbosity {
        Unknown(u64),
        Error = 0,
        Warn = 1,
        Notice = 2,
        Info = 3,
        Talkative = 4,
        Chatty = 5,
        Debug = 6,
        Vomit = 7,
    }
}

impl Verbosity {
    pub const fn to_tracing(&self) -> tracing::Level {
        use tracing::Level;
        use Verbosity::*;
        match self {
            Error => Level::ERROR,
            Warn => Level::WARN,
            Notice => Level::INFO,
            Info => Level::INFO,
            Talkative => Level::INFO,
            Chatty => Level::DEBUG,
            Debug => Level::DEBUG,
            Vomit => Level::TRACE,
            _ => Level::TRACE,
        }
    }
}

impl<'a> From<&'a tracing::Level> for Verbosity {
    fn from(value: &'a tracing::Level) -> Self {
        match *value {
            tracing::Level::ERROR => Verbosity::Error,
            tracing::Level::WARN => Verbosity::Warn,
            tracing::Level::INFO => Verbosity::Info,
            tracing::Level::DEBUG => Verbosity::Debug,
            tracing::Level::TRACE => Verbosity::Vomit,
        }
    }
}

impl From<Verbosity> for tracing::Level {
    fn from(value: Verbosity) -> Self {
        value.to_tracing()
    }
}

#[derive(Debug, Error)]
pub enum Error {
    #[error("Store path set forms a cycle")]
    CycleDetected,
    #[error("wanted to fetch '{0}' but the legacy ssh protocol doesn't support merely substituting drv files via the build paths command. It would build them instead. Try using ssh-ng://")]
    WantedFetchInLegacy(String),
    #[error("{0}")]
    StorePath(
        #[from]
        #[source]
        ReadStorePathError,
    ),
    #[error("{0}")]
    BadDerivation(
        #[from]
        #[source]
        ReadDerivationError,
    ),
    #[error("{0}")]
    DerivationWrite(
        #[from]
        #[source]
        WriteDerivationError,
    ),
    #[error("{0}")]
    BadDrvOutput(
        #[from]
        #[source]
        ParseDrvOutputError,
    ),
    #[error("{0}")]
    BadDerivedPath(
        #[from]
        #[source]
        ReadDerivedPathError,
    ),
    #[error("path '{0}' is not a valid store path")]
    InvalidPath(String),
    #[error("path '{}' is not a store path", .0.display())]
    BadStorePath(std::path::PathBuf),
    #[error("path '{}' is not in the Nix store", .0.display())]
    NotInStore(std::path::PathBuf),
    #[error(".narinfo file is corrupt")]
    BadNarInfo,
    #[error("invalid base32 string")]
    BadBase32(
        #[from]
        #[source]
        crate::base32::BadBase32,
    ),
    #[error("store path name is empty")]
    StorePathNameEmpty,
    #[error("store path name is longer than 211 characters")]
    StorePathNameTooLong,
    #[error("store path name '{0}' contains forbidden character")]
    BadStorePathName(String),
    #[error("size field in NAR is too big")]
    NarSizeFieldTooBig,
    #[error("NAR string is not valid UTF-8")]
    BadNarString,
    #[error("NAR padding is not zero")]
    BadNarPadding,
    #[error("unsupported NAR version")]
    BadNarVersionMagic,
    #[error("NAR open tag is missing")]
    MissingNarOpenTag,
    #[error("NAR close tag is missing")]
    MissingNarCloseTag,
    #[error("expected NAR field is missing")]
    MissingNarField,
    #[error("unrecognized NAR field '{0}'")]
    BadNarField(String),
    #[error("bad 'executable' field in NAR")]
    BadExecutableField,
    #[error("I/O error: {source}")]
    IOError { source: std::io::Error },
    #[error("Join error: {0}")]
    JoinError(
        #[from]
        #[source]
        tokio::task::JoinError,
    ),
    #[error("URL error: {0}")]
    URLError(
        #[from]
        #[source]
        url::ParseError,
    ),
    #[error("HTTP error: {0}")]
    ReqwestError(
        #[from]
        #[source]
        reqwest::Error,
    ),
    #[error("{0}")]
    Misc(String),
    #[error("Unsupported compression '{0}'")]
    UnsupportedCompression(Compression),
    #[error("Unsupported operation '{0}'")]
    UnsupportedOperation(String),
    #[error("Unknown protocol command '{0}'")]
    UnknownProtocolCommand(u64),
    #[error("Compression error {0}")]
    CompressionError(
        #[from]
        #[source]
        compress_tools::Error,
    ),
    #[error("JSON error: {0}")]
    JSONError(
        #[from]
        #[source]
        serde_json::Error,
    ),
    #[error("client requested repeating builds, but this is not currently implemented")]
    RepeatingBuildsUnsupported,
    #[error("protocol mismatch")]
    DaemonProtocolMismatch,
    #[error("Nix daemon protocol version not supported")]
    UnsupportedDaemonProtocol,
    #[error("the Nix daemon version is too old")]
    DaemonVersionTooOld,
    #[error("the Nix client version is too old")]
    DaemonClientVersionTooOld,
    #[error("Invalid trusted status from remote")]
    InvalidTrustedStatus,
    #[error("no sink")]
    NoSink,
    #[error("no source")]
    NoSource,
    #[error("got unknown message type {0:x} from Nix daemon")]
    UnknownMessageType(u64),
    #[error("cannot open connection to remote store '{0}': {1}")]
    OpenConnectionFailed(String, #[source] Box<Error>),
    #[error("{msg}")]
    ErrorInfo {
        level: Verbosity,
        msg: String,
        traces: Vec<String>,
    },
    #[error("got unsupported field type {0:x} from Nix daemon")]
    UnsupportedFieldType(u64),
    #[error("trying to request '{0}', but daemon protocol {1}.{2} is too old (< 1.29) to request a derivation file")]
    ProtocolTooOld(String, u64, u64),
    #[error("wanted to build a derivation that is itself a build product, but the legacy 'ssh://' protocol doesn't support that. Try using 'ssh-ng://'")]
    DerivationIsBuildProduct,
    #[error("repairing or checking is not supported when building through the Nix daemon")]
    RepairingOrCheckingNotSupported,
    #[error("invalid operation {0}")]
    InvalidOperation(WorkerProtoOp),
    #[error("Removed operation {0}")]
    RemovedOperation(WorkerProtoOp),
    #[error("repairing is not allowed because you are not in 'trusted-users'")]
    RepairNotAllowed,
    #[error("you are not privileged to build input-addressed derivations")]
    MissingPrivilegesToBuild,
    #[error("{0}")]
    DerivationOutputs(
        #[from]
        #[source]
        DerivationOutputsError,
    ),
    #[error("{0}")]
    ParseSetting(
        #[from]
        #[source]
        ParseSettingError,
    ),
    #[error("{0} is not allowed")]
    WriteOnlyLegacyStore(ServeCommand),
    #[error("tar archive contains illegal file name '{0}'")]
    BadTarFileMemberName(String),
    #[error("protocol mismatch 0x{0:x}")]
    LegacyProtocolServeMismatch(u64),
    #[error("protocol mismatch with 'nix-store --serve' on '{0}'")]
    LegacyProtocolMismatch(String),
    #[error("unsupported 'nix-store --serve' protocol version on '{0}'")]
    UnsupportedLegacyProtocol(String),
    #[error("failed to add path '{0}' to remote host '{1}")]
    FailedToAddToStore(String, String),
    #[error("NAR hash is now mandatory")]
    MandatoryNARHash,
    #[error("{0}")]
    BadHash(
        #[from]
        #[source]
        hash::ParseHashError,
    ),
    #[error("{0}")]
    BadSignature(
        #[from]
        #[source]
        signature::ParseSignatureError,
    ),
    #[error("{0}")]
    BadContentAddress(
        #[from]
        #[source]
        ParseContentAddressError,
    ),
    #[error("{1}")]
    Custom(u64, String),
}

impl Error {
    pub fn exit_code(&self) -> u64 {
        match self {
            Error::Custom(exit, _) => *exit,
            Error::LegacyProtocolServeMismatch(_) => 2,
            _ => 1,
        }
    }

    pub fn level(&self) -> Verbosity {
        if let Error::ErrorInfo { level, .. } = self {
            *level
        } else {
            Verbosity::Error
        }
    }

    pub fn traces(&self) -> Option<&Vec<String>> {
        if let Error::ErrorInfo { traces, .. } = self {
            Some(traces)
        } else {
            None
        }
    }

    pub async fn write<S: AsyncWrite + Unpin>(&self, mut sink: S) -> io::Result<()> {
        sink.write_str("Error").await?;
        sink.write_enum(self.level()).await?;
        sink.write_str("Error").await?; // Removed
        sink.write_string(self.to_string()).await?;
        sink.write_u64_le(0).await?; // info.errPos
        if let Some(traces) = self.traces() {
            sink.write_usize(traces.len()).await?;
            for trace in traces.iter() {
                sink.write_u64_le(0).await?; // trace.errPos
                sink.write_str(trace).await?;
            }
        } else {
            sink.write_u64_le(0).await?;
        }

        Ok(())
    }
}

impl From<std::io::Error> for Error {
    fn from(source: std::io::Error) -> Self {
        eprintln!("Error {}", Backtrace::capture());
        Error::IOError { source }
    }
}

impl From<ParseStorePathError> for Error {
    fn from(v: ParseStorePathError) -> Error {
        Error::StorePath(ReadStorePathError::BadStorePath(v))
    }
}

/*
impl From<hash::UnknownAlgorithm> for Error {
    fn from(v: hash::UnknownAlgorithm) -> Error {
        Error::BadHash(hash::ParseHashError::Algorithm(v))
    }
} */
