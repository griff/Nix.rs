use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::future::ready;
use std::pin::Pin;
use std::time::Duration;

use bstr::ByteSlice;
use bytes::Bytes;
use derive_more::Display;
use futures::Stream;
use nixrs_derive::{NixDeserialize, NixSerialize};
use num_enum::{IntoPrimitive, TryFromPrimitive};
#[cfg(any(test, feature = "test"))]
use proptest::prelude::any_with;
#[cfg(any(test, feature = "test"))]
use test_strategy::Arbitrary;
use thiserror::Error;
use tokio::io::AsyncBufRead;

use crate::daemon::logger::FutureResultExt;
use crate::daemon::wire::logger::{LogError, TraceLine};
use crate::daemon::{ProtocolRange, ResultLogExt};
use crate::derivation::{BasicDerivation, OutputName};
use crate::derived_path::DerivedPath;
use crate::hash::NarHash;
use crate::log::Verbosity;
use crate::realisation::{DrvOutput, Realisation};
use crate::signature::Signature;
use crate::store_path::{
    ContentAddress, ContentAddressMethodAlgorithm, HasStoreDir, StorePath, StorePathHash,
    StorePathSet,
};
#[cfg(any(test, feature = "test"))]
use crate::test::arbitrary::signature::arb_signatures;

use super::ProtocolVersion;
use super::logger::ResultLog;
use super::wire::{IgnoredTrue, IgnoredZero};

pub type DaemonString = Bytes;
pub type DaemonPath = Bytes;
pub type DaemonInt = libc::c_uint;
pub type DaemonTime = libc::time_t;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, NixDeserialize, NixSerialize)]
#[repr(transparent)]
pub struct Microseconds(i64);

impl From<i64> for Microseconds {
    fn from(value: i64) -> Self {
        Microseconds(value)
    }
}

impl From<Microseconds> for Duration {
    fn from(value: Microseconds) -> Self {
        Duration::from_micros(value.0.unsigned_abs())
    }
}

impl TryFrom<Duration> for Microseconds {
    type Error = std::num::TryFromIntError;
    fn try_from(value: Duration) -> Result<Self, Self::Error> {
        Ok(Microseconds(value.as_micros().try_into()?))
    }
}

impl From<Microseconds> for i64 {
    fn from(value: Microseconds) -> Self {
        value.0
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
    Display,
    NixDeserialize,
    NixSerialize,
)]
#[nix(try_from = "u64", into = "u64")]
#[repr(u64)]
pub enum Operation {
    IsValidPath = 1,
    QueryReferrers = 6,
    AddToStore = 7,
    BuildPaths = 9,
    EnsurePath = 10,
    AddTempRoot = 11,
    AddIndirectRoot = 12,
    FindRoots = 14,
    SetOptions = 19,
    CollectGarbage = 20,
    QueryAllValidPaths = 23,
    QueryPathInfo = 26,
    QueryPathFromHashPart = 29,
    QueryValidPaths = 31,
    QuerySubstitutablePaths = 32,
    QueryValidDerivers = 33,
    OptimiseStore = 34,
    VerifyStore = 35,
    BuildDerivation = 36,
    AddSignatures = 37,
    NarFromPath = 38,
    AddToStoreNar = 39,
    QueryMissing = 40,
    QueryDerivationOutputMap = 41,
    RegisterDrvOutput = 42,
    QueryRealisation = 43,
    AddMultipleToStore = 44,
    AddBuildLog = 45,
    BuildPathsWithResults = 46,
    AddPermRoot = 47,

    /// Obsolete Nix 2.5.0 Protocol 1.32
    SyncWithGC = 13,
    /// Obsolete Nix 2.4 Protocol 1.25
    AddTextToStore = 8,
    /// Obsolete Nix 2.4 Protocol 1.22*
    QueryDerivationOutputs = 22,
    /// Obsolete Nix 2.4 Protocol 1.21
    QueryDerivationOutputNames = 28,
    /// Obsolete Nix 2.0, Protocol 1.19*
    QuerySubstitutablePathInfos = 30,
    /// Obsolete Nix 2.0 Protocol 1.17
    ExportPath = 16,
    /// Obsolete Nix 2.0 Protocol 1.17
    ImportPaths = 27,
    /// Obsolete Nix 2.0 Protocol 1.16
    QueryPathHash = 4,
    /// Obsolete Nix 2.0 Protocol 1.16
    QueryReferences = 5,
    /// Obsolete Nix 2.0 Protocol 1.16
    QueryDeriver = 18,
    /// Obsolete Nix 1.2 Protocol 1.12
    HasSubstitutes = 3,
    /// Obsolete Nix 1.2 Protocol 1.12
    QuerySubstitutablePathInfo = 21,
    // Removed Nix 2.0 Protocol 1.16
    // QueryFailedPaths = 24,
    // Removed Nix 2.0 Protocol 1.16
    // ClearFailedPaths = 25,
    // Removed Nix 1.0 Protocol 1.09
    // ImportPath = 17,
    // Became dead code in Nix 0.11 and removed in Nix 1.8
    // Quit = 0,
    // Removed Nix 0.12 Protocol 1.02
    // RemovedCollectGarbage = 15,
}

impl Operation {
    pub fn versions(&self) -> ProtocolRange {
        match self {
            Operation::IsValidPath => (..).into(),
            Operation::HasSubstitutes => (..12).into(),
            Operation::QueryPathHash => (..16).into(),
            Operation::QueryReferences => (..16).into(),
            Operation::QueryReferrers => (..).into(),
            Operation::AddToStore => (..).into(),
            Operation::AddTextToStore => (..25).into(),
            Operation::BuildPaths => (..).into(),
            Operation::EnsurePath => (..).into(),
            Operation::AddTempRoot => (..).into(),
            Operation::AddIndirectRoot => (..).into(),
            Operation::SyncWithGC => (..32).into(),
            Operation::FindRoots => (..).into(),
            Operation::ExportPath => (..17).into(),
            Operation::QueryDeriver => (..16).into(),
            Operation::SetOptions => (..).into(),
            Operation::CollectGarbage => (2..).into(),
            Operation::QuerySubstitutablePathInfo => (2..12).into(),
            Operation::QueryDerivationOutputs => (5..22).into(),
            Operation::QueryAllValidPaths => (5..).into(),
            Operation::QueryPathInfo => (6..).into(),
            Operation::ImportPaths => (9..17).into(),
            Operation::QueryDerivationOutputNames => (8..21).into(),
            Operation::QueryPathFromHashPart => (11..).into(),
            Operation::QuerySubstitutablePathInfos => (12..19).into(),
            Operation::QueryValidPaths => (12..).into(),
            Operation::QuerySubstitutablePaths => (12..).into(),
            Operation::QueryValidDerivers => (13..).into(),
            Operation::OptimiseStore => (14..).into(),
            Operation::VerifyStore => (14..).into(),
            Operation::BuildDerivation => (14..).into(),
            Operation::AddSignatures => (16..).into(),
            Operation::NarFromPath => (17..).into(),
            Operation::AddToStoreNar => (17..).into(),
            Operation::QueryMissing => (19..).into(),
            Operation::QueryDerivationOutputMap => (22..).into(),
            Operation::RegisterDrvOutput => (27..).into(),
            Operation::QueryRealisation => (27..).into(),
            Operation::AddMultipleToStore => (32..).into(),
            Operation::AddBuildLog => (32..).into(),
            Operation::BuildPathsWithResults => (34..).into(),
            Operation::AddPermRoot => (36..).into(),
        }
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
    NixDeserialize,
    NixSerialize,
)]
#[nix(try_from = "u16", into = "u16")]
#[repr(u16)]
pub enum BuildMode {
    Normal = 0,
    Repair = 1,
    Check = 2,
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
    Default,
    NixDeserialize,
    NixSerialize,
)]
#[nix(try_from = "u16", into = "u16")]
#[repr(u16)]
pub enum GCAction {
    #[default]
    ReturnLive = 0,
    ReturnDead = 1,
    DeleteDead = 2,
    DeleteSpecific = 3,
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
    NixDeserialize,
    NixSerialize,
)]
#[nix(try_from = "u16", into = "u16")]
#[repr(u16)]
pub enum BuildStatus {
    Built = 0,
    Substituted = 1,
    AlreadyValid = 2,
    PermanentFailure = 3,
    InputRejected = 4,
    OutputRejected = 5,
    TransientFailure = 6,
    CachedFailure = 7,
    TimedOut = 8,
    MiscFailure = 9,
    DependencyFailed = 10,
    LogLimitExceeded = 11,
    NotDeterministic = 12,
    ResolvesToAlreadyValid = 13,
    NoSubstituters = 14,
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
    NixDeserialize,
    NixSerialize,
)]
#[nix(try_from = "u64", into = "u64")]
#[repr(u64)]
pub enum TrustLevel {
    Unknown = 0,
    Trusted = 1,
    NotTrusted = 2,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, NixDeserialize, NixSerialize)]
pub struct ClientOptions {
    pub keep_failed: bool,
    pub keep_going: bool,
    pub try_fallback: bool,
    pub verbosity: Verbosity,
    pub max_build_jobs: DaemonInt,
    pub max_silent_time: DaemonTime,
    pub(crate) _use_build_hook: IgnoredTrue,
    pub verbose_build: Verbosity,
    pub(crate) _log_type: IgnoredZero,
    pub(crate) _print_build_trace: IgnoredZero,
    pub build_cores: DaemonInt,
    pub use_substitutes: bool,
    pub other_settings: BTreeMap<String, DaemonString>,
}

impl Default for ClientOptions {
    fn default() -> Self {
        Self {
            keep_failed: Default::default(),
            keep_going: Default::default(),
            try_fallback: Default::default(),
            verbosity: Default::default(),
            max_build_jobs: 1,
            max_silent_time: Default::default(),
            _use_build_hook: Default::default(),
            verbose_build: Default::default(),
            _log_type: Default::default(),
            _print_build_trace: Default::default(),
            build_cores: 1,
            use_substitutes: true,
            other_settings: Default::default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, NixDeserialize, NixSerialize)]
#[cfg_attr(any(test, feature = "test"), derive(Arbitrary))]
pub struct UnkeyedValidPathInfo {
    pub deriver: Option<StorePath>,
    pub nar_hash: NarHash,
    pub references: BTreeSet<StorePath>,
    pub registration_time: DaemonTime,
    pub nar_size: u64,
    pub ultimate: bool,
    #[cfg_attr(any(test, feature = "test"), strategy(arb_signatures()))]
    pub signatures: BTreeSet<Signature>,
    pub ca: Option<ContentAddress>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, NixDeserialize, NixSerialize)]
#[cfg_attr(any(test, feature = "test"), derive(Arbitrary))]
pub struct ValidPathInfo {
    pub path: StorePath,
    pub info: UnkeyedValidPathInfo,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, NixDeserialize, NixSerialize)]
pub struct UnkeyedSubstitutablePathInfo {
    pub deriver: Option<StorePath>,
    pub references: StorePathSet,
    pub download_size: u64,
    pub nar_size: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, NixDeserialize, NixSerialize)]
pub struct BuildResult {
    pub status: BuildStatus,
    pub error_msg: DaemonString,
    #[nix(version = "29..")]
    pub times_built: DaemonInt,
    #[nix(version = "29..")]
    pub is_non_deterministic: bool,
    #[nix(version = "29..")]
    pub start_time: DaemonTime,
    #[nix(version = "29..")]
    pub stop_time: DaemonTime,
    #[nix(version = "37..")]
    pub cpu_user: Option<Microseconds>,
    #[nix(version = "37..")]
    pub cpu_system: Option<Microseconds>,
    #[nix(version = "28..")]
    pub built_outputs: BTreeMap<DrvOutput, Realisation>,
}

pub type KeyedBuildResults = Vec<KeyedBuildResult>;
#[derive(Debug, Clone, PartialEq, Eq, Hash, NixDeserialize, NixSerialize)]
#[cfg_attr(any(test, feature = "test"), derive(Arbitrary))]
#[cfg_attr(any(test, feature = "test"), arbitrary(args = ProtocolVersion))]
pub struct KeyedBuildResult {
    pub path: DerivedPath,
    #[cfg_attr(any(test, feature = "test"), strategy(any_with::<BuildResult>(*args)))]
    pub result: BuildResult,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, NixDeserialize, NixSerialize)]
#[cfg_attr(any(test, feature = "test"), derive(Arbitrary))]
pub struct QueryMissingResult {
    pub will_build: StorePathSet,
    pub will_substitute: StorePathSet,
    pub unknown: StorePathSet,
    pub download_size: u64,
    pub nar_size: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Default, NixDeserialize, NixSerialize)]
pub struct CollectGarbageResponse {
    pub paths_deleted: Vec<DaemonString>,
    pub bytes_freed: u64,
    _obsolete: IgnoredZero,
}

pub type DaemonResult<T> = Result<T, DaemonError>;
pub trait DaemonResultExt<T> {
    fn with_operation(self, op: Operation) -> DaemonResult<T>;
    fn with_field(self, field: &'static str) -> DaemonResult<T>;
}
impl<T, E> DaemonResultExt<T> for Result<T, E>
where
    E: Into<DaemonError>,
{
    fn with_operation(self, op: Operation) -> DaemonResult<T> {
        self.map_err(|err| err.into().fill_operation(op))
    }

    fn with_field(self, field: &'static str) -> DaemonResult<T> {
        self.map_err(|err| {
            let mut err = err.into();
            err.context.fields.push(field);
            err
        })
    }
}

#[derive(Debug, Default, Clone)]
pub struct DaemonErrorContext {
    operation: Option<Operation>,
    fields: Vec<&'static str>,
}

impl fmt::Display for DaemonErrorContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(op) = self.operation.as_ref() {
            write!(f, "{op}")?;
            for field in self.fields.iter() {
                write!(f, ".{field}")?;
            }
        } else {
            let mut it = self.fields.iter();
            if let Some(field) = it.next() {
                f.write_str(field)?;
                for field in it {
                    write!(f, ".{field}")?;
                }
            }
        }
        Ok(())
    }
}

#[derive(Error, Debug, Clone)]
#[error("{context}: {kind}")]
pub struct DaemonError {
    context: DaemonErrorContext,
    kind: DaemonErrorKind,
}

impl DaemonError {
    pub fn custom<D: fmt::Display>(source: D) -> Self {
        DaemonErrorKind::Custom(source.to_string()).into()
    }
    pub fn unimplemented(op: Operation) -> Self {
        DaemonError {
            kind: DaemonErrorKind::UnimplementedOperation(op),
            context: DaemonErrorContext {
                operation: Some(op),
                ..Default::default()
            },
        }
    }
    pub fn fill_operation(mut self, op: Operation) -> Self {
        if self.context.operation.is_none() {
            self.context.operation = Some(op);
        }
        self
    }
    pub fn kind(&self) -> &DaemonErrorKind {
        &self.kind
    }

    pub fn operation(&self) -> Option<&Operation> {
        self.context.operation.as_ref()
    }

    pub fn fields(&self) -> &[&'static str] {
        &self.context.fields
    }
}

#[derive(Error, Debug)]
pub enum DaemonErrorKind {
    #[error("wrong magic 0x{0:x}")]
    WrongMagic(u64),
    #[error("unsupported version {0}")]
    UnsupportedVersion(ProtocolVersion),
    #[error("unimplemented operation '{0:?}'")]
    UnimplementedOperation(Operation),
    #[error("no source for logger write")]
    NoSinkForLoggerWrite,
    #[error("no sink for logger read")]
    NoSourceForLoggerRead,
    #[error("io error {0}")]
    IO(
        #[from]
        #[source]
        std::io::Error,
    ),
    #[error("remote error: {0}")]
    Remote(
        #[from]
        #[source]
        RemoteError,
    ),
    #[error("{0}")]
    Custom(String),
}

impl Clone for DaemonErrorKind {
    fn clone(&self) -> Self {
        match self {
            Self::WrongMagic(arg0) => Self::WrongMagic(*arg0),
            Self::UnsupportedVersion(arg0) => Self::UnsupportedVersion(*arg0),
            Self::UnimplementedOperation(arg0) => Self::UnimplementedOperation(*arg0),
            Self::NoSinkForLoggerWrite => Self::NoSinkForLoggerWrite,
            Self::NoSourceForLoggerRead => Self::NoSourceForLoggerRead,
            Self::IO(arg0) => Self::IO(std::io::Error::new(arg0.kind(), arg0.to_string())),
            Self::Remote(arg0) => Self::Remote(arg0.clone()),
            Self::Custom(arg0) => Self::Custom(arg0.clone()),
        }
    }
}

impl From<LogError> for DaemonError {
    fn from(value: LogError) -> Self {
        DaemonError {
            context: DaemonErrorContext::default(),
            kind: DaemonErrorKind::Remote(value.into()),
        }
    }
}

impl From<std::io::Error> for DaemonError {
    fn from(value: std::io::Error) -> Self {
        DaemonError {
            context: DaemonErrorContext::default(),
            kind: DaemonErrorKind::IO(value),
        }
    }
}

impl From<RemoteError> for DaemonError {
    fn from(value: RemoteError) -> Self {
        DaemonError {
            context: DaemonErrorContext::default(),
            kind: DaemonErrorKind::Remote(value),
        }
    }
}

impl From<DaemonErrorKind> for DaemonError {
    fn from(kind: DaemonErrorKind) -> Self {
        DaemonError {
            context: DaemonErrorContext::default(),
            kind,
        }
    }
}

#[derive(Clone, Error, Debug, PartialEq, Eq, Hash)]
#[error("{}", msg.as_bstr())]
pub struct RemoteError {
    pub level: Verbosity,
    pub msg: DaemonString,
    pub exit_status: DaemonInt,
    pub traces: Vec<TraceLine>,
}

pub struct AddToStoreItem<R> {
    pub info: ValidPathInfo,
    pub reader: R,
}

pub trait HasTrustLevel {
    fn trust_level(&self) -> TrustLevel;
}

pub trait HandshakeDaemonStore: HasStoreDir {
    type Store: DaemonStore + Send;
    fn handshake(self) -> impl ResultLog<Output = DaemonResult<Self::Store>> + Send;
}

#[allow(unused_variables)]
pub trait DaemonStore: HasStoreDir + HasTrustLevel + Send {
    /// Sets options on server.
    /// This is usually called by the client just after the handshake to set
    /// options for the rest of the session.
    fn set_options<'a>(
        &'a mut self,
        options: &'a ClientOptions,
    ) -> impl ResultLog<Output = DaemonResult<()>> + Send + 'a {
        ready(Err(DaemonError::unimplemented(Operation::SetOptions))).empty_logs()
    }

    fn is_valid_path<'a>(
        &'a mut self,
        path: &'a StorePath,
    ) -> impl ResultLog<Output = DaemonResult<bool>> + Send + 'a {
        ready(Err(DaemonError::unimplemented(Operation::IsValidPath))).empty_logs()
    }

    fn query_valid_paths<'a>(
        &'a mut self,
        paths: &'a StorePathSet,
        substitute: bool,
    ) -> impl ResultLog<Output = DaemonResult<StorePathSet>> + Send + 'a {
        ready(Err(DaemonError::unimplemented(Operation::QueryValidPaths))).empty_logs()
    }

    fn query_path_info<'a>(
        &'a mut self,
        path: &'a StorePath,
    ) -> impl ResultLog<Output = DaemonResult<Option<UnkeyedValidPathInfo>>> + Send + 'a {
        ready(Err(DaemonError::unimplemented(Operation::QueryPathInfo))).empty_logs()
    }

    fn nar_from_path<'s>(
        &'s mut self,
        path: &'s StorePath,
    ) -> impl ResultLog<Output = DaemonResult<impl AsyncBufRead + Send + use<Self>>> + Send + 's
    {
        ready(Err(DaemonError::unimplemented(Operation::NarFromPath)) as Result<&[u8], DaemonError>)
            .empty_logs()
    }

    fn build_paths<'a>(
        &'a mut self,
        drvs: &'a [DerivedPath],
        mode: BuildMode,
    ) -> impl ResultLog<Output = DaemonResult<()>> + Send + 'a {
        ready(Err(DaemonError::unimplemented(Operation::BuildPaths))).empty_logs()
    }

    fn build_paths_with_results<'a>(
        &'a mut self,
        drvs: &'a [DerivedPath],
        mode: BuildMode,
    ) -> impl ResultLog<Output = DaemonResult<Vec<KeyedBuildResult>>> + Send + 'a {
        ready(Err(DaemonError::unimplemented(
            Operation::BuildPathsWithResults,
        )))
        .empty_logs()
    }

    fn build_derivation<'a>(
        &'a mut self,
        drv: &'a BasicDerivation,
        mode: BuildMode,
    ) -> impl ResultLog<Output = DaemonResult<BuildResult>> + Send + 'a {
        ready(Err(DaemonError::unimplemented(Operation::BuildDerivation))).empty_logs()
    }

    fn query_missing<'a>(
        &'a mut self,
        paths: &'a [DerivedPath],
    ) -> impl ResultLog<Output = DaemonResult<QueryMissingResult>> + Send + 'a {
        ready(Err(DaemonError::unimplemented(Operation::QueryMissing))).empty_logs()
    }

    fn add_to_store_nar<'s, 'r, 'i, R>(
        &'s mut self,
        info: &'i ValidPathInfo,
        source: R,
        repair: bool,
        dont_check_sigs: bool,
    ) -> Pin<Box<dyn ResultLog<Output = DaemonResult<()>> + Send + 'r>>
    where
        R: AsyncBufRead + Send + Unpin + 'r,
        's: 'r,
        'i: 'r,
    {
        ready(Err(DaemonError::unimplemented(Operation::AddToStoreNar)))
            .empty_logs()
            .boxed_result()
    }

    fn add_multiple_to_store<'s, 'i, 'r, S, R>(
        &'s mut self,
        repair: bool,
        dont_check_sigs: bool,
        stream: S,
    ) -> Pin<Box<dyn ResultLog<Output = DaemonResult<()>> + Send + 'r>>
    where
        S: Stream<Item = Result<AddToStoreItem<R>, DaemonError>> + Send + 'i,
        R: AsyncBufRead + Send + Unpin + 'i,
        's: 'r,
        'i: 'r,
    {
        ready(Err(DaemonError::unimplemented(
            Operation::AddMultipleToStore,
        )))
        .empty_logs()
        .boxed_result()
    }

    fn query_all_valid_paths(
        &mut self,
    ) -> impl ResultLog<Output = DaemonResult<StorePathSet>> + Send + '_ {
        ready(Err(DaemonError::unimplemented(
            Operation::QueryAllValidPaths,
        )))
        .empty_logs()
    }

    fn query_referrers<'a>(
        &'a mut self,
        path: &'a StorePath,
    ) -> impl ResultLog<Output = DaemonResult<StorePathSet>> + Send + 'a {
        ready(Err(DaemonError::unimplemented(Operation::QueryReferrers))).empty_logs()
    }

    fn ensure_path<'a>(
        &'a mut self,
        path: &'a StorePath,
    ) -> impl ResultLog<Output = DaemonResult<()>> + Send + 'a {
        ready(Err(DaemonError::unimplemented(Operation::EnsurePath))).empty_logs()
    }

    fn add_temp_root<'a>(
        &'a mut self,
        path: &'a StorePath,
    ) -> impl ResultLog<Output = DaemonResult<()>> + Send + 'a {
        ready(Err(DaemonError::unimplemented(Operation::AddTempRoot))).empty_logs()
    }

    fn add_indirect_root<'a>(
        &'a mut self,
        path: &'a DaemonPath,
    ) -> impl ResultLog<Output = DaemonResult<()>> + Send + 'a {
        ready(Err(DaemonError::unimplemented(Operation::AddIndirectRoot))).empty_logs()
    }

    fn find_roots(
        &mut self,
    ) -> impl ResultLog<Output = DaemonResult<BTreeMap<DaemonPath, StorePath>>> + Send + '_ {
        ready(Err(DaemonError::unimplemented(Operation::FindRoots))).empty_logs()
    }

    fn collect_garbage<'a>(
        &'a mut self,
        action: GCAction,
        paths_to_delete: &'a StorePathSet,
        ignore_liveness: bool,
        max_freed: u64,
    ) -> impl ResultLog<Output = DaemonResult<CollectGarbageResponse>> + Send + 'a {
        ready(Err(DaemonError::unimplemented(Operation::CollectGarbage))).empty_logs()
    }

    fn query_path_from_hash_part<'a>(
        &'a mut self,
        hash: &'a StorePathHash,
    ) -> impl ResultLog<Output = DaemonResult<Option<StorePath>>> + Send + 'a {
        ready(Err(DaemonError::unimplemented(
            Operation::QueryPathFromHashPart,
        )))
        .empty_logs()
    }

    fn query_substitutable_paths<'a>(
        &'a mut self,
        paths: &'a StorePathSet,
    ) -> impl ResultLog<Output = DaemonResult<StorePathSet>> + Send + 'a {
        ready(Err(DaemonError::unimplemented(
            Operation::QuerySubstitutablePaths,
        )))
        .empty_logs()
    }

    fn query_valid_derivers<'a>(
        &'a mut self,
        path: &'a StorePath,
    ) -> impl ResultLog<Output = DaemonResult<StorePathSet>> + Send + 'a {
        ready(Err(DaemonError::unimplemented(
            Operation::QueryValidDerivers,
        )))
        .empty_logs()
    }

    fn optimise_store(&mut self) -> impl ResultLog<Output = DaemonResult<()>> + Send + '_ {
        ready(Err(DaemonError::unimplemented(Operation::OptimiseStore))).empty_logs()
    }

    fn verify_store(
        &mut self,
        check_contents: bool,
        repair: bool,
    ) -> impl ResultLog<Output = DaemonResult<bool>> + Send + '_ {
        ready(Err(DaemonError::unimplemented(Operation::VerifyStore))).empty_logs()
    }

    fn add_signatures<'a>(
        &'a mut self,
        path: &'a StorePath,
        signatures: &'a [Signature],
    ) -> impl ResultLog<Output = DaemonResult<()>> + Send + 'a {
        ready(Err(DaemonError::unimplemented(Operation::AddSignatures))).empty_logs()
    }

    fn query_derivation_output_map<'a>(
        &'a mut self,
        path: &'a StorePath,
    ) -> impl ResultLog<Output = DaemonResult<BTreeMap<OutputName, Option<StorePath>>>> + Send + 'a
    {
        ready(Err(DaemonError::unimplemented(
            Operation::QueryDerivationOutputMap,
        )))
        .empty_logs()
    }

    fn register_drv_output<'a>(
        &'a mut self,
        realisation: &'a Realisation,
    ) -> impl ResultLog<Output = DaemonResult<()>> + Send + 'a {
        ready(Err(DaemonError::unimplemented(
            Operation::RegisterDrvOutput,
        )))
        .empty_logs()
    }

    fn query_realisation<'a>(
        &'a mut self,
        output_id: &'a DrvOutput,
    ) -> impl ResultLog<Output = DaemonResult<Option<Realisation>>> + Send + 'a {
        ready(Err(DaemonError::unimplemented(Operation::QueryRealisation))).empty_logs()
    }

    fn add_build_log<'s, 'r, 'p, R>(
        &'s mut self,
        path: &'p StorePath,
        source: R,
    ) -> Pin<Box<dyn ResultLog<Output = DaemonResult<()>> + Send + 'r>>
    where
        R: AsyncBufRead + Send + Unpin + 'r,
        's: 'r,
        'p: 'r,
    {
        ready(Err(DaemonError::unimplemented(Operation::AddBuildLog)))
            .empty_logs()
            .boxed_result()
    }

    fn add_perm_root<'a>(
        &'a mut self,
        path: &'a StorePath,
        gc_root: &'a DaemonPath,
    ) -> impl ResultLog<Output = DaemonResult<DaemonPath>> + Send + 'a {
        ready(Err(DaemonError::unimplemented(Operation::AddPermRoot))).empty_logs()
    }

    fn sync_with_gc(&mut self) -> impl ResultLog<Output = DaemonResult<()>> + Send + '_ {
        ready(Err(DaemonError::unimplemented(Operation::SyncWithGC))).empty_logs()
    }

    fn query_derivation_outputs<'a>(
        &'a mut self,
        path: &'a StorePath,
    ) -> impl ResultLog<Output = DaemonResult<StorePathSet>> + Send + 'a {
        ready(Err(DaemonError::unimplemented(
            Operation::QueryDerivationOutputs,
        )))
        .empty_logs()
    }

    fn query_derivation_output_names<'a>(
        &'a mut self,
        path: &'a StorePath,
    ) -> impl ResultLog<Output = DaemonResult<BTreeSet<OutputName>>> + Send + 'a {
        ready(Err(DaemonError::unimplemented(
            Operation::QueryDerivationOutputNames,
        )))
        .empty_logs()
    }

    fn add_ca_to_store<'a, 'r, R>(
        &'a mut self,
        name: &'a str,
        cam: ContentAddressMethodAlgorithm,
        refs: &'a StorePathSet,
        repair: bool,
        source: R,
    ) -> Pin<Box<dyn ResultLog<Output = DaemonResult<ValidPathInfo>> + Send + 'r>>
    where
        R: AsyncBufRead + Send + Unpin + 'r,
        'a: 'r,
    {
        ready(Err(DaemonError::unimplemented(Operation::AddToStore)))
            .empty_logs()
            .boxed_result()
    }

    fn shutdown(&mut self) -> impl ResultLog<Output = DaemonResult<()>> + Send + '_;
}

#[forbid(clippy::missing_trait_methods)]
impl<S> HasTrustLevel for &mut S
where
    S: HasTrustLevel,
{
    fn trust_level(&self) -> TrustLevel {
        (**self).trust_level()
    }
}

#[forbid(clippy::missing_trait_methods)]
impl<'os, S> DaemonStore for &'os mut S
where
    S: DaemonStore,
{
    fn set_options<'a>(
        &'a mut self,
        options: &'a ClientOptions,
    ) -> impl ResultLog<Output = DaemonResult<()>> + Send + 'a {
        (**self).set_options(options)
    }

    fn is_valid_path<'a>(
        &'a mut self,
        path: &'a StorePath,
    ) -> impl ResultLog<Output = DaemonResult<bool>> + Send + 'a {
        (**self).is_valid_path(path)
    }

    fn query_valid_paths<'a>(
        &'a mut self,
        paths: &'a StorePathSet,
        substitute: bool,
    ) -> impl ResultLog<Output = DaemonResult<StorePathSet>> + Send + 'a {
        (**self).query_valid_paths(paths, substitute)
    }

    fn query_path_info<'a>(
        &'a mut self,
        path: &'a StorePath,
    ) -> impl ResultLog<Output = DaemonResult<Option<UnkeyedValidPathInfo>>> + Send + 'a {
        (**self).query_path_info(path)
    }

    fn nar_from_path<'s>(
        &'s mut self,
        path: &'s StorePath,
    ) -> impl ResultLog<Output = DaemonResult<impl AsyncBufRead + use<'os, S>>> + Send + 's {
        (**self).nar_from_path(path)
    }

    fn build_paths<'a>(
        &'a mut self,
        paths: &'a [DerivedPath],
        mode: BuildMode,
    ) -> impl ResultLog<Output = DaemonResult<()>> + Send + 'a {
        (**self).build_paths(paths, mode)
    }

    fn build_derivation<'a>(
        &'a mut self,
        drv: &'a BasicDerivation,
        mode: BuildMode,
    ) -> impl ResultLog<Output = DaemonResult<BuildResult>> + 'a {
        (**self).build_derivation(drv, mode)
    }

    fn query_missing<'a>(
        &'a mut self,
        paths: &'a [DerivedPath],
    ) -> impl ResultLog<Output = DaemonResult<QueryMissingResult>> + 'a {
        (**self).query_missing(paths)
    }

    fn add_to_store_nar<'s, 'r, 'i, R>(
        &'s mut self,
        info: &'i ValidPathInfo,
        source: R,
        repair: bool,
        dont_check_sigs: bool,
    ) -> Pin<Box<dyn ResultLog<Output = DaemonResult<()>> + Send + 'r>>
    where
        R: AsyncBufRead + Send + Unpin + 'r,
        's: 'r,
        'i: 'r,
    {
        (**self).add_to_store_nar(info, source, repair, dont_check_sigs)
    }

    fn add_multiple_to_store<'s, 'i, 'r, I, R>(
        &'s mut self,
        repair: bool,
        dont_check_sigs: bool,
        stream: I,
    ) -> Pin<Box<dyn ResultLog<Output = DaemonResult<()>> + Send + 'r>>
    where
        I: Stream<Item = Result<AddToStoreItem<R>, DaemonError>> + Send + 'i,
        R: AsyncBufRead + Send + Unpin + 'i,
        's: 'r,
        'i: 'r,
    {
        (**self).add_multiple_to_store(repair, dont_check_sigs, stream)
    }

    fn build_paths_with_results<'a>(
        &'a mut self,
        drvs: &'a [DerivedPath],
        mode: BuildMode,
    ) -> impl ResultLog<Output = DaemonResult<Vec<KeyedBuildResult>>> + Send + 'a {
        (**self).build_paths_with_results(drvs, mode)
    }

    fn query_all_valid_paths(
        &mut self,
    ) -> impl ResultLog<Output = DaemonResult<StorePathSet>> + Send + '_ {
        (**self).query_all_valid_paths()
    }

    fn query_referrers<'a>(
        &'a mut self,
        path: &'a StorePath,
    ) -> impl ResultLog<Output = DaemonResult<StorePathSet>> + Send + 'a {
        (**self).query_referrers(path)
    }

    fn ensure_path<'a>(
        &'a mut self,
        path: &'a StorePath,
    ) -> impl ResultLog<Output = DaemonResult<()>> + Send + 'a {
        (**self).ensure_path(path)
    }

    fn add_temp_root<'a>(
        &'a mut self,
        path: &'a StorePath,
    ) -> impl ResultLog<Output = DaemonResult<()>> + Send + 'a {
        (**self).add_temp_root(path)
    }

    fn add_indirect_root<'a>(
        &'a mut self,
        path: &'a DaemonPath,
    ) -> impl ResultLog<Output = DaemonResult<()>> + Send + 'a {
        (**self).add_indirect_root(path)
    }

    fn find_roots(
        &mut self,
    ) -> impl ResultLog<Output = DaemonResult<BTreeMap<DaemonPath, StorePath>>> + Send + '_ {
        (**self).find_roots()
    }

    fn collect_garbage<'a>(
        &'a mut self,
        action: GCAction,
        paths_to_delete: &'a StorePathSet,
        ignore_liveness: bool,
        max_freed: u64,
    ) -> impl ResultLog<Output = DaemonResult<CollectGarbageResponse>> + Send + 'a {
        (**self).collect_garbage(action, paths_to_delete, ignore_liveness, max_freed)
    }

    fn query_path_from_hash_part<'a>(
        &'a mut self,
        hash: &'a StorePathHash,
    ) -> impl ResultLog<Output = DaemonResult<Option<StorePath>>> + Send + 'a {
        (**self).query_path_from_hash_part(hash)
    }

    fn query_substitutable_paths<'a>(
        &'a mut self,
        paths: &'a StorePathSet,
    ) -> impl ResultLog<Output = DaemonResult<StorePathSet>> + Send + 'a {
        (**self).query_substitutable_paths(paths)
    }

    fn query_valid_derivers<'a>(
        &'a mut self,
        path: &'a StorePath,
    ) -> impl ResultLog<Output = DaemonResult<StorePathSet>> + Send + 'a {
        (**self).query_valid_derivers(path)
    }

    fn optimise_store(&mut self) -> impl ResultLog<Output = DaemonResult<()>> + Send + '_ {
        (**self).optimise_store()
    }

    fn verify_store(
        &mut self,
        check_contents: bool,
        repair: bool,
    ) -> impl ResultLog<Output = DaemonResult<bool>> + Send + '_ {
        (**self).verify_store(check_contents, repair)
    }

    fn add_signatures<'a>(
        &'a mut self,
        path: &'a StorePath,
        signatures: &'a [Signature],
    ) -> impl ResultLog<Output = DaemonResult<()>> + Send + 'a {
        (**self).add_signatures(path, signatures)
    }

    fn query_derivation_output_map<'a>(
        &'a mut self,
        path: &'a StorePath,
    ) -> impl ResultLog<Output = DaemonResult<BTreeMap<OutputName, Option<StorePath>>>> + Send + 'a
    {
        (**self).query_derivation_output_map(path)
    }

    fn register_drv_output<'a>(
        &'a mut self,
        realisation: &'a Realisation,
    ) -> impl ResultLog<Output = DaemonResult<()>> + Send + 'a {
        (**self).register_drv_output(realisation)
    }

    fn query_realisation<'a>(
        &'a mut self,
        output_id: &'a DrvOutput,
    ) -> impl ResultLog<Output = DaemonResult<Option<Realisation>>> + Send + 'a {
        (**self).query_realisation(output_id)
    }

    fn add_build_log<'s, 'r, 'p, R>(
        &'s mut self,
        path: &'p StorePath,
        source: R,
    ) -> Pin<Box<dyn ResultLog<Output = DaemonResult<()>> + Send + 'r>>
    where
        R: AsyncBufRead + Send + Unpin + 'r,
        's: 'r,
        'p: 'r,
    {
        (**self).add_build_log(path, source)
    }

    fn add_perm_root<'a>(
        &'a mut self,
        path: &'a StorePath,
        gc_root: &'a DaemonPath,
    ) -> impl ResultLog<Output = DaemonResult<DaemonPath>> + Send + 'a {
        (**self).add_perm_root(path, gc_root)
    }

    fn sync_with_gc(&mut self) -> impl ResultLog<Output = DaemonResult<()>> + Send + '_ {
        (**self).sync_with_gc()
    }

    fn query_derivation_outputs<'a>(
        &'a mut self,
        path: &'a StorePath,
    ) -> impl ResultLog<Output = DaemonResult<StorePathSet>> + Send + 'a {
        (**self).query_derivation_outputs(path)
    }

    fn query_derivation_output_names<'a>(
        &'a mut self,
        path: &'a StorePath,
    ) -> impl ResultLog<Output = DaemonResult<BTreeSet<OutputName>>> + Send + 'a {
        (**self).query_derivation_output_names(path)
    }

    fn add_ca_to_store<'a, 'r, R>(
        &'a mut self,
        name: &'a str,
        cam: ContentAddressMethodAlgorithm,
        refs: &'a StorePathSet,
        repair: bool,
        source: R,
    ) -> Pin<Box<dyn ResultLog<Output = DaemonResult<ValidPathInfo>> + Send + 'r>>
    where
        R: AsyncBufRead + Send + Unpin + 'r,
        'a: 'r,
    {
        (**self).add_ca_to_store(name, cam, refs, repair, source)
    }

    fn shutdown(&mut self) -> impl ResultLog<Output = DaemonResult<()>> + Send + '_ {
        (**self).shutdown()
    }
}
