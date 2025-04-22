use std::collections::BTreeMap;
use std::fmt;
use std::future::ready;

use bstr::ByteSlice;
use bytes::Bytes;
use futures::stream::empty;
use futures::Stream;
#[cfg(feature = "nixrs-derive")]
use nixrs_derive::{NixDeserialize, NixSerialize};
use num_enum::{IntoPrimitive, TryFromPrimitive};
#[cfg(any(test, feature = "test"))]
use proptest_derive::Arbitrary;
use thiserror::Error;
use tokio::io::{AsyncBufRead, AsyncWrite};

use crate::derivation::BasicDerivation;
use crate::derived_path::DerivedPath;
use crate::signature::Signature;
use crate::store_path::{ContentAddress, StorePathSet};
use crate::{hash::NarHash, store_path::StorePath};

use super::logger::{LocalLoggerResult, LogError, ResultLog, ResultProcess, TraceLine, Verbosity};
use super::wire::types::Operation;
use super::wire::types2::{
    BuildMode, BuildResult, KeyedBuildResult, QueryMissingResult, ValidPathInfo,
};
use super::wire::{IgnoredTrue, IgnoredZero};
use super::ProtocolVersion;

pub type DaemonString = Bytes;
pub type DaemonPath = Bytes;
pub type DaemonInt = libc::c_uint;
pub type DaemonTime = libc::time_t;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "nixrs-derive", derive(NixDeserialize, NixSerialize))]
pub struct ClientOptions {
    pub keep_failed: bool,
    pub keep_going: bool,
    pub try_fallback: bool,
    pub verbosity: Verbosity,
    pub max_build_jobs: DaemonInt,
    pub max_silent_time: DaemonTime,
    _use_build_hook: IgnoredTrue,
    pub verbose_build: Verbosity,
    _log_type: IgnoredZero,
    _print_build_trace: IgnoredZero,
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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(any(test, feature = "test"), derive(Arbitrary))]
#[cfg_attr(feature = "nixrs-derive", derive(NixDeserialize, NixSerialize))]
pub struct UnkeyedValidPathInfo {
    pub deriver: Option<StorePath>,
    pub nar_hash: NarHash,
    pub references: Vec<StorePath>,
    pub registration_time: DaemonTime,
    pub nar_size: u64,
    pub ultimate: bool,
    pub signatures: Vec<Signature>,
    pub ca: Option<ContentAddress>,
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, TryFromPrimitive, IntoPrimitive,
)]
#[cfg_attr(feature = "nixrs-derive", derive(NixDeserialize, NixSerialize))]
#[cfg_attr(feature = "nixrs-derive", nix(try_from = "u64", into = "u64"))]
#[repr(u64)]
pub enum TrustLevel {
    Unknown = 0,
    Trusted = 1,
    NotTrusted = 2,
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
            write!(f, "{}", op)?;
            for field in self.fields.iter() {
                write!(f, ".{}", field)?;
            }
        } else {
            let mut it = self.fields.iter();
            if let Some(field) = it.next() {
                f.write_str(field)?;
                for field in it {
                    write!(f, ".{}", field)?;
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

pub trait HandshakeDaemonStore {
    type Store: DaemonStore + Send;
    fn handshake(self) -> impl ResultLog<Output = DaemonResult<Self::Store>> + Send;
}

#[allow(unused_variables)]
pub trait DaemonStore: Send {
    fn trust_level(&self) -> TrustLevel;

    /// Sets options on server.
    /// This is usually called by the client just after the handshake to set
    /// options for the rest of the session.
    fn set_options<'a>(
        &'a mut self,
        options: &'a ClientOptions,
    ) -> impl ResultLog<Output = DaemonResult<()>> + Send + 'a {
        ResultProcess {
            stream: empty(),
            result: ready(
                Err(super::DaemonErrorKind::UnimplementedOperation(
                    super::wire::types::Operation::SetOptions,
                ))
                .with_operation(super::wire::types::Operation::SetOptions),
            ),
        }
    }

    fn is_valid_path<'a>(
        &'a mut self,
        path: &'a StorePath,
    ) -> impl ResultLog<Output = DaemonResult<bool>> + Send + 'a {
        ResultProcess {
            stream: empty(),
            result: ready(
                Err(super::DaemonErrorKind::UnimplementedOperation(
                    super::wire::types::Operation::IsValidPath,
                ))
                .with_operation(super::wire::types::Operation::IsValidPath),
            ),
        }
    }

    fn query_valid_paths<'a>(
        &'a mut self,
        paths: &'a StorePathSet,
        substitute: bool,
    ) -> impl ResultLog<Output = DaemonResult<StorePathSet>> + Send + 'a {
        ResultProcess {
            stream: empty(),
            result: ready(
                Err(super::DaemonErrorKind::UnimplementedOperation(
                    super::wire::types::Operation::QueryValidPaths,
                ))
                .with_operation(super::wire::types::Operation::QueryValidPaths),
            ),
        }
    }

    fn query_path_info<'a>(
        &'a mut self,
        path: &'a StorePath,
    ) -> impl ResultLog<Output = DaemonResult<Option<UnkeyedValidPathInfo>>> + Send + 'a {
        ResultProcess {
            stream: empty(),
            result: ready(
                Err(super::DaemonErrorKind::UnimplementedOperation(
                    super::wire::types::Operation::QueryPathInfo,
                ))
                .with_operation(super::wire::types::Operation::QueryPathInfo),
            ),
        }
    }

    fn nar_from_path<'s>(
        &'s mut self,
        path: &'s StorePath,
    ) -> impl ResultLog<Output = DaemonResult<impl AsyncBufRead + Send + 's>> + Send + 's {
        ResultProcess {
            stream: empty(),
            result: ready(
                Err(super::DaemonErrorKind::UnimplementedOperation(
                    super::wire::types::Operation::NarFromPath,
                ))
                .with_operation(super::wire::types::Operation::NarFromPath)
                    as Result<&[u8], DaemonError>,
            ),
        }
    }

    fn build_paths<'a>(
        &'a mut self,
        paths: &'a [DerivedPath],
        mode: BuildMode,
    ) -> impl ResultLog<Output = DaemonResult<()>> + Send + 'a {
        ResultProcess {
            stream: empty(),
            result: ready(
                Err(super::DaemonErrorKind::UnimplementedOperation(
                    super::wire::types::Operation::BuildPaths,
                ))
                .with_operation(super::wire::types::Operation::BuildPaths),
            ),
        }
    }
    fn build_paths_with_results<'a>(
        &'a mut self,
        drvs: &'a [DerivedPath],
        mode: BuildMode,
    ) -> impl ResultLog<Output = DaemonResult<Vec<KeyedBuildResult>>> + Send + 'a;

    fn build_derivation<'a>(
        &'a mut self,
        drv: &'a BasicDerivation,
        build_mode: BuildMode,
    ) -> impl ResultLog<Output = DaemonResult<BuildResult>> + Send + 'a {
        ResultProcess {
            stream: empty(),
            result: ready(
                Err(super::DaemonErrorKind::UnimplementedOperation(
                    super::wire::types::Operation::BuildDerivation,
                ))
                .with_operation(super::wire::types::Operation::BuildDerivation),
            ),
        }
    }

    fn query_missing<'a>(
        &'a mut self,
        paths: &'a [DerivedPath],
    ) -> impl ResultLog<Output = DaemonResult<QueryMissingResult>> + Send + 'a {
        ResultProcess {
            stream: empty(),
            result: ready(
                Err(super::DaemonErrorKind::UnimplementedOperation(
                    super::wire::types::Operation::QueryMissing,
                ))
                .with_operation(super::wire::types::Operation::QueryMissing),
            ),
        }
    }

    fn add_to_store_nar<'s, 'r, 'i, R>(
        &'s mut self,
        info: &'i ValidPathInfo,
        source: R,
        repair: bool,
        dont_check_sigs: bool,
    ) -> impl ResultLog<Output = DaemonResult<()>> + Send + 'r
    where
        R: AsyncBufRead + Send + Unpin + 'r,
        's: 'r,
        'i: 'r,
    {
        ResultProcess {
            stream: empty(),
            result: ready(
                Err(super::DaemonErrorKind::UnimplementedOperation(
                    super::wire::types::Operation::AddToStoreNar,
                ))
                .with_operation(super::wire::types::Operation::AddToStoreNar),
            ),
        }
    }

    fn add_multiple_to_store<'s, 'i, 'r, S, R>(
        &'s mut self,
        repair: bool,
        dont_check_sigs: bool,
        stream: S,
    ) -> impl ResultLog<Output = DaemonResult<()>> + Send + 'r
    where
        S: Stream<Item = Result<AddToStoreItem<R>, DaemonError>> + Send + 'i,
        R: AsyncBufRead + Send + Unpin + 'i,
        's: 'r,
        'i: 'r,
    {
        ResultProcess {
            stream: empty(),
            result: ready(
                Err(super::DaemonErrorKind::UnimplementedOperation(
                    super::wire::types::Operation::AddMultipleToStore,
                ))
                .with_operation(super::wire::types::Operation::AddMultipleToStore),
            ),
        }
    }

    fn query_all_valid_paths(
        &mut self,
    ) -> impl ResultLog<Output = DaemonResult<StorePathSet>> + Send + '_;
}

impl<S> DaemonStore for &mut S
where
    S: DaemonStore,
{
    fn trust_level(&self) -> TrustLevel {
        (**self).trust_level()
    }

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
    ) -> impl ResultLog<Output = DaemonResult<impl AsyncBufRead + 's>> + Send + 's {
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
        build_mode: BuildMode,
    ) -> impl ResultLog<Output = DaemonResult<BuildResult>> + 'a {
        (**self).build_derivation(drv, build_mode)
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
    ) -> impl ResultLog<Output = DaemonResult<()>> + 'r
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
    ) -> impl ResultLog<Output = DaemonResult<()>> + Send + 'r
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
}

pub trait LocalHandshakeDaemonStore {
    type Store: LocalDaemonStore + Send;
    fn handshake(self) -> impl LocalLoggerResult<Self::Store, DaemonError>;
}

pub trait LocalDaemonStore {
    fn trust_level(&self) -> TrustLevel;
    fn set_options<'a>(
        &'a mut self,
        options: &'a ClientOptions,
    ) -> impl LocalLoggerResult<(), DaemonError> + 'a;
    fn is_valid_path<'a>(
        &'a mut self,
        path: &'a StorePath,
    ) -> impl LocalLoggerResult<bool, DaemonError> + 'a;
    fn query_valid_paths<'a>(
        &'a mut self,
        paths: &'a StorePathSet,
        substitute: bool,
    ) -> impl LocalLoggerResult<StorePathSet, DaemonError> + 'a;
    fn query_path_info<'a>(
        &'a mut self,
        path: &'a StorePath,
    ) -> impl LocalLoggerResult<Option<UnkeyedValidPathInfo>, DaemonError> + 'a;
    fn nar_from_path<'s, 'p, 'r, W>(
        &'s mut self,
        path: &'p StorePath,
        sink: W,
    ) -> impl LocalLoggerResult<(), DaemonError> + 'r
    where
        W: AsyncWrite + Unpin + 'r,
        's: 'r,
        'p: 'r;
}

impl<S> LocalDaemonStore for &mut S
where
    S: LocalDaemonStore,
{
    fn trust_level(&self) -> TrustLevel {
        (**self).trust_level()
    }

    fn set_options<'a>(
        &'a mut self,
        options: &'a ClientOptions,
    ) -> impl LocalLoggerResult<(), DaemonError> + 'a {
        (**self).set_options(options)
    }

    fn is_valid_path<'a>(
        &'a mut self,
        path: &'a StorePath,
    ) -> impl LocalLoggerResult<bool, DaemonError> + 'a {
        (**self).is_valid_path(path)
    }

    fn query_valid_paths<'a>(
        &'a mut self,
        paths: &'a StorePathSet,
        substitute: bool,
    ) -> impl LocalLoggerResult<StorePathSet, DaemonError> + 'a {
        (**self).query_valid_paths(paths, substitute)
    }

    fn query_path_info<'a>(
        &'a mut self,
        path: &'a StorePath,
    ) -> impl LocalLoggerResult<Option<UnkeyedValidPathInfo>, DaemonError> + 'a {
        (**self).query_path_info(path)
    }

    fn nar_from_path<'a, 'p, 'r, W>(
        &'a mut self,
        path: &'p StorePath,
        sink: W,
    ) -> impl LocalLoggerResult<(), DaemonError> + 'r
    where
        W: AsyncWrite + Unpin + 'r,
        'a: 'r,
        'p: 'r,
    {
        (**self).nar_from_path(path, sink)
    }
}

#[cfg(any(test, feature = "test"))]
mod proptests {
    use ::proptest::collection::btree_map;
    use ::proptest::prelude::*;
    use ::proptest::sample::SizeRange;

    use super::*;

    fn arb_client_settings(
        size: impl Into<SizeRange>,
    ) -> impl Strategy<Value = BTreeMap<String, DaemonString>> {
        let key = any::<String>();
        let value = any::<Vec<u8>>().prop_map(DaemonString::from);
        btree_map(key, value, size)
    }

    impl Arbitrary for ClientOptions {
        type Parameters = ();
        type Strategy = BoxedStrategy<Self>;

        fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
            (
                any::<bool>(),
                any::<bool>(),
                any::<bool>(),
                any::<Verbosity>(),
                any::<DaemonInt>(),
                any::<DaemonTime>(),
                any::<Verbosity>(),
                any::<DaemonInt>(),
                any::<bool>(),
                arb_client_settings(..30),
            )
                .prop_map(
                    |(
                        keep_failed,
                        keep_going,
                        try_fallback,
                        verbosity,
                        max_build_jobs,
                        max_silent_time,
                        verbose_build,
                        build_cores,
                        use_substitutes,
                        other_settings,
                    )| {
                        ClientOptions {
                            keep_failed,
                            keep_going,
                            try_fallback,
                            verbosity,
                            max_build_jobs,
                            max_silent_time,
                            verbose_build,
                            build_cores,
                            use_substitutes,
                            other_settings,
                            _use_build_hook: IgnoredTrue,
                            _log_type: IgnoredZero,
                            _print_build_trace: IgnoredZero,
                        }
                    },
                )
                .boxed()
        }
    }
}
