use std::collections::BTreeMap;

use bstr::ByteSlice;
use bytes::Bytes;
#[cfg(feature = "nixrs-derive")]
use nixrs_derive::{NixDeserialize, NixSerialize};
use num_enum::{IntoPrimitive, TryFromPrimitive};
#[cfg(any(test, feature = "nixrs-derive"))]
use proptest_derive::Arbitrary;
use thiserror::Error;
use tokio::io::AsyncWrite;

use crate::store_path::{FromStoreDirStr, ParseStorePathError, StoreDirDisplay, StorePathSet};
use crate::{hash::NarHash, store_path::StorePath};

use super::logger::{LogError, LoggerResult, TraceLine, Verbosity};
use super::wire::types::Operation;
use super::wire::{IgnoredTrue, IgnoredZero};
use super::ProtocolVersion;

pub type Signature = String;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(any(test, feature = "test"), derive(Arbitrary))]
#[cfg_attr(feature = "nixrs-derive", derive(NixDeserialize, NixSerialize))]
#[cfg_attr(feature = "nixrs-derive", nix(from_store_dir_str, store_dir_display))]
pub struct ContentAddress(String);
impl FromStoreDirStr for ContentAddress {
    type Error = ParseStorePathError;

    fn from_store_dir_str(
        _store_dir: &crate::store_path::StoreDir,
        s: &str,
    ) -> Result<Self, Self::Error> {
        Ok(ContentAddress(s.to_owned()))
    }
}

impl StoreDirDisplay for ContentAddress {
    fn fmt(
        &self,
        _store_dir: &crate::store_path::StoreDir,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

pub type DaemonString = Bytes;
pub type DaemonPath = Bytes;
pub type DaemonInt = libc::c_uint;
pub type DaemonTime = libc::time_t;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
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
/*
impl Default for ClientOptions {
    fn default() -> Self {
        Self {
            keep_failed: Default::default(),
            keep_going: Default::default(),
            try_fallback: Default::default(),
            verbosity: Default::default(),
            max_build_jobs: Default::default(),
            max_silent_time: Default::default(),
            _use_build_hook: Default::default(),
            verbose_build: Default::default(),
            _log_type: Default::default(),
            _print_build_trace: Default::default(),
            build_cores: Default::default(),
            use_substributes: Default::default(),
            other_settings: Default::default()
        }
    }
}
*/

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
#[cfg_attr(feature = "nixrs-derive", nix(try_from = "u8", into = "u8"))]
#[repr(u8)]
pub enum TrustLevel {
    Unknown = 0,
    Trusted = 1,
    NotTrusted = 2,
}

pub type DaemonResult<T> = Result<T, DaemonError>;

#[derive(Error, Debug)]
pub enum DaemonError {
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

impl Clone for DaemonError {
    fn clone(&self) -> Self {
        match self {
            Self::WrongMagic(arg0) => Self::WrongMagic(arg0.clone()),
            Self::UnsupportedVersion(arg0) => Self::UnsupportedVersion(arg0.clone()),
            Self::UnimplementedOperation(arg0) => Self::UnimplementedOperation(arg0.clone()),
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
        DaemonError::Remote(value.into())
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

pub trait HandshakeDaemonStore {
    type Store: DaemonStore;
    fn handshake(self) -> impl LoggerResult<Self::Store, DaemonError>;
}

pub trait DaemonStore {
    fn trust_level(&self) -> TrustLevel;
    fn set_options<'a>(
        &'a mut self,
        options: &'a ClientOptions,
    ) -> impl LoggerResult<(), DaemonError> + 'a;
    fn is_valid_path<'a>(
        &'a mut self,
        path: &'a StorePath,
    ) -> impl LoggerResult<bool, DaemonError> + 'a;
    fn query_valid_paths<'a>(
        &'a mut self,
        paths: &'a StorePathSet,
        substitute: bool,
    ) -> impl LoggerResult<StorePathSet, DaemonError> + 'a;
    fn query_path_info<'a>(
        &'a mut self,
        path: &'a StorePath,
    ) -> impl LoggerResult<Option<UnkeyedValidPathInfo>, DaemonError> + 'a;
    fn nar_from_path<'a, W>(
        &'a mut self,
        path: &'a StorePath,
        sink: W,
    ) -> impl LoggerResult<(), DaemonError> + 'a
    where
        W: AsyncWrite + Unpin + 'a;
}

impl<'s, S> DaemonStore for &'s mut S
where
    S: DaemonStore,
{
    fn trust_level(&self) -> TrustLevel {
        (**self).trust_level()
    }

    fn set_options<'a>(
        &'a mut self,
        options: &'a ClientOptions,
    ) -> impl LoggerResult<(), DaemonError> + 'a {
        (**self).set_options(options)
    }

    fn is_valid_path<'a>(
        &'a mut self,
        path: &'a StorePath,
    ) -> impl LoggerResult<bool, DaemonError> + 'a {
        (**self).is_valid_path(path)
    }

    fn query_valid_paths<'a>(
        &'a mut self,
        paths: &'a StorePathSet,
        substitute: bool,
    ) -> impl LoggerResult<StorePathSet, DaemonError> + 'a {
        (**self).query_valid_paths(paths, substitute)
    }

    fn query_path_info<'a>(
        &'a mut self,
        path: &'a StorePath,
    ) -> impl LoggerResult<Option<UnkeyedValidPathInfo>, DaemonError> + 'a {
        (**self).query_path_info(path)
    }

    fn nar_from_path<'a, W>(
        &'a mut self,
        path: &'a StorePath,
        sink: W,
    ) -> impl LoggerResult<(), DaemonError> + 'a
    where
        W: AsyncWrite + Unpin + 'a,
    {
        (**self).nar_from_path(path, sink)
    }
}

#[cfg(any(test, feature = "test"))]
mod proptest {
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
