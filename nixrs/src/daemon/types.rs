use std::str::from_utf8;
use std::{collections::BTreeMap, str::FromStr};

use bytes::Bytes;
use nnixrs_derive::NixDeserialize;
use num_enum::TryFromPrimitive;

use crate::hash;
use crate::store_path::StorePath;

use super::de::{Error, NixDeserialize};

type NixInt = libc::c_uint;
type NixTime = libc::time_t;
// TODO: This is not true.
type CppString = String;

#[derive(NixDeserialize)]
pub struct Microseconds(i64);

impl From<i64> for Microseconds {
    fn from(value: i64) -> Self {
        Microseconds(value)
    }
}

impl From<Microseconds> for i64 {
    fn from(value: Microseconds) -> Self {
        value.0
    }
}

#[derive(NixDeserialize)]
pub struct ContentAddressMethodWithAlgo(String);

#[derive(NixDeserialize)]
pub struct ContentAddress(String);
impl FromStr for ContentAddress {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(ContentAddress(s.to_owned()))
    }
}

#[derive(NixDeserialize)]
pub struct BaseStorePath(StorePath);

#[derive(NixDeserialize)]
pub struct Signature(CppString);

#[derive(NixDeserialize)]
pub struct NarHash(String);

#[derive(NixDeserialize)]
pub struct DerivedPath(String);

#[derive(NixDeserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct DrvOutput(String);

#[derive(NixDeserialize)]
pub struct Realisation(String);

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, NixDeserialize, TryFromPrimitive,
)]
#[nix(try_from = "u8")]
#[repr(u8)]
pub enum FileIngestionMethod {
    Flat = 0,
    Recursive = 1,
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, NixDeserialize, TryFromPrimitive,
)]
#[nix(try_from = "u16")]
#[repr(u16)]
pub enum BuildMode {
    Normal = 0,
    Repair = 1,
    Check = 2,
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, NixDeserialize, TryFromPrimitive,
)]
#[nix(try_from = "u16")]
#[repr(u16)]
pub enum Verbosity {
    Error = 0,
    Warn = 1,
    Notice = 2,
    Info = 3,
    Talkative = 4,
    Chatty = 5,
    Debug = 6,
    Vomit = 7,
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, NixDeserialize, TryFromPrimitive,
)]
#[nix(try_from = "u16")]
#[repr(u16)]
pub enum GCAction {
    ReturnLive = 0,
    ReturnDead = 1,
    DeleteDead = 2,
    DeleteSpecific = 3,
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, NixDeserialize, TryFromPrimitive,
)]
#[nix(try_from = "u16")]
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

#[derive(NixDeserialize)]
pub struct UnkeyedValidPathInfo {
    pub deriver: Option<StorePath>,
    pub nar_hash: NarHash,
    pub references: Vec<StorePath>,
    pub registration_time: NixTime,
    pub nar_size: u64,
    pub ultimate: bool,
    pub signatures: Vec<Signature>,
    pub ca: Option<ContentAddress>,
}

#[derive(NixDeserialize)]
pub struct ValidPathInfo {
    pub path: StorePath,
    pub info: UnkeyedValidPathInfo,
}

#[derive(NixDeserialize)]
pub struct UnkeyedSubstitutablePathInfo {
    pub deriver: Option<StorePath>,
    pub references: Vec<StorePath>,
    pub download_size: u64,
    pub nar_size: u64,
}

#[derive(NixDeserialize)]
pub struct SubstitutablePathInfo {
    pub path: StorePath,
    pub info: UnkeyedSubstitutablePathInfo,
}

#[derive(NixDeserialize)]
pub struct DerivationOutput {
    pub path: CppString,
    pub hash_algo: CppString,
    pub hash: CppString,
}

#[derive(NixDeserialize)]
pub struct BasicDerivation {
    pub outputs: BTreeMap<String, DerivationOutput>,
    pub input_srcs: Vec<StorePath>,
    pub platform: CppString,
    pub builder: CppString,
    pub args: Vec<CppString>,
    pub env: BTreeMap<CppString, CppString>,
}

#[derive(NixDeserialize)]
pub struct BuildResult {
    pub status: BuildStatus,
    pub error_msg: CppString,
    #[nix(version = "29..")]
    pub times_built: NixInt,
    #[nix(version = "29..")]
    pub is_non_deterministic: bool,
    #[nix(version = "29..")]
    pub start_time: NixTime,
    #[nix(version = "29..")]
    pub stop_time: NixTime,
    #[nix(version = "37..")]
    pub cpu_user: Option<Microseconds>,
    #[nix(version = "37..")]
    pub cpu_system: Option<Microseconds>,
    #[nix(version = "28..")]
    pub built_outputs: BTreeMap<DrvOutput, Realisation>,
}

#[derive(NixDeserialize)]
pub struct KeyedBuildResult {
    path: DerivedPath,
    result: BuildResult,
}

#[derive(NixDeserialize)]
pub enum AddToStoreRequest {
    #[nix(version = "..=24")]
    ProtocolPre25(AddToStoreRequestPre25),
    #[nix(version = "25..")]
    Protocol25(AddToStoreRequest25),
}

#[derive(NixDeserialize)]
pub struct AddToStoreRequestPre25 {
    pub base_name: String,
    pub fixed: bool,
    pub recursive: FileIngestionMethod,
    pub hash_algo: hash::Algorithm,
    // NAR dump
}

#[derive(NixDeserialize)]
pub struct AddToStoreRequest25 {
    pub name: String,
    pub cam_str: ContentAddressMethodWithAlgo,
    pub refs: Vec<StorePath>,
    pub repair: bool,
    // Framed NAR dump
}

#[derive(NixDeserialize)]
pub struct AddTextToStoreRequest {
    pub suffix: String,
    pub text: Bytes,
    pub refs: Vec<StorePath>,
}

#[derive(NixDeserialize)]
pub struct BuildPathsRequest {
    pub paths: Vec<DerivedPath>,
    pub mode: BuildMode,
}

#[derive(NixDeserialize)]
pub struct SetOptionsRequest {
    pub keep_failed: bool,
    pub keep_going: bool,
    pub try_fallback: bool,
    pub verbosity: Verbosity,
    pub max_build_jobs: NixInt,
    pub max_silent_time: NixTime,
    _use_build_hook: IgnoredTrue,
    pub verbose_build: Verbosity,
    _log_type: IgnoredZero,
    _print_build_trace: IgnoredZero,
    pub build_cores: NixInt,
    pub use_substributes: bool,
    pub other_settings: BTreeMap<String, String>,
}

#[derive(NixDeserialize)]
pub struct CollectGarbageRequest {
    pub action: GCAction,
    pub paths_to_delete: Vec<StorePath>,
    pub ignore_liveness: bool,
    pub max_freed: u64,
    _removed1: IgnoredZero,
    _removed2: IgnoredZero,
    _removed3: IgnoredZero,
}

#[derive(NixDeserialize)]
pub struct CollectGarbageResponse {
    pub paths_deleted: Vec<CppString>,
    pub bytes_freed: u64,
    _obsolete: IgnoredZero,
}

#[derive(NixDeserialize)]
pub enum QuerySubstitutablePathInfosRequest {
    #[nix(version = "..=21")]
    ProtocolPre22(Vec<StorePath>),
    #[nix(version = "22..")]
    Protocol22(BTreeMap<StorePath, Option<ContentAddress>>),
}

#[derive(NixDeserialize)]
pub struct QueryValidPathsRequest {
    pub paths: Vec<StorePath>,
    #[nix(version = "27..")]
    pub substitute: bool,
}

#[derive(NixDeserialize)]
pub struct VerifyStoreRequest {
    pub check_contents: bool,
    pub repair: bool,
}

#[derive(NixDeserialize)]
pub struct BuildDerivationRequest {
    pub drv_path: StorePath,
    pub drv: BasicDerivation,
    pub build_mode: BuildMode,
}

#[derive(NixDeserialize)]
pub struct AddSignaturesRequest {
    pub path: StorePath,
    pub signatures: Vec<Signature>,
}

#[derive(NixDeserialize)]
pub struct AddToStoreNarRequests {
    pub path: StorePath,
    pub deriver: Option<StorePath>,
    pub nar_hash: NarHash,
    pub references: Vec<StorePath>,
    pub registration_time: NixTime,
    pub nar_size: u64,
    pub ultimate: bool,
    pub signatures: Vec<Signature>,
    pub ca: Option<ContentAddress>,
    pub repair: bool,
    pub dont_check_sigs: bool,
}

#[derive(NixDeserialize)]
pub struct QueryMissingResponse {
    will_build: Vec<StorePath>,
    will_substitute: Vec<StorePath>,
    unknown: Vec<StorePath>,
    download_size: u64,
    nar_size: u64,
}

#[derive(NixDeserialize)]
pub enum QueryRealisationResponse {
    #[nix(version = "..=30")]
    ProtocolPre31(Vec<StorePath>),
    #[nix(version = "31..")]
    Protocol31(Vec<Realisation>),
}

#[derive(NixDeserialize)]
pub struct AddMultipleToStoreRequest {
    repair: bool,
    dont_check_sigs: bool,
}

#[derive(NixDeserialize)]
pub struct BuildPathsWithResultsRequest {
    drvs: Vec<DerivedPath>,
    mode: BuildMode,
}

#[derive(NixDeserialize)]
pub struct AddPermRootRequest {
    store_path: StorePath,
    gc_root: CppString,
}

macro_rules! optional_info {
    ($sub:ty) => {
        impl NixDeserialize for Option<$sub> {
            fn try_deserialize<R>(
                reader: &mut R,
            ) -> impl std::future::Future<Output = Result<Option<Self>, R::Error>> + Send + '_
            where
                R: ?Sized + super::de::NixRead + Send,
            {
                async move {
                    if let Some(found) = reader.try_read_value::<bool>().await? {
                        if found {
                            Ok(Some(Some(reader.read_value().await?)))
                        } else {
                            Ok(Some(None))
                        }
                    } else {
                        Ok(None)
                    }
                }
            }
        }
    };
}
optional_info!(UnkeyedSubstitutablePathInfo);
optional_info!(UnkeyedValidPathInfo);

macro_rules! optional_string {
    ($sub:ty) => {
        impl NixDeserialize for Option<$sub> {
            fn try_deserialize<R>(
                reader: &mut R,
            ) -> impl std::future::Future<Output = Result<Option<Self>, R::Error>> + Send + '_
            where
                R: ?Sized + super::de::NixRead + Send,
            {
                async move {
                    if let Some(buf) = reader.try_read_bytes().await? {
                        let s = from_utf8(&buf).map_err(R::Error::invalid_data)?;
                        if s == "" {
                            Ok(Some(None))
                        } else {
                            Ok(Some(Some(s.parse().map_err(R::Error::invalid_data)?)))
                        }    
                    } else {
                        Ok(None)
                    }
                }
            }
        }
    };
}
optional_string!(StorePath);
optional_string!(ContentAddress);

impl NixDeserialize for Option<Microseconds> {
    fn try_deserialize<R>(
        reader: &mut R,
    ) -> impl std::future::Future<Output = Result<Option<Self>, R::Error>> + Send + '_
    where
        R: ?Sized + super::de::NixRead + Send,
    {
        async move {
            if let Some(tag) = reader.try_read_value::<u8>().await? {
                match tag {
                    0 => Ok(None),
                    1 => Ok(Some(reader.read_value().await?)),
                    _ => Err(R::Error::invalid_data("invalid optional tag from remote")),
                }    
            } else {
                Ok(None)
            }
        }
    }
}

#[derive(NixDeserialize)]
#[nix(from = "u64")]
struct IgnoredZero;
impl From<u64> for IgnoredZero {
    fn from(_value: u64) -> Self {
        IgnoredZero
    }
}

#[derive(NixDeserialize)]
#[nix(from = "bool")]
struct IgnoredTrue;
impl From<bool> for IgnoredTrue {
    fn from(_value: bool) -> Self {
        IgnoredTrue
    }
}
