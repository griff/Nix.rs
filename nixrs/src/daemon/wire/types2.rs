#[cfg(feature = "nixrs-derive")]
use std::str::from_utf8;
use std::{collections::BTreeMap, str::FromStr};

use bytes::Bytes;
#[cfg(feature = "nixrs-derive")]
use nixrs_derive::{NixDeserialize, NixSerialize};
use num_enum::{IntoPrimitive, TryFromPrimitive};

use crate::daemon::{
    ClientOptions, DaemonInt, DaemonPath, DaemonString, DaemonTime, UnkeyedValidPathInfo,
};
use crate::hash;
use crate::store_path::{StorePath, StorePathHash, StorePathSet};

#[cfg(feature = "nixrs-derive")]
use crate::daemon::de::{Error as _, NixDeserialize, NixRead};
#[cfg(feature = "nixrs-derive")]
use crate::daemon::ser::{NixSerialize, NixWrite};

use super::types::Operation;
use super::IgnoredZero;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "nixrs-derive", derive(NixDeserialize, NixSerialize))]
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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "nixrs-derive", derive(NixDeserialize, NixSerialize))]
pub struct ContentAddressMethodWithAlgo(String);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "nixrs-derive", derive(NixDeserialize, NixSerialize))]
pub struct ContentAddress(String);
impl FromStr for ContentAddress {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(ContentAddress(s.to_owned()))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "nixrs-derive", derive(NixDeserialize, NixSerialize))]
pub struct BaseStorePath(pub StorePath);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "nixrs-derive", derive(NixDeserialize, NixSerialize))]
pub struct Signature(DaemonString);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "nixrs-derive", derive(NixDeserialize, NixSerialize))]
pub struct DerivedPath(String);

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "nixrs-derive", derive(NixDeserialize, NixSerialize))]
pub struct DrvOutput(String);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "nixrs-derive", derive(NixDeserialize, NixSerialize))]
pub struct Realisation(String);

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, TryFromPrimitive, IntoPrimitive,
)]
#[cfg_attr(feature = "nixrs-derive", derive(NixDeserialize, NixSerialize))]
#[cfg_attr(feature = "nixrs-derive", nix(try_from = "u8", into = "u8"))]
#[repr(u8)]
pub enum FileIngestionMethod {
    Flat = 0,
    Recursive = 1,
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, TryFromPrimitive, IntoPrimitive,
)]
#[cfg_attr(feature = "nixrs-derive", derive(NixDeserialize, NixSerialize))]
#[cfg_attr(feature = "nixrs-derive", nix(try_from = "u16", into = "u16"))]
#[repr(u16)]
pub enum BuildMode {
    Normal = 0,
    Repair = 1,
    Check = 2,
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, TryFromPrimitive, IntoPrimitive,
)]
#[cfg_attr(feature = "nixrs-derive", derive(NixDeserialize, NixSerialize))]
#[cfg_attr(feature = "nixrs-derive", nix(try_from = "u16", into = "u16"))]
#[repr(u16)]
pub enum GCAction {
    ReturnLive = 0,
    ReturnDead = 1,
    DeleteDead = 2,
    DeleteSpecific = 3,
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, TryFromPrimitive, IntoPrimitive,
)]
#[cfg_attr(feature = "nixrs-derive", derive(NixDeserialize, NixSerialize))]
#[cfg_attr(feature = "nixrs-derive", nix(try_from = "u16", into = "u16"))]
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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "nixrs-derive", derive(NixDeserialize, NixSerialize))]
#[cfg_attr(feature = "nixrs-derive", nix(tag = "Operation"))]
pub enum Request {
    IsValidPath(StorePath),
    QueryReferrers(StorePath),
    AddToStore(AddToStoreRequest),
    BuildPaths(BuildPathsRequest),
    EnsurePath(StorePath),
    AddTempRoot(StorePath),
    AddIndirectRoot(DaemonPath),
    FindRoots,
    SetOptions(ClientOptions),
    CollectGarbage(CollectGarbageRequest),
    QueryAllValidPaths,
    QueryPathInfo(StorePath),
    QueryPathFromHashPart(StorePathHash),
    QueryValidPaths(QueryValidPathsRequest),
    QuerySubstitutablePaths(StorePathSet),
    QueryValidDerivers(StorePath),
    OptimiseStore,
    VerifyStore(VerifyStoreRequest),
    BuildDerivation(BuildDerivationRequest),
    AddSignatures(AddSignaturesRequest),
    NarFromPath(StorePath),
    AddToStoreNar(AddToStoreNarRequest),
    QueryMissing(Vec<DerivedPath>),
    QueryDerivationOutputMap(StorePath),
    RegisterDrvOutput(RegisterDrvOutputRequest),
    QueryRealisation(DrvOutput),
    AddMultipleToStore(AddMultipleToStoreRequest),
    AddBuildLog(BaseStorePath),
    BuildPathsWithResults(BuildPathsWithResultsRequest),
    AddPermRoot(AddPermRootRequest),

    /// Obsolete Nix 2.5.0 Protocol 1.32
    SyncWithGC,
    /// Obsolete Nix 2.4 Protocol 1.25
    AddTextToStore(AddTextToStoreRequest),
    /// Obsolete Nix 2.4 Protocol 1.22*
    QueryDerivationOutputs(StorePath),
    /// Obsolete Nix 2.4 Protocol 1.21
    QueryDerivationOutputNames(StorePath),
    /// Obsolete Nix 2.0, Protocol 1.19*
    QuerySubstitutablePathInfos(QuerySubstitutablePathInfosRequest),
    /// Obsolete Nix 2.0 Protocol 1.17
    ExportPath(StorePath),
    /// Obsolete Nix 2.0 Protocol 1.17
    ImportPaths,
    /// Obsolete Nix 2.0 Protocol 1.16
    QueryPathHash(StorePath),
    /// Obsolete Nix 2.0 Protocol 1.16
    QueryReferences(StorePath),
    /// Obsolete Nix 2.0 Protocol 1.16
    QueryDeriver(StorePath),
    /// Obsolete Nix 1.2 Protocol 1.12
    HasSubstitutes(StorePathSet),
    /// Obsolete Nix 1.2 Protocol 1.12
    QuerySubstitutablePathInfo(StorePath),
}

impl Request {
    pub fn operation(&self) -> Operation {
        match self {
            Request::IsValidPath(_) => Operation::IsValidPath,
            Request::QueryReferrers(_) => Operation::QueryReferences,
            Request::AddToStore(_) => Operation::AddToStore,
            Request::BuildPaths(_) => Operation::BuildPaths,
            Request::EnsurePath(_) => Operation::EnsurePath,
            Request::AddTempRoot(_) => Operation::AddTempRoot,
            Request::AddIndirectRoot(_) => Operation::AddIndirectRoot,
            Request::FindRoots => Operation::FindRoots,
            Request::SetOptions(_) => Operation::SetOptions,
            Request::CollectGarbage(_) => Operation::CollectGarbage,
            Request::QueryAllValidPaths => Operation::QueryAllValidPaths,
            Request::QueryPathInfo(_) => Operation::QueryPathInfo,
            Request::QueryPathFromHashPart(_) => Operation::QueryPathFromHashPart,
            Request::QueryValidPaths(_) => Operation::QueryValidPaths,
            Request::QuerySubstitutablePaths(_) => Operation::QuerySubstitutablePaths,
            Request::QueryValidDerivers(_) => Operation::QueryValidDerivers,
            Request::OptimiseStore => Operation::OptimiseStore,
            Request::VerifyStore(_) => Operation::VerifyStore,
            Request::BuildDerivation(_) => Operation::BuildDerivation,
            Request::AddSignatures(_) => Operation::AddSignatures,
            Request::NarFromPath(_) => Operation::NarFromPath,
            Request::AddToStoreNar(_) => Operation::AddToStoreNar,
            Request::QueryMissing(_) => Operation::QueryMissing,
            Request::QueryDerivationOutputMap(_) => Operation::QueryDerivationOutputMap,
            Request::RegisterDrvOutput(_) => Operation::RegisterDrvOutput,
            Request::QueryRealisation(_) => Operation::QueryRealisation,
            Request::AddMultipleToStore(_) => Operation::AddMultipleToStore,
            Request::AddBuildLog(_) => Operation::AddBuildLog,
            Request::BuildPathsWithResults(_) => Operation::BuildPathsWithResults,
            Request::AddPermRoot(_) => Operation::AddPermRoot,
            Request::SyncWithGC => Operation::SyncWithGC,
            Request::AddTextToStore(_) => Operation::AddTextToStore,
            Request::QueryDerivationOutputs(_) => Operation::QueryDerivationOutputs,
            Request::QueryDerivationOutputNames(_) => Operation::QueryDerivationOutputNames,
            Request::QuerySubstitutablePathInfos(_) => Operation::QuerySubstitutablePathInfos,
            Request::ExportPath(_) => Operation::ExportPath,
            Request::ImportPaths => Operation::ImportPaths,
            Request::QueryPathHash(_) => Operation::QueryPathHash,
            Request::QueryReferences(_) => Operation::QueryReferences,
            Request::QueryDeriver(_) => Operation::QueryDeriver,
            Request::HasSubstitutes(_) => Operation::HasSubstitutes,
            Request::QuerySubstitutablePathInfo(_) => Operation::QuerySubstitutablePathInfo,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "nixrs-derive", derive(NixDeserialize, NixSerialize))]
pub struct ValidPathInfo {
    pub path: StorePath,
    pub info: UnkeyedValidPathInfo,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "nixrs-derive", derive(NixDeserialize, NixSerialize))]
pub struct UnkeyedSubstitutablePathInfo {
    pub deriver: Option<StorePath>,
    pub references: StorePathSet,
    pub download_size: u64,
    pub nar_size: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "nixrs-derive", derive(NixDeserialize, NixSerialize))]
pub struct SubstitutablePathInfo {
    pub path: StorePath,
    pub info: UnkeyedSubstitutablePathInfo,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "nixrs-derive", derive(NixDeserialize, NixSerialize))]
pub struct DerivationOutput {
    pub path: DaemonString,
    pub hash_algo: DaemonString,
    pub hash: DaemonString,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "nixrs-derive", derive(NixDeserialize, NixSerialize))]
pub struct BasicDerivation {
    pub outputs: BTreeMap<String, DerivationOutput>,
    pub input_srcs: StorePathSet,
    pub platform: DaemonString,
    pub builder: DaemonString,
    pub args: Vec<DaemonString>,
    pub env: BTreeMap<DaemonString, DaemonString>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "nixrs-derive", derive(NixDeserialize, NixSerialize))]
pub struct BuildResult {
    pub status: BuildStatus,
    pub error_msg: DaemonString,
    #[cfg_attr(feature = "nixrs-derive", nix(version = "29.."))]
    pub times_built: DaemonInt,
    #[cfg_attr(feature = "nixrs-derive", nix(version = "29.."))]
    pub is_non_deterministic: bool,
    #[cfg_attr(feature = "nixrs-derive", nix(version = "29.."))]
    pub start_time: DaemonTime,
    #[cfg_attr(feature = "nixrs-derive", nix(version = "29.."))]
    pub stop_time: DaemonTime,
    #[cfg_attr(feature = "nixrs-derive", nix(version = "37.."))]
    pub cpu_user: Option<Microseconds>,
    #[cfg_attr(feature = "nixrs-derive", nix(version = "37.."))]
    pub cpu_system: Option<Microseconds>,
    #[cfg_attr(feature = "nixrs-derive", nix(version = "28.."))]
    pub built_outputs: BTreeMap<DrvOutput, Realisation>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "nixrs-derive", derive(NixDeserialize, NixSerialize))]
pub struct KeyedBuildResult {
    path: DerivedPath,
    result: BuildResult,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "nixrs-derive", derive(NixDeserialize, NixSerialize))]
pub enum AddToStoreRequest {
    #[cfg_attr(feature = "nixrs-derive", nix(version = "..=24"))]
    ProtocolPre25(AddToStoreRequestPre25),
    #[cfg_attr(feature = "nixrs-derive", nix(version = "25.."))]
    Protocol25(AddToStoreRequest25),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "nixrs-derive", derive(NixDeserialize, NixSerialize))]
pub struct AddToStoreRequestPre25 {
    pub base_name: String,
    pub fixed: bool,
    pub recursive: FileIngestionMethod,
    pub hash_algo: hash::Algorithm,
    // NAR dump
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "nixrs-derive", derive(NixDeserialize, NixSerialize))]
pub struct AddToStoreRequest25 {
    pub name: String,
    pub cam_str: ContentAddressMethodWithAlgo,
    pub refs: StorePathSet,
    pub repair: bool,
    // Framed NAR dump
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "nixrs-derive", derive(NixDeserialize, NixSerialize))]
pub struct AddTextToStoreRequest {
    pub suffix: String,
    pub text: Bytes,
    pub refs: StorePathSet,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "nixrs-derive", derive(NixDeserialize, NixSerialize))]
pub struct BuildPathsRequest {
    pub paths: Vec<DerivedPath>,
    pub mode: BuildMode,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "nixrs-derive", derive(NixDeserialize, NixSerialize))]
pub struct CollectGarbageRequest {
    pub action: GCAction,
    pub paths_to_delete: StorePathSet,
    pub ignore_liveness: bool,
    pub max_freed: u64,
    _removed1: IgnoredZero,
    _removed2: IgnoredZero,
    _removed3: IgnoredZero,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "nixrs-derive", derive(NixDeserialize, NixSerialize))]
pub struct CollectGarbageResponse {
    pub paths_deleted: Vec<DaemonString>,
    pub bytes_freed: u64,
    _obsolete: IgnoredZero,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "nixrs-derive", derive(NixDeserialize, NixSerialize))]
pub enum QuerySubstitutablePathInfosRequest {
    #[cfg_attr(feature = "nixrs-derive", nix(version = "..=21"))]
    ProtocolPre22(StorePathSet),
    #[cfg_attr(feature = "nixrs-derive", nix(version = "22.."))]
    Protocol22(BTreeMap<StorePath, Option<ContentAddress>>),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "nixrs-derive", derive(NixDeserialize, NixSerialize))]
pub struct QueryValidPathsRequest {
    pub paths: StorePathSet,
    #[cfg_attr(feature = "nixrs-derive", nix(version = "27.."))]
    pub substitute: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "nixrs-derive", derive(NixDeserialize, NixSerialize))]
pub struct VerifyStoreRequest {
    pub check_contents: bool,
    pub repair: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "nixrs-derive", derive(NixDeserialize, NixSerialize))]
pub struct BuildDerivationRequest {
    pub drv_path: StorePath,
    pub drv: BasicDerivation,
    pub build_mode: BuildMode,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "nixrs-derive", derive(NixDeserialize, NixSerialize))]
pub struct AddSignaturesRequest {
    pub path: StorePath,
    pub signatures: Vec<Signature>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "nixrs-derive", derive(NixDeserialize, NixSerialize))]
pub struct AddToStoreNarRequest {
    pub path_info: ValidPathInfo,
    pub repair: bool,
    pub dont_check_sigs: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "nixrs-derive", derive(NixDeserialize, NixSerialize))]
pub struct QueryMissingResult {
    will_build: StorePathSet,
    will_substitute: StorePathSet,
    unknown: StorePathSet,
    download_size: u64,
    nar_size: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "nixrs-derive", derive(NixDeserialize, NixSerialize))]
pub enum RegisterDrvOutputRequest {
    #[cfg_attr(feature = "nixrs-derive", nix(version = "31.."))]
    Post31(Realisation),
    #[cfg_attr(feature = "nixrs-derive", nix(version = "..=30"))]
    Pre31 {
        output_id: DrvOutput,
        output_path: StorePath,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "nixrs-derive", derive(NixDeserialize, NixSerialize))]
pub enum QueryRealisationResponse {
    #[cfg_attr(feature = "nixrs-derive", nix(version = "..=30"))]
    ProtocolPre31(StorePathSet),
    #[cfg_attr(feature = "nixrs-derive", nix(version = "31.."))]
    Protocol31(Vec<Realisation>),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "nixrs-derive", derive(NixDeserialize, NixSerialize))]
pub struct AddMultipleToStoreRequest {
    repair: bool,
    dont_check_sigs: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "nixrs-derive", derive(NixDeserialize, NixSerialize))]
pub struct BuildPathsWithResultsRequest {
    drvs: Vec<DerivedPath>,
    mode: BuildMode,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "nixrs-derive", derive(NixDeserialize, NixSerialize))]
pub struct AddPermRootRequest {
    store_path: StorePath,
    gc_root: DaemonString,
}

#[cfg(feature = "nixrs-derive")]
macro_rules! optional_info {
    ($sub:ty) => {
        impl NixDeserialize for Option<$sub> {
            async fn try_deserialize<R>(
                reader: &mut R,
            ) -> Result<Option<Self>, R::Error>
            where
                R: ?Sized + NixRead + Send,
            {
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
        impl NixSerialize for Option<$sub> {
            async fn serialize<W>(&self, writer: &mut W) -> Result<(), W::Error>
            where
                W: NixWrite,
            {
                if let Some(value) = self.as_ref() {
                    writer.write_value(&true).await?;
                    writer.write_value(value).await
                } else {
                    writer.write_value(&false).await
                }
            }
        }
    };
}
#[cfg(feature = "nixrs-derive")]
optional_info!(UnkeyedSubstitutablePathInfo);
#[cfg(feature = "nixrs-derive")]
optional_info!(UnkeyedValidPathInfo);

#[cfg(feature = "nixrs-derive")]
macro_rules! optional_string {
    ($sub:ty) => {
        impl NixDeserialize for Option<$sub> {
            async fn try_deserialize<R>(
                reader: &mut R,
            ) -> Result<Option<Self>, R::Error>
            where
                R: ?Sized + NixRead + Send,
            {
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
        impl NixSerialize for Option<$sub> {
            async fn serialize<W>(&self, writer: &mut W) -> Result<(), W::Error>
            where
                W: NixWrite,
            {
                if let Some(value) = self.as_ref() {
                    writer.write_value(value).await
                } else {
                    writer.write_slice(b"").await
                }
            }
        }
    };
}
#[cfg(feature = "nixrs-derive")]
optional_string!(ContentAddress);

#[cfg(feature = "nixrs-derive")]
impl NixDeserialize for Option<Microseconds> {
    async fn try_deserialize<R>(
        reader: &mut R,
    ) -> Result<Option<Self>, R::Error>
    where
        R: ?Sized + NixRead + Send,
    {
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

#[cfg(feature = "nixrs-derive")]
impl NixSerialize for Option<Microseconds> {
    async fn serialize<W>(&self, writer: &mut W) -> Result<(), W::Error>
    where
        W: NixWrite,
    {
        if let Some(value) = self.as_ref() {
            writer.write_number(1).await?;
            writer.write_value(value).await
        } else {
            writer.write_number(0).await
        }
    }
}
