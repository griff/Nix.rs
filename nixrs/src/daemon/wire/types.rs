use std::collections::BTreeMap;
use std::fmt;
use std::str::FromStr;
use std::str::from_utf8;

use bytes::Bytes;
use derive_more::Display;
use nixrs_derive::{NixDeserialize, NixSerialize};
use num_enum::{IntoPrimitive, TryFromPrimitive};
use tracing::{Span, debug_span};

use crate::daemon::UnkeyedValidPathInfo;
use crate::daemon::de::{Error as _, NixDeserialize, NixRead};
use crate::daemon::ser::{NixSerialize, NixWrite};
use crate::daemon::version::ProtocolRange;
use crate::daemon::wire::IgnoredZero;
use crate::daemon::{
    BuildMode, ClientOptions, DaemonPath, GCAction, Microseconds, UnkeyedSubstitutablePathInfo,
    ValidPathInfo,
};
use crate::derivation::BasicDerivation;
use crate::derived_path::DerivedPath;
use crate::hash;
use crate::realisation::{DrvOutput, Realisation};
use crate::signature::Signature;
use crate::store_path::{
    ContentAddress, ContentAddressMethodAlgorithm, StorePath, StorePathHash, StorePathSet,
};

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
pub enum FileIngestionMethod {
    Flat = 0,
    Recursive = 1,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, NixDeserialize, NixSerialize)]
#[nix(from_str, display)]
pub struct BaseStorePath(pub StorePath);
impl FromStr for BaseStorePath {
    type Err = crate::store_path::ParseStorePathError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(BaseStorePath(StorePath::from_str(s)?))
    }
}
impl fmt::Display for BaseStorePath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
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

#[derive(Debug, Clone, PartialEq, Eq, Hash, NixDeserialize, NixSerialize)]
#[nix(tag = "Operation")]
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
    BuildPathsWithResults(BuildPathsRequest),
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

    pub fn span(&self) -> Span {
        match self {
            Request::IsValidPath(path) => debug_span!("IsValidPath", ?path),
            Request::QueryReferrers(path) => debug_span!("QueryReferrers", ?path),
            Request::AddToStore(AddToStoreRequest::Protocol25(req)) => {
                debug_span!("AddToStore", name=?req.name, cam=?req.cam, refs=req.refs.len(), repair=req.repair)
            }
            Request::AddToStore(AddToStoreRequest::ProtocolPre25(req)) => {
                debug_span!("AddToStore", base_name=req.base_name, fixed=req.fixed,
                    recursive=?req.recursive,
                    hash_algo=?req.hash_algo)
            }
            Request::BuildPaths(req) => {
                debug_span!("BuildPaths", paths=req.paths.len(), mode=?req.mode)
            }
            Request::EnsurePath(path) => debug_span!("EnsurePath", ?path),
            Request::AddTempRoot(path) => debug_span!("AddTempRoot", ?path),
            Request::AddIndirectRoot(raw_path) => {
                let path = String::from_utf8_lossy(raw_path);
                debug_span!("AddIndirectRoot", ?path)
            }
            Request::FindRoots => debug_span!("FindRoots"),
            Request::SetOptions(_options) => debug_span!("SetOptions"),
            Request::CollectGarbage(req) => {
                debug_span!("CollectGarbage",
                    action=?req.action,
                    paths_to_delete=req.paths_to_delete.len(),
                    ignore_liveness=req.ignore_liveness,
                    max_freed=req.max_freed)
            }
            Request::QueryAllValidPaths => debug_span!("QueryAllValidPaths"),
            Request::QueryPathInfo(path) => debug_span!("QueryPathInfo", ?path),
            Request::QueryPathFromHashPart(hash) => debug_span!("QueryPathFromHashPart", ?hash),
            Request::QueryValidPaths(req) => {
                debug_span!(
                    "QueryValidPaths",
                    paths = req.paths.len(),
                    substitute = req.substitute
                )
            }
            Request::QuerySubstitutablePaths(paths) => {
                debug_span!("QuerySubstitutablePaths", paths = paths.len())
            }
            Request::QueryValidDerivers(path) => debug_span!("QueryValidDerivers", ?path),
            Request::OptimiseStore => debug_span!("OptimiseStore"),
            Request::VerifyStore(req) => {
                debug_span!(
                    "VerifyStore",
                    check_contents = req.check_contents,
                    repair = req.repair
                )
            }
            Request::BuildDerivation(req) => {
                debug_span!("BuildDerivation",
                    drv_path=?req.drv.drv_path,
                    mode=?req.mode)
            }
            Request::AddSignatures(req) => {
                debug_span!("AddSignatures", path=?req.path, signatures=?req.signatures)
            }
            Request::NarFromPath(path) => debug_span!("NarFromPath", ?path),
            Request::AddToStoreNar(req) => {
                let path = &req.path_info.path;
                let info = &req.path_info.info;
                debug_span!(
                    "AddToStoreNar",
                    ?path,
                    ?info,
                    repair = req.repair,
                    dont_check_sigs = req.dont_check_sigs
                )
            }
            Request::QueryMissing(paths) => debug_span!("QueryMissing", paths = paths.len()),
            Request::QueryDerivationOutputMap(path) => {
                debug_span!("QueryDerivationOutputMap", ?path)
            }
            Request::RegisterDrvOutput(RegisterDrvOutputRequest::Post31(realisation)) => {
                debug_span!("RegisterDrvOutput", ?realisation)
            }
            Request::RegisterDrvOutput(RegisterDrvOutputRequest::Pre31 {
                output_id,
                output_path,
            }) => {
                debug_span!("RegisterDrvOutput", ?output_id, ?output_path)
            }
            Request::QueryRealisation(drv_output) => {
                debug_span!("QueryRealisation", ?drv_output)
            }
            Request::AddMultipleToStore(req) => {
                debug_span!(
                    "AddMultipleToStore",
                    repair = req.repair,
                    dont_check_sigs = req.dont_check_sigs
                )
            }
            Request::AddBuildLog(path) => debug_span!("AddBuildLog", ?path),
            Request::BuildPathsWithResults(req) => {
                debug_span!("BuildPathsWithResults", paths=?req.paths.len(), mode=?req.mode)
            }
            Request::AddPermRoot(req) => {
                let gc_root = String::from_utf8_lossy(&req.gc_root);
                debug_span!("AddPermRoot", path=?req.store_path, ?gc_root)
            }
            Request::SyncWithGC => debug_span!("SyncWithGC"),
            Request::AddTextToStore(req) => {
                debug_span!(
                    "AddTextToStore",
                    suffix = req.suffix,
                    text = req.text.len(),
                    refs = req.refs.len()
                )
            }
            Request::QueryDerivationOutputs(path) => debug_span!("QueryDerivationOutputs", ?path),
            Request::QueryDerivationOutputNames(path) => {
                debug_span!("QueryDerivationOutputNames", ?path)
            }
            Request::QuerySubstitutablePathInfos(
                QuerySubstitutablePathInfosRequest::Protocol22(infos),
            ) => {
                debug_span!("QuerySubstitutablePathInfos", infos = infos.len())
            }
            Request::QuerySubstitutablePathInfos(
                QuerySubstitutablePathInfosRequest::ProtocolPre22(paths),
            ) => {
                debug_span!("QuerySubstitutablePathInfos", paths = paths.len())
            }
            Request::ExportPath(path) => debug_span!("ExportPath", ?path),
            Request::ImportPaths => debug_span!("ImportPaths"),
            Request::QueryPathHash(path) => debug_span!("QueryPathHash", ?path),
            Request::QueryReferences(path) => debug_span!("QueryReferences", ?path),
            Request::QueryDeriver(path) => debug_span!("QueryDeriver", ?path),
            Request::HasSubstitutes(paths) => debug_span!("HasSubstitutes", paths = paths.len()),
            Request::QuerySubstitutablePathInfo(path) => {
                debug_span!("QuerySubstitutablePathInfo", ?path)
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, NixDeserialize, NixSerialize)]
pub enum AddToStoreRequest {
    #[nix(version = "..=24")]
    ProtocolPre25(AddToStoreRequestPre25),
    #[nix(version = "25..")]
    Protocol25(AddToStoreRequest25),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, NixDeserialize, NixSerialize)]
pub struct AddToStoreRequestPre25 {
    pub base_name: String,
    pub fixed: bool,
    pub recursive: FileIngestionMethod,
    pub hash_algo: hash::Algorithm,
    // NAR dump
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, NixDeserialize, NixSerialize)]
pub struct AddToStoreRequest25 {
    pub name: String,
    pub cam: ContentAddressMethodAlgorithm,
    pub refs: StorePathSet,
    pub repair: bool,
    // Framed NAR dump
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, NixDeserialize, NixSerialize)]
pub struct AddTextToStoreRequest {
    pub suffix: String,
    pub text: Bytes,
    pub refs: StorePathSet,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, NixDeserialize, NixSerialize)]
pub struct BuildPathsRequest {
    pub paths: Vec<DerivedPath>,
    pub mode: BuildMode,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Default, NixDeserialize, NixSerialize)]
pub struct CollectGarbageRequest {
    pub action: GCAction,
    pub paths_to_delete: StorePathSet,
    pub ignore_liveness: bool,
    pub max_freed: u64,
    _removed1: IgnoredZero,
    _removed2: IgnoredZero,
    _removed3: IgnoredZero,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, NixDeserialize, NixSerialize)]
pub enum QuerySubstitutablePathInfosRequest {
    #[nix(version = "..=21")]
    ProtocolPre22(StorePathSet),
    #[nix(version = "22..")]
    Protocol22(BTreeMap<StorePath, Option<ContentAddress>>),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, NixDeserialize, NixSerialize)]
pub struct QueryValidPathsRequest {
    pub paths: StorePathSet,
    #[nix(version = "27..")]
    pub substitute: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, NixDeserialize, NixSerialize)]
pub struct VerifyStoreRequest {
    pub check_contents: bool,
    pub repair: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, NixDeserialize, NixSerialize)]
pub struct BuildDerivationRequest {
    pub drv: BasicDerivation,
    pub mode: BuildMode,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, NixDeserialize, NixSerialize)]
pub struct AddSignaturesRequest {
    pub path: StorePath,
    pub signatures: Vec<Signature>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, NixDeserialize, NixSerialize)]
pub struct AddToStoreNarRequest {
    pub path_info: ValidPathInfo,
    pub repair: bool,
    pub dont_check_sigs: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, NixDeserialize, NixSerialize)]
pub enum RegisterDrvOutputRequest {
    #[nix(version = "31..")]
    Post31(Realisation),
    #[nix(version = "..=30")]
    Pre31 {
        output_id: DrvOutput,
        output_path: StorePath,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, NixDeserialize, NixSerialize)]
pub enum QueryRealisationResponse {
    #[nix(version = "..=30")]
    ProtocolPre31(Vec<StorePath>),
    #[nix(version = "31..")]
    Protocol31(Vec<Realisation>),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, NixDeserialize, NixSerialize)]
pub struct AddMultipleToStoreRequest {
    pub repair: bool,
    pub dont_check_sigs: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, NixDeserialize, NixSerialize)]
pub struct AddPermRootRequest {
    pub store_path: StorePath,
    pub gc_root: DaemonPath,
}

macro_rules! optional_from_store_dir_str {
    ($sub:ty) => {
        impl NixDeserialize for Option<$sub> {
            async fn try_deserialize<R>(reader: &mut R) -> Result<Option<Self>, R::Error>
            where
                R: ?Sized + NixRead + Send,
            {
                use nixrs::daemon::de::Error;
                use nixrs::store_path::FromStoreDirStr;
                if let Some(buf) = reader.try_read_bytes().await? {
                    let s = ::std::str::from_utf8(&buf).map_err(Error::invalid_data)?;
                    if s == "" {
                        Ok(Some(None))
                    } else {
                        let dir = reader.store_dir();
                        <$sub as FromStoreDirStr>::from_store_dir_str(dir, s)
                            .map_err(Error::invalid_data)
                            .map(|v| Some(Some(v)))
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
optional_from_store_dir_str!(StorePath);

macro_rules! optional_info {
    ($sub:ty) => {
        impl NixDeserialize for Option<$sub> {
            async fn try_deserialize<R>(reader: &mut R) -> Result<Option<Self>, R::Error>
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
optional_info!(UnkeyedSubstitutablePathInfo);
optional_info!(UnkeyedValidPathInfo);

macro_rules! optional_from_str {
    ($sub:ty) => {
        impl NixDeserialize for Option<$sub> {
            async fn try_deserialize<R>(reader: &mut R) -> Result<Option<Self>, R::Error>
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
optional_from_str!(String);
optional_from_str!(ContentAddress);
optional_from_str!(ContentAddressMethodAlgorithm);

impl NixDeserialize for Option<Microseconds> {
    async fn try_deserialize<R>(reader: &mut R) -> Result<Option<Self>, R::Error>
    where
        R: ?Sized + NixRead + Send,
    {
        if let Some(tag) = reader.try_read_value::<u8>().await? {
            match tag {
                0 => Ok(Some(None)),
                1 => Ok(Some(Some(reader.read_value::<Microseconds>().await?))),
                _ => Err(R::Error::invalid_data("invalid optional tag from remote")),
            }
        } else {
            Ok(None)
        }
    }
}

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
