use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::future::{Future, ready};
use std::io::Cursor;
use std::mem::take;
use std::pin::{Pin, pin};
use std::task::Poll;
use std::{fmt, thread};

use arbitrary::MockOperationParams;
use bytes::Bytes;
use futures::Stream;
#[cfg(any(test, feature = "test"))]
use futures::StreamExt as _;
use futures::channel::mpsc;
use futures::future::Either;
use futures::stream::empty;
use futures::stream::{TryStreamExt, iter};
use pin_project_lite::pin_project;
#[cfg(any(test, feature = "test"))]
use proptest::prelude::*;
#[cfg(any(test, feature = "test"))]
use proptest::prop_assert_eq;
#[cfg(any(test, feature = "test"))]
use test_strategy::Arbitrary;
use tokio::io::{AsyncBufRead, AsyncReadExt as _};
use tracing::trace;

use super::logger::{FutureResult, ResultLogExt as _, ResultProcess};
use super::types::AddToStoreItem;
use super::wire::types::Operation;
use super::wire::types2::{
    AddMultipleToStoreRequest, AddPermRootRequest, AddSignaturesRequest, AddToStoreNarRequest,
    AddToStoreRequest25, BuildDerivationRequest, BuildMode, BuildPathsRequest, BuildResult,
    CollectGarbageRequest, CollectGarbageResponse, GCAction, KeyedBuildResults, QueryMissingResult,
    QueryValidPathsRequest, ValidPathInfo, VerifyStoreRequest,
};
use super::{
    ClientOptions, DaemonError, DaemonPath, DaemonResult, DaemonResultExt, DaemonStore,
    HandshakeDaemonStore, ResultLog, TrustLevel, UnkeyedValidPathInfo,
};
use crate::daemon::FutureResultExt;
use crate::derivation::BasicDerivation;
use crate::derived_path::{DerivedPath, OutputName};
use crate::log::{Activity, ActivityResult, LogMessage, Message, StopActivity};
#[cfg(any(test, feature = "test"))]
use crate::pretty_prop_assert_eq;
use crate::realisation::{DrvOutput, Realisation};
use crate::signature::Signature;
use crate::store_path::{ContentAddressMethodAlgorithm, StorePath, StorePathHash, StorePathSet};

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone)]
pub enum MockOperation {
    SetOptions(ClientOptions, DaemonResult<()>),
    IsValidPath(StorePath, DaemonResult<bool>),
    QueryValidPaths(QueryValidPathsRequest, DaemonResult<StorePathSet>),
    QueryPathInfo(StorePath, DaemonResult<Option<UnkeyedValidPathInfo>>),
    NarFromPath(StorePath, DaemonResult<Bytes>),
    BuildPaths(BuildPathsRequest, DaemonResult<()>),
    BuildPathsWithResults(BuildPathsRequest, DaemonResult<KeyedBuildResults>),
    BuildDerivation(BuildDerivationRequest, DaemonResult<BuildResult>),
    QueryMissing(Vec<DerivedPath>, DaemonResult<QueryMissingResult>),
    AddToStoreNar(AddToStoreNarRequest, Bytes, DaemonResult<()>),
    AddMultipleToStore(
        AddMultipleToStoreRequest,
        Vec<(ValidPathInfo, Bytes)>,
        DaemonResult<()>,
    ),
    QueryAllValidPaths(DaemonResult<StorePathSet>),
    QueryReferrers(StorePath, DaemonResult<StorePathSet>),
    AddToStore(AddToStoreRequest25, Bytes, DaemonResult<ValidPathInfo>),
    EnsurePath(StorePath, DaemonResult<()>),
    AddTempRoot(StorePath, DaemonResult<()>),
    AddIndirectRoot(DaemonPath, DaemonResult<()>),
    FindRoots(DaemonResult<BTreeMap<DaemonPath, StorePath>>),
    CollectGarbage(CollectGarbageRequest, DaemonResult<CollectGarbageResponse>),
    QueryPathFromHashPart(StorePathHash, DaemonResult<Option<StorePath>>),
    QuerySubstitutablePaths(StorePathSet, DaemonResult<StorePathSet>),
    QueryValidDerivers(StorePath, DaemonResult<StorePathSet>),
    OptimiseStore(DaemonResult<()>),
    VerifyStore(VerifyStoreRequest, DaemonResult<bool>),
    AddSignatures(AddSignaturesRequest, DaemonResult<()>),
    QueryDerivationOutputMap(
        StorePath,
        DaemonResult<BTreeMap<OutputName, Option<StorePath>>>,
    ),
    RegisterDrvOutput(Realisation, DaemonResult<()>),
    QueryRealisation(DrvOutput, DaemonResult<BTreeSet<Realisation>>),
    AddBuildLog(StorePath, Bytes, DaemonResult<()>),
    AddPermRoot(AddPermRootRequest, DaemonResult<DaemonPath>),
    SyncWithGC(DaemonResult<()>),
    QueryDerivationOutputs(StorePath, DaemonResult<StorePathSet>),
    QueryDerivationOutputNames(StorePath, DaemonResult<BTreeSet<OutputName>>),
}

impl MockOperation {
    pub fn request(&self) -> MockRequest {
        match self {
            Self::SetOptions(request, _) => MockRequest::SetOptions(request.clone()),
            Self::IsValidPath(request, _) => MockRequest::IsValidPath(request.clone()),
            Self::QueryValidPaths(request, _) => MockRequest::QueryValidPaths(request.clone()),
            Self::QueryPathInfo(request, _) => MockRequest::QueryPathInfo(request.clone()),
            Self::NarFromPath(request, _) => MockRequest::NarFromPath(request.clone()),
            Self::BuildPaths(request, _) => MockRequest::BuildPaths(request.clone()),
            Self::BuildPathsWithResults(request, _) => {
                MockRequest::BuildPathsWithResults(request.clone())
            }
            Self::BuildDerivation(request, _) => MockRequest::BuildDerivation(request.clone()),
            Self::QueryMissing(request, _) => MockRequest::QueryMissing(request.clone()),
            Self::AddToStoreNar(request, nar, _) => {
                MockRequest::AddToStoreNar(request.clone(), nar.clone())
            }
            Self::AddMultipleToStore(request, stream, _) => {
                MockRequest::AddMultipleToStore(request.clone(), stream.clone())
            }
            Self::QueryReferrers(request, _) => MockRequest::QueryReferrers(request.clone()),
            Self::AddToStore(request, content, _) => {
                MockRequest::AddToStore(request.clone(), content.clone())
            }
            Self::EnsurePath(request, _) => MockRequest::EnsurePath(request.clone()),
            Self::AddTempRoot(request, _) => MockRequest::AddTempRoot(request.clone()),
            Self::AddIndirectRoot(request, _) => MockRequest::AddIndirectRoot(request.clone()),
            Self::FindRoots(_) => MockRequest::FindRoots,
            Self::CollectGarbage(request, _) => MockRequest::CollectGarbage(request.clone()),
            Self::QueryAllValidPaths(_) => MockRequest::QueryAllValidPaths,
            Self::QueryPathFromHashPart(request, _) => MockRequest::QueryPathFromHashPart(*request),
            Self::QuerySubstitutablePaths(request, _) => {
                MockRequest::QuerySubstitutablePaths(request.clone())
            }
            Self::QueryValidDerivers(request, _) => {
                MockRequest::QueryValidDerivers(request.clone())
            }
            Self::OptimiseStore(_) => MockRequest::OptimiseStore,
            Self::VerifyStore(request, _) => MockRequest::VerifyStore(request.clone()),
            Self::AddSignatures(request, _) => MockRequest::AddSignatures(request.clone()),
            Self::QueryDerivationOutputMap(request, _) => {
                MockRequest::QueryDerivationOutputMap(request.clone())
            }
            Self::RegisterDrvOutput(request, _) => MockRequest::RegisterDrvOutput(request.clone()),
            Self::QueryRealisation(request, _) => MockRequest::QueryRealisation(request.clone()),
            Self::AddBuildLog(request, log, _) => {
                MockRequest::AddBuildLog(request.clone(), log.clone())
            }
            Self::AddPermRoot(request, _) => MockRequest::AddPermRoot(request.clone()),
            Self::SyncWithGC(_) => MockRequest::SyncWithGC,
            Self::QueryDerivationOutputs(request, _) => {
                MockRequest::QueryDerivationOutputs(request.clone())
            }
            Self::QueryDerivationOutputNames(request, _) => {
                MockRequest::QueryDerivationOutputNames(request.clone())
            }
        }
    }

    pub fn operation(&self) -> Operation {
        match self {
            Self::SetOptions(_, _) => Operation::SetOptions,
            Self::IsValidPath(_, _) => Operation::IsValidPath,
            Self::QueryValidPaths(_, _) => Operation::QueryValidPaths,
            Self::QueryPathInfo(_, _) => Operation::QueryPathInfo,
            Self::NarFromPath(_, _) => Operation::NarFromPath,
            Self::BuildPaths(_, _) => Operation::BuildPaths,
            Self::BuildPathsWithResults(_, _) => Operation::BuildPathsWithResults,
            Self::BuildDerivation(_, _) => Operation::BuildDerivation,
            Self::QueryMissing(_, _) => Operation::QueryMissing,
            Self::AddToStoreNar(_, _, _) => Operation::AddToStoreNar,
            Self::AddMultipleToStore(_, _, _) => Operation::AddMultipleToStore,
            Self::QueryReferrers(_, _) => Operation::QueryReferrers,
            Self::AddToStore(_, _, _) => Operation::AddToStore,
            Self::EnsurePath(_, _) => Operation::EnsurePath,
            Self::AddTempRoot(_, _) => Operation::AddTempRoot,
            Self::AddIndirectRoot(_, _) => Operation::AddIndirectRoot,
            Self::FindRoots(_) => Operation::FindRoots,
            Self::CollectGarbage(_, _) => Operation::CollectGarbage,
            Self::QueryAllValidPaths(_) => Operation::QueryAllValidPaths,
            Self::QueryPathFromHashPart(_, _) => Operation::QueryPathFromHashPart,
            Self::QuerySubstitutablePaths(_, _) => Operation::QuerySubstitutablePaths,
            Self::QueryValidDerivers(_, _) => Operation::QueryValidDerivers,
            Self::OptimiseStore(_) => Operation::OptimiseStore,
            Self::VerifyStore(_, _) => Operation::VerifyStore,
            Self::AddSignatures(_, _) => Operation::AddSignatures,
            Self::QueryDerivationOutputMap(_, _) => Operation::QueryDerivationOutputMap,
            Self::RegisterDrvOutput(_, _) => Operation::RegisterDrvOutput,
            Self::QueryRealisation(_, _) => Operation::QueryRealisation,
            Self::AddBuildLog(_, _, _) => Operation::AddBuildLog,
            Self::AddPermRoot(_, _) => Operation::AddPermRoot,
            Self::SyncWithGC(_) => Operation::SyncWithGC,
            Self::QueryDerivationOutputs(_, _) => Operation::QueryDerivationOutputs,
            Self::QueryDerivationOutputNames(_, _) => Operation::QueryDerivationOutputNames,
        }
    }

    pub fn response(&self) -> DaemonResult<MockResponse> {
        match self {
            Self::SetOptions(_, result) => result.clone().map(|value| value.into()),
            Self::IsValidPath(_, result) => result.clone().map(|value| value.into()),
            Self::QueryValidPaths(_, result) => result.clone().map(|value| value.into()),
            Self::QueryPathInfo(_, result) => result.clone().map(|value| value.into()),
            Self::NarFromPath(_, result) => result.clone().map(|value| value.into()),
            Self::BuildPaths(_, result) => result.clone().map(|value| value.into()),
            Self::BuildPathsWithResults(_, result) => result.clone().map(|value| value.into()),
            Self::BuildDerivation(_, result) => result.clone().map(|value| value.into()),
            Self::QueryMissing(_, result) => result.clone().map(|value| value.into()),
            Self::AddToStoreNar(_, _, result) => result.clone().map(|value| value.into()),
            Self::AddMultipleToStore(_, _, result) => result.clone().map(|value| value.into()),
            Self::QueryReferrers(_, result) => result.clone().map(|value| value.into()),
            Self::AddToStore(_, _, result) => result.clone().map(|value| value.into()),
            Self::EnsurePath(_, result) => result.clone().map(|value| value.into()),
            Self::AddTempRoot(_, result) => result.clone().map(|value| value.into()),
            Self::AddIndirectRoot(_, result) => result.clone().map(|value| value.into()),
            Self::FindRoots(result) => result.clone().map(|value| value.into()),
            Self::CollectGarbage(_, result) => result.clone().map(|value| value.into()),
            Self::QueryAllValidPaths(result) => result.clone().map(|value| value.into()),
            Self::QueryPathFromHashPart(_, result) => result.clone().map(|value| value.into()),
            Self::QuerySubstitutablePaths(_, result) => result.clone().map(|value| value.into()),
            Self::QueryValidDerivers(_, result) => result.clone().map(|value| value.into()),
            Self::OptimiseStore(result) => result.clone().map(|value| value.into()),
            Self::VerifyStore(_, result) => result.clone().map(|value| value.into()),
            Self::AddSignatures(_, result) => result.clone().map(|value| value.into()),
            Self::QueryDerivationOutputMap(_, result) => result.clone().map(|value| value.into()),
            Self::RegisterDrvOutput(_, result) => result.clone().map(|value| value.into()),
            Self::QueryRealisation(_, result) => result.clone().map(|value| value.into()),
            Self::AddBuildLog(_, _, result) => result.clone().map(|value| value.into()),
            Self::AddPermRoot(_, result) => result.clone().map(|value| value.into()),
            Self::SyncWithGC(result) => result.clone().map(|value| value.into()),
            Self::QueryDerivationOutputs(_, result) => result.clone().map(|value| value.into()),
            Self::QueryDerivationOutputNames(_, result) => result.clone().map(|value| value.into()),
        }
    }
}

enum ResponseResultLog<
    F1,
    F2,
    F3,
    F4,
    F5,
    F6,
    F7,
    F8,
    F9,
    F10,
    F11,
    F12,
    F13,
    F14,
    F15,
    F16,
    F17,
    F18,
    F19,
    F20,
    F21,
    F22,
    F23,
    F24,
    F25,
    F26,
    F27,
    F28,
    F29,
    F30,
    F31,
    F32,
    F33,
> {
    SetOptions(F1),
    IsValidPath(F2),
    QueryValidPaths(F3),
    QueryPathInfo(F4),
    QueryAllValidPaths(F5),
    NarFromPath(F6),
    BuildPaths(F7),
    BuildPathsWithResults(F8),
    BuildDerivation(F9),
    QueryMissing(F10),
    AddToStoreNar(F11),
    AddMultipleToStore(F12),
    QueryReferrers(F13),
    AddToStore(F14),
    EnsurePath(F15),
    AddTempRoot(F16),
    AddIndirectRoot(F17),
    FindRoots(F18),
    CollectGarbage(F19),
    QueryPathFromHashPart(F20),
    QuerySubstitutablePaths(F21),
    QueryValidDerivers(F22),
    OptimiseStore(F23),
    VerifyStore(F24),
    AddSignatures(F25),
    QueryDerivationOutputMap(F26),
    RegisterDrvOutput(F27),
    QueryRealisation(F28),
    AddBuildLog(F29),
    AddPermRoot(F30),
    SyncWithGC(F31),
    QueryDerivationOutputs(F32),
    QueryDerivationOutputNames(F33),
}

impl<
    F1,
    F2,
    F3,
    F4,
    F5,
    F6,
    F7,
    F8,
    F9,
    F10,
    F11,
    F12,
    F13,
    F14,
    F15,
    F16,
    F17,
    F18,
    F19,
    F20,
    F21,
    F22,
    F23,
    F24,
    F25,
    F26,
    F27,
    F28,
    F29,
    F30,
    F31,
    F32,
    F33,
>
    ResponseResultLog<
        F1,
        F2,
        F3,
        F4,
        F5,
        F6,
        F7,
        F8,
        F9,
        F10,
        F11,
        F12,
        F13,
        F14,
        F15,
        F16,
        F17,
        F18,
        F19,
        F20,
        F21,
        F22,
        F23,
        F24,
        F25,
        F26,
        F27,
        F28,
        F29,
        F30,
        F31,
        F32,
        F33,
    >
{
    #[allow(clippy::type_complexity)]
    fn as_pin_mut(
        self: Pin<&mut Self>,
    ) -> ResponseResultLog<
        Pin<&mut F1>,
        Pin<&mut F2>,
        Pin<&mut F3>,
        Pin<&mut F4>,
        Pin<&mut F5>,
        Pin<&mut F6>,
        Pin<&mut F7>,
        Pin<&mut F8>,
        Pin<&mut F9>,
        Pin<&mut F10>,
        Pin<&mut F11>,
        Pin<&mut F12>,
        Pin<&mut F13>,
        Pin<&mut F14>,
        Pin<&mut F15>,
        Pin<&mut F16>,
        Pin<&mut F17>,
        Pin<&mut F18>,
        Pin<&mut F19>,
        Pin<&mut F20>,
        Pin<&mut F21>,
        Pin<&mut F22>,
        Pin<&mut F23>,
        Pin<&mut F24>,
        Pin<&mut F25>,
        Pin<&mut F26>,
        Pin<&mut F27>,
        Pin<&mut F28>,
        Pin<&mut F29>,
        Pin<&mut F30>,
        Pin<&mut F31>,
        Pin<&mut F32>,
        Pin<&mut F33>,
    > {
        unsafe {
            match self.get_unchecked_mut() {
                ResponseResultLog::SetOptions(pointer) => {
                    ResponseResultLog::SetOptions(Pin::new_unchecked(pointer))
                }
                ResponseResultLog::IsValidPath(pointer) => {
                    ResponseResultLog::IsValidPath(Pin::new_unchecked(pointer))
                }
                ResponseResultLog::QueryValidPaths(pointer) => {
                    ResponseResultLog::QueryValidPaths(Pin::new_unchecked(pointer))
                }
                ResponseResultLog::QueryPathInfo(pointer) => {
                    ResponseResultLog::QueryPathInfo(Pin::new_unchecked(pointer))
                }
                ResponseResultLog::QueryAllValidPaths(pointer) => {
                    ResponseResultLog::QueryAllValidPaths(Pin::new_unchecked(pointer))
                }
                ResponseResultLog::NarFromPath(pointer) => {
                    ResponseResultLog::NarFromPath(Pin::new_unchecked(pointer))
                }
                ResponseResultLog::BuildPaths(pointer) => {
                    ResponseResultLog::BuildPaths(Pin::new_unchecked(pointer))
                }
                ResponseResultLog::BuildPathsWithResults(pointer) => {
                    ResponseResultLog::BuildPathsWithResults(Pin::new_unchecked(pointer))
                }
                ResponseResultLog::BuildDerivation(pointer) => {
                    ResponseResultLog::BuildDerivation(Pin::new_unchecked(pointer))
                }
                ResponseResultLog::QueryMissing(pointer) => {
                    ResponseResultLog::QueryMissing(Pin::new_unchecked(pointer))
                }
                ResponseResultLog::AddToStoreNar(pointer) => {
                    ResponseResultLog::AddToStoreNar(Pin::new_unchecked(pointer))
                }
                ResponseResultLog::AddMultipleToStore(pointer) => {
                    ResponseResultLog::AddMultipleToStore(Pin::new_unchecked(pointer))
                }
                ResponseResultLog::QueryReferrers(pointer) => {
                    ResponseResultLog::QueryReferrers(Pin::new_unchecked(pointer))
                }
                ResponseResultLog::AddToStore(pointer) => {
                    ResponseResultLog::AddToStore(Pin::new_unchecked(pointer))
                }
                ResponseResultLog::EnsurePath(pointer) => {
                    ResponseResultLog::EnsurePath(Pin::new_unchecked(pointer))
                }
                ResponseResultLog::AddTempRoot(pointer) => {
                    ResponseResultLog::AddTempRoot(Pin::new_unchecked(pointer))
                }
                ResponseResultLog::AddIndirectRoot(pointer) => {
                    ResponseResultLog::AddIndirectRoot(Pin::new_unchecked(pointer))
                }
                ResponseResultLog::FindRoots(pointer) => {
                    ResponseResultLog::FindRoots(Pin::new_unchecked(pointer))
                }
                ResponseResultLog::CollectGarbage(pointer) => {
                    ResponseResultLog::CollectGarbage(Pin::new_unchecked(pointer))
                }
                ResponseResultLog::QueryPathFromHashPart(pointer) => {
                    ResponseResultLog::QueryPathFromHashPart(Pin::new_unchecked(pointer))
                }
                ResponseResultLog::QuerySubstitutablePaths(pointer) => {
                    ResponseResultLog::QuerySubstitutablePaths(Pin::new_unchecked(pointer))
                }
                ResponseResultLog::QueryValidDerivers(pointer) => {
                    ResponseResultLog::QueryValidDerivers(Pin::new_unchecked(pointer))
                }
                ResponseResultLog::OptimiseStore(pointer) => {
                    ResponseResultLog::OptimiseStore(Pin::new_unchecked(pointer))
                }
                ResponseResultLog::VerifyStore(pointer) => {
                    ResponseResultLog::VerifyStore(Pin::new_unchecked(pointer))
                }
                ResponseResultLog::AddSignatures(pointer) => {
                    ResponseResultLog::AddSignatures(Pin::new_unchecked(pointer))
                }
                ResponseResultLog::QueryDerivationOutputMap(pointer) => {
                    ResponseResultLog::QueryDerivationOutputMap(Pin::new_unchecked(pointer))
                }
                ResponseResultLog::RegisterDrvOutput(pointer) => {
                    ResponseResultLog::RegisterDrvOutput(Pin::new_unchecked(pointer))
                }
                ResponseResultLog::QueryRealisation(pointer) => {
                    ResponseResultLog::QueryRealisation(Pin::new_unchecked(pointer))
                }
                ResponseResultLog::AddBuildLog(pointer) => {
                    ResponseResultLog::AddBuildLog(Pin::new_unchecked(pointer))
                }
                ResponseResultLog::AddPermRoot(pointer) => {
                    ResponseResultLog::AddPermRoot(Pin::new_unchecked(pointer))
                }
                ResponseResultLog::SyncWithGC(pointer) => {
                    ResponseResultLog::SyncWithGC(Pin::new_unchecked(pointer))
                }
                ResponseResultLog::QueryDerivationOutputs(pointer) => {
                    ResponseResultLog::QueryDerivationOutputs(Pin::new_unchecked(pointer))
                }
                ResponseResultLog::QueryDerivationOutputNames(pointer) => {
                    ResponseResultLog::QueryDerivationOutputNames(Pin::new_unchecked(pointer))
                }
            }
        }
    }
}

impl<
    F1,
    F2,
    F3,
    F4,
    F5,
    F6,
    F7,
    F8,
    F9,
    F10,
    F11,
    F12,
    F13,
    F14,
    F15,
    F16,
    F17,
    F18,
    F19,
    F20,
    F21,
    F22,
    F23,
    F24,
    F25,
    F26,
    F27,
    F28,
    F29,
    F30,
    F31,
    F32,
    F33,
> Stream
    for ResponseResultLog<
        F1,
        F2,
        F3,
        F4,
        F5,
        F6,
        F7,
        F8,
        F9,
        F10,
        F11,
        F12,
        F13,
        F14,
        F15,
        F16,
        F17,
        F18,
        F19,
        F20,
        F21,
        F22,
        F23,
        F24,
        F25,
        F26,
        F27,
        F28,
        F29,
        F30,
        F31,
        F32,
        F33,
    >
where
    F1: Stream<Item = LogMessage>,
    F2: Stream<Item = LogMessage>,
    F3: Stream<Item = LogMessage>,
    F4: Stream<Item = LogMessage>,
    F5: Stream<Item = LogMessage>,
    F6: Stream<Item = LogMessage>,
    F7: Stream<Item = LogMessage>,
    F8: Stream<Item = LogMessage>,
    F9: Stream<Item = LogMessage>,
    F10: Stream<Item = LogMessage>,
    F11: Stream<Item = LogMessage>,
    F12: Stream<Item = LogMessage>,
    F13: Stream<Item = LogMessage>,
    F14: Stream<Item = LogMessage>,
    F15: Stream<Item = LogMessage>,
    F16: Stream<Item = LogMessage>,
    F17: Stream<Item = LogMessage>,
    F18: Stream<Item = LogMessage>,
    F19: Stream<Item = LogMessage>,
    F20: Stream<Item = LogMessage>,
    F21: Stream<Item = LogMessage>,
    F22: Stream<Item = LogMessage>,
    F23: Stream<Item = LogMessage>,
    F24: Stream<Item = LogMessage>,
    F25: Stream<Item = LogMessage>,
    F26: Stream<Item = LogMessage>,
    F27: Stream<Item = LogMessage>,
    F28: Stream<Item = LogMessage>,
    F29: Stream<Item = LogMessage>,
    F30: Stream<Item = LogMessage>,
    F31: Stream<Item = LogMessage>,
    F32: Stream<Item = LogMessage>,
    F33: Stream<Item = LogMessage>,
{
    type Item = LogMessage;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        match self.as_pin_mut() {
            ResponseResultLog::SetOptions(res) => res.poll_next(cx),
            ResponseResultLog::IsValidPath(res) => res.poll_next(cx),
            ResponseResultLog::QueryValidPaths(res) => res.poll_next(cx),
            ResponseResultLog::QueryPathInfo(res) => res.poll_next(cx),
            ResponseResultLog::QueryAllValidPaths(res) => res.poll_next(cx),
            ResponseResultLog::NarFromPath(res) => res.poll_next(cx),
            ResponseResultLog::BuildPaths(res) => res.poll_next(cx),
            ResponseResultLog::BuildPathsWithResults(res) => res.poll_next(cx),
            ResponseResultLog::BuildDerivation(res) => res.poll_next(cx),
            ResponseResultLog::QueryMissing(res) => res.poll_next(cx),
            ResponseResultLog::AddToStoreNar(res) => res.poll_next(cx),
            ResponseResultLog::AddMultipleToStore(res) => res.poll_next(cx),
            ResponseResultLog::QueryReferrers(res) => res.poll_next(cx),
            ResponseResultLog::AddToStore(res) => res.poll_next(cx),
            ResponseResultLog::EnsurePath(res) => res.poll_next(cx),
            ResponseResultLog::AddTempRoot(res) => res.poll_next(cx),
            ResponseResultLog::AddIndirectRoot(res) => res.poll_next(cx),
            ResponseResultLog::FindRoots(res) => res.poll_next(cx),
            ResponseResultLog::CollectGarbage(res) => res.poll_next(cx),
            ResponseResultLog::QueryPathFromHashPart(res) => res.poll_next(cx),
            ResponseResultLog::QuerySubstitutablePaths(res) => res.poll_next(cx),
            ResponseResultLog::QueryValidDerivers(res) => res.poll_next(cx),
            ResponseResultLog::OptimiseStore(res) => res.poll_next(cx),
            ResponseResultLog::VerifyStore(res) => res.poll_next(cx),
            ResponseResultLog::AddSignatures(res) => res.poll_next(cx),
            ResponseResultLog::QueryDerivationOutputMap(res) => res.poll_next(cx),
            ResponseResultLog::RegisterDrvOutput(res) => res.poll_next(cx),
            ResponseResultLog::QueryRealisation(res) => res.poll_next(cx),
            ResponseResultLog::AddBuildLog(res) => res.poll_next(cx),
            ResponseResultLog::AddPermRoot(res) => res.poll_next(cx),
            ResponseResultLog::SyncWithGC(res) => res.poll_next(cx),
            ResponseResultLog::QueryDerivationOutputs(res) => res.poll_next(cx),
            ResponseResultLog::QueryDerivationOutputNames(res) => res.poll_next(cx),
        }
    }
}
impl<
    F1,
    F2,
    F3,
    F4,
    F5,
    F6,
    F7,
    F8,
    F9,
    F10,
    F11,
    F12,
    F13,
    F14,
    F15,
    F16,
    F17,
    F18,
    F19,
    F20,
    F21,
    F22,
    F23,
    F24,
    F25,
    F26,
    F27,
    F28,
    F29,
    F30,
    F31,
    F32,
    F33,
    R,
> Future
    for ResponseResultLog<
        F1,
        F2,
        F3,
        F4,
        F5,
        F6,
        F7,
        F8,
        F9,
        F10,
        F11,
        F12,
        F13,
        F14,
        F15,
        F16,
        F17,
        F18,
        F19,
        F20,
        F21,
        F22,
        F23,
        F24,
        F25,
        F26,
        F27,
        F28,
        F29,
        F30,
        F31,
        F32,
        F33,
    >
where
    F1: Future<Output = R>,
    F2: Future<Output = R>,
    F3: Future<Output = R>,
    F4: Future<Output = R>,
    F5: Future<Output = R>,
    F6: Future<Output = R>,
    F7: Future<Output = R>,
    F8: Future<Output = R>,
    F9: Future<Output = R>,
    F10: Future<Output = R>,
    F11: Future<Output = R>,
    F12: Future<Output = R>,
    F13: Future<Output = R>,
    F14: Future<Output = R>,
    F15: Future<Output = R>,
    F16: Future<Output = R>,
    F17: Future<Output = R>,
    F18: Future<Output = R>,
    F19: Future<Output = R>,
    F20: Future<Output = R>,
    F21: Future<Output = R>,
    F22: Future<Output = R>,
    F23: Future<Output = R>,
    F24: Future<Output = R>,
    F25: Future<Output = R>,
    F26: Future<Output = R>,
    F27: Future<Output = R>,
    F28: Future<Output = R>,
    F29: Future<Output = R>,
    F30: Future<Output = R>,
    F31: Future<Output = R>,
    F32: Future<Output = R>,
    F33: Future<Output = R>,
{
    type Output = R;

    fn poll(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        match self.as_pin_mut() {
            ResponseResultLog::SetOptions(res) => res.poll(cx),
            ResponseResultLog::IsValidPath(res) => res.poll(cx),
            ResponseResultLog::QueryValidPaths(res) => res.poll(cx),
            ResponseResultLog::QueryPathInfo(res) => res.poll(cx),
            ResponseResultLog::QueryAllValidPaths(res) => res.poll(cx),
            ResponseResultLog::NarFromPath(res) => res.poll(cx),
            ResponseResultLog::BuildPaths(res) => res.poll(cx),
            ResponseResultLog::BuildPathsWithResults(res) => res.poll(cx),
            ResponseResultLog::BuildDerivation(res) => res.poll(cx),
            ResponseResultLog::QueryMissing(res) => res.poll(cx),
            ResponseResultLog::AddToStoreNar(res) => res.poll(cx),
            ResponseResultLog::AddMultipleToStore(res) => res.poll(cx),
            ResponseResultLog::QueryReferrers(res) => res.poll(cx),
            ResponseResultLog::AddToStore(res) => res.poll(cx),
            ResponseResultLog::EnsurePath(res) => res.poll(cx),
            ResponseResultLog::AddTempRoot(res) => res.poll(cx),
            ResponseResultLog::AddIndirectRoot(res) => res.poll(cx),
            ResponseResultLog::FindRoots(res) => res.poll(cx),
            ResponseResultLog::CollectGarbage(res) => res.poll(cx),
            ResponseResultLog::QueryPathFromHashPart(res) => res.poll(cx),
            ResponseResultLog::QuerySubstitutablePaths(res) => res.poll(cx),
            ResponseResultLog::QueryValidDerivers(res) => res.poll(cx),
            ResponseResultLog::OptimiseStore(res) => res.poll(cx),
            ResponseResultLog::VerifyStore(res) => res.poll(cx),
            ResponseResultLog::AddSignatures(res) => res.poll(cx),
            ResponseResultLog::QueryDerivationOutputMap(res) => res.poll(cx),
            ResponseResultLog::RegisterDrvOutput(res) => res.poll(cx),
            ResponseResultLog::QueryRealisation(res) => res.poll(cx),
            ResponseResultLog::AddBuildLog(res) => res.poll(cx),
            ResponseResultLog::AddPermRoot(res) => res.poll(cx),
            ResponseResultLog::SyncWithGC(res) => res.poll(cx),
            ResponseResultLog::QueryDerivationOutputs(res) => res.poll(cx),
            ResponseResultLog::QueryDerivationOutputNames(res) => res.poll(cx),
        }
    }
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub enum MockRequest {
    SetOptions(ClientOptions),
    IsValidPath(StorePath),
    QueryValidPaths(QueryValidPathsRequest),
    QueryPathInfo(StorePath),
    QueryAllValidPaths,
    NarFromPath(StorePath),
    BuildPaths(BuildPathsRequest),
    BuildPathsWithResults(BuildPathsRequest),
    BuildDerivation(BuildDerivationRequest),
    QueryMissing(Vec<DerivedPath>),
    AddToStoreNar(AddToStoreNarRequest, Bytes),
    AddMultipleToStore(AddMultipleToStoreRequest, Vec<(ValidPathInfo, Bytes)>),
    QueryReferrers(StorePath),
    AddToStore(AddToStoreRequest25, Bytes),
    EnsurePath(StorePath),
    AddTempRoot(StorePath),
    AddIndirectRoot(DaemonPath),
    FindRoots,
    CollectGarbage(CollectGarbageRequest),
    QueryPathFromHashPart(StorePathHash),
    QuerySubstitutablePaths(StorePathSet),
    QueryValidDerivers(StorePath),
    OptimiseStore,
    VerifyStore(VerifyStoreRequest),
    AddSignatures(AddSignaturesRequest),
    QueryDerivationOutputMap(StorePath),
    RegisterDrvOutput(Realisation),
    QueryRealisation(DrvOutput),
    AddBuildLog(StorePath, Bytes),
    AddPermRoot(AddPermRootRequest),
    SyncWithGC,
    QueryDerivationOutputs(StorePath),
    QueryDerivationOutputNames(StorePath),
}

impl MockRequest {
    pub fn operation(&self) -> Operation {
        match self {
            Self::SetOptions(_) => Operation::SetOptions,
            Self::IsValidPath(_) => Operation::IsValidPath,
            Self::QueryValidPaths(_) => Operation::QueryValidPaths,
            Self::QueryPathInfo(_) => Operation::QueryPathInfo,
            Self::QueryAllValidPaths => Operation::QueryAllValidPaths,
            Self::NarFromPath(_) => Operation::NarFromPath,
            Self::BuildPaths(_) => Operation::BuildPaths,
            Self::BuildPathsWithResults(_) => Operation::BuildPathsWithResults,
            Self::BuildDerivation(_) => Operation::BuildDerivation,
            Self::QueryMissing(_) => Operation::QueryMissing,
            Self::AddToStoreNar(_, _) => Operation::AddToStoreNar,
            Self::AddMultipleToStore(_, _) => Operation::AddMultipleToStore,
            Self::QueryReferrers(_) => Operation::QueryReferrers,
            Self::AddToStore(_, _) => Operation::AddToStore,
            Self::EnsurePath(_) => Operation::EnsurePath,
            Self::AddTempRoot(_) => Operation::AddTempRoot,
            Self::AddIndirectRoot(_) => Operation::AddIndirectRoot,
            Self::FindRoots => Operation::FindRoots,
            Self::CollectGarbage(_) => Operation::CollectGarbage,
            Self::QueryPathFromHashPart(_) => Operation::QueryPathFromHashPart,
            Self::QuerySubstitutablePaths(_) => Operation::QuerySubstitutablePaths,
            Self::QueryValidDerivers(_) => Operation::QueryValidDerivers,
            Self::OptimiseStore => Operation::OptimiseStore,
            Self::VerifyStore(_) => Operation::VerifyStore,
            Self::AddSignatures(_) => Operation::AddSignatures,
            Self::QueryDerivationOutputMap(_) => Operation::QueryDerivationOutputMap,
            Self::RegisterDrvOutput(_) => Operation::RegisterDrvOutput,
            Self::QueryRealisation(_) => Operation::QueryRealisation,
            Self::AddBuildLog(_, _) => Operation::AddBuildLog,
            Self::AddPermRoot(_) => Operation::AddPermRoot,
            Self::SyncWithGC => Operation::SyncWithGC,
            Self::QueryDerivationOutputs(_) => Operation::QueryDerivationOutputs,
            Self::QueryDerivationOutputNames(_) => Operation::QueryDerivationOutputNames,
        }
    }
    pub fn get_response<'s, S>(
        &'s self,
        store: &'s mut S,
    ) -> impl ResultLog<Output = DaemonResult<MockResponse>> + 's
    where
        S: DaemonStore + 's,
    {
        match self {
            Self::SetOptions(options) => ResponseResultLog::SetOptions(
                store.set_options(options).map_ok(|value| value.into()),
            ),
            Self::IsValidPath(path) => {
                ResponseResultLog::IsValidPath(store.is_valid_path(path).map_ok(From::from))
            }
            Self::QueryValidPaths(request) => ResponseResultLog::QueryValidPaths(
                store
                    .query_valid_paths(&request.paths, request.substitute)
                    .map_ok(From::from),
            ),
            Self::QueryPathInfo(path) => {
                ResponseResultLog::QueryPathInfo(store.query_path_info(path).map_ok(From::from))
            }
            Self::NarFromPath(path) => ResponseResultLog::NarFromPath(
                store.nar_from_path(path).and_then(|reader| async move {
                    let mut reader = pin!(reader);
                    let mut out = Vec::new();
                    reader.read_to_end(&mut out).await?;
                    Ok(From::from(Bytes::from(out)))
                }),
            ),
            Self::BuildPaths(request) => ResponseResultLog::BuildPaths(
                store
                    .build_paths(&request.paths, request.mode)
                    .map_ok(From::from),
            ),
            Self::BuildPathsWithResults(request) => ResponseResultLog::BuildPathsWithResults(
                store
                    .build_paths_with_results(&request.paths, request.mode)
                    .map_ok(From::from),
            ),
            Self::BuildDerivation(request) => ResponseResultLog::BuildDerivation(
                store
                    .build_derivation(&request.drv, request.mode)
                    .map_ok(From::from),
            ),
            Self::QueryMissing(paths) => {
                ResponseResultLog::QueryMissing(store.query_missing(paths).map_ok(From::from))
            }
            Self::AddToStoreNar(request, source) => ResponseResultLog::AddToStoreNar(
                store
                    .add_to_store_nar(
                        &request.path_info,
                        Cursor::new(source),
                        request.repair,
                        request.dont_check_sigs,
                    )
                    .map_ok(|value| value.into()),
            ),
            Self::AddMultipleToStore(request, stream) => ResponseResultLog::AddMultipleToStore(
                store
                    .add_multiple_to_store(
                        request.repair,
                        request.dont_check_sigs,
                        iter(stream.iter().map(|(info, content)| {
                            Ok(AddToStoreItem {
                                info: info.clone(),
                                reader: Cursor::new(content.clone()),
                            })
                        })),
                    )
                    .map_ok(|value| value.into()),
            ),
            Self::QueryAllValidPaths => ResponseResultLog::QueryAllValidPaths(
                store.query_all_valid_paths().map_ok(From::from),
            ),
            Self::QueryReferrers(path) => {
                ResponseResultLog::QueryReferrers(store.query_referrers(path).map_ok(From::from))
            }
            Self::AddToStore(request, source) => ResponseResultLog::AddToStore(
                store
                    .add_ca_to_store(
                        &request.name,
                        request.cam,
                        &request.refs,
                        request.repair,
                        Cursor::new(source),
                    )
                    .map_ok(|value| value.into()),
            ),
            Self::EnsurePath(path) => {
                ResponseResultLog::EnsurePath(store.ensure_path(path).map_ok(From::from))
            }
            Self::AddTempRoot(path) => {
                ResponseResultLog::AddTempRoot(store.add_temp_root(path).map_ok(From::from))
            }
            Self::AddIndirectRoot(path) => {
                ResponseResultLog::AddIndirectRoot(store.add_indirect_root(path).map_ok(From::from))
            }
            Self::FindRoots => ResponseResultLog::FindRoots(store.find_roots().map_ok(From::from)),
            Self::CollectGarbage(request) => ResponseResultLog::CollectGarbage(
                store
                    .collect_garbage(
                        request.action,
                        &request.paths_to_delete,
                        request.ignore_liveness,
                        request.max_freed,
                    )
                    .map_ok(From::from),
            ),
            Self::QueryPathFromHashPart(hash) => ResponseResultLog::QueryPathFromHashPart(
                store.query_path_from_hash_part(hash).map_ok(From::from),
            ),
            Self::QuerySubstitutablePaths(paths) => ResponseResultLog::QuerySubstitutablePaths(
                store.query_substitutable_paths(paths).map_ok(From::from),
            ),
            Self::QueryValidDerivers(path) => ResponseResultLog::QueryValidDerivers(
                store.query_valid_derivers(path).map_ok(From::from),
            ),
            Self::OptimiseStore => {
                ResponseResultLog::OptimiseStore(store.optimise_store().map_ok(From::from))
            }
            Self::VerifyStore(request) => ResponseResultLog::VerifyStore(
                store
                    .verify_store(request.check_contents, request.repair)
                    .map_ok(From::from),
            ),
            Self::AddSignatures(request) => ResponseResultLog::AddSignatures(
                store
                    .add_signatures(&request.path, &request.signatures)
                    .map_ok(From::from),
            ),
            Self::QueryDerivationOutputMap(path) => ResponseResultLog::QueryDerivationOutputMap(
                store.query_derivation_output_map(path).map_ok(From::from),
            ),
            Self::RegisterDrvOutput(realisation) => ResponseResultLog::RegisterDrvOutput(
                store.register_drv_output(realisation).map_ok(From::from),
            ),
            Self::QueryRealisation(output_id) => ResponseResultLog::QueryRealisation(
                store.query_realisation(output_id).map_ok(From::from),
            ),
            Self::AddBuildLog(path, log) => ResponseResultLog::AddBuildLog(
                store
                    .add_build_log(path, Cursor::new(log))
                    .map_ok(From::from),
            ),
            Self::AddPermRoot(request) => ResponseResultLog::AddPermRoot(
                store
                    .add_perm_root(&request.store_path, &request.gc_root)
                    .map_ok(From::from),
            ),
            Self::SyncWithGC => {
                ResponseResultLog::SyncWithGC(store.sync_with_gc().map_ok(From::from))
            }
            Self::QueryDerivationOutputs(path) => ResponseResultLog::QueryDerivationOutputs(
                store.query_derivation_outputs(path).map_ok(From::from),
            ),
            Self::QueryDerivationOutputNames(path) => {
                ResponseResultLog::QueryDerivationOutputNames(
                    store.query_derivation_output_names(path).map_ok(From::from),
                )
            }
        }
    }
}

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub enum MockResponse {
    Empty,
    Bool(bool),
    StorePathSet(StorePathSet),
    BuildResult(BuildResult),
    KeyedBuildResults(KeyedBuildResults),
    Bytes(Bytes),
    UnkeyedValidPathInfo(Option<UnkeyedValidPathInfo>),
    ValidPathInfo(ValidPathInfo),
    QueryMissingResult(QueryMissingResult),
    GCRoots(BTreeMap<DaemonPath, StorePath>),
    CollectGarbageResponse(CollectGarbageResponse),
    OptStorePath(Option<StorePath>),
    OutputMap(BTreeMap<OutputName, Option<StorePath>>),
    Realisations(BTreeSet<Realisation>),
    OutputNames(BTreeSet<OutputName>),
}

impl MockResponse {
    pub fn unwrap_empty(self) {
        match self {
            Self::Empty => (),
            _ => panic!("Unexpected response {self:?}"),
        }
    }

    pub fn unwrap_bool(self) -> bool {
        match self {
            Self::Bool(val) => val,
            _ => panic!("Unexpected response {self:?}"),
        }
    }

    pub fn unwrap_store_path_set(self) -> StorePathSet {
        match self {
            Self::StorePathSet(val) => val,
            _ => panic!("Unexpected response {self:?}"),
        }
    }

    pub fn unwrap_build_result(self) -> BuildResult {
        match self {
            Self::BuildResult(val) => val,
            _ => panic!("Unexpected response {self:?}"),
        }
    }

    pub fn unwrap_keyed_build_results(self) -> KeyedBuildResults {
        match self {
            Self::KeyedBuildResults(val) => val,
            _ => panic!("Unexpected response {self:?}"),
        }
    }

    pub fn unwrap_bytes(self) -> Bytes {
        match self {
            Self::Bytes(val) => val,
            _ => panic!("Unexpected response {self:?}"),
        }
    }

    pub fn unwrap_unkeyed_valid_path_info(self) -> Option<UnkeyedValidPathInfo> {
        match self {
            Self::UnkeyedValidPathInfo(val) => val,
            _ => panic!("Unexpected response {self:?}"),
        }
    }

    pub fn unwrap_valid_path_info(self) -> ValidPathInfo {
        match self {
            Self::ValidPathInfo(val) => val,
            _ => panic!("Unexpected response {self:?}"),
        }
    }

    pub fn unwrap_query_missing_result(self) -> QueryMissingResult {
        match self {
            Self::QueryMissingResult(val) => val,
            _ => panic!("Unexpected response {self:?}"),
        }
    }

    pub fn unwrap_gc_roots(self) -> BTreeMap<DaemonPath, StorePath> {
        match self {
            Self::GCRoots(val) => val,
            _ => panic!("Unexpected response {self:?}"),
        }
    }

    pub fn unwrap_collect_garbage_response(self) -> CollectGarbageResponse {
        match self {
            Self::CollectGarbageResponse(val) => val,
            _ => panic!("Unexpected response {self:?}"),
        }
    }

    pub fn unwrap_optional_store_path(self) -> Option<StorePath> {
        match self {
            Self::OptStorePath(val) => val,
            _ => panic!("Unexpected response {self:?}"),
        }
    }
    pub fn unwrap_output_map(self) -> BTreeMap<OutputName, Option<StorePath>> {
        match self {
            Self::OutputMap(val) => val,
            _ => panic!("Unexpected response {self:?}"),
        }
    }
    pub fn unwrap_realisations(self) -> BTreeSet<Realisation> {
        match self {
            Self::Realisations(val) => val,
            _ => panic!("Unexpected response {self:?}"),
        }
    }
    pub fn unwrap_output_names(self) -> BTreeSet<OutputName> {
        match self {
            Self::OutputNames(val) => val,
            _ => panic!("Unexpected response {self:?}"),
        }
    }
}

impl From<()> for MockResponse {
    fn from(_: ()) -> Self {
        MockResponse::Empty
    }
}
impl From<MockResponse> for () {
    fn from(value: MockResponse) -> Self {
        value.unwrap_empty()
    }
}
impl From<bool> for MockResponse {
    fn from(val: bool) -> Self {
        MockResponse::Bool(val)
    }
}
impl From<MockResponse> for bool {
    fn from(value: MockResponse) -> Self {
        value.unwrap_bool()
    }
}
impl From<StorePathSet> for MockResponse {
    fn from(v: StorePathSet) -> Self {
        MockResponse::StorePathSet(v)
    }
}
impl From<MockResponse> for StorePathSet {
    fn from(value: MockResponse) -> Self {
        value.unwrap_store_path_set()
    }
}
impl From<BuildResult> for MockResponse {
    fn from(v: BuildResult) -> Self {
        MockResponse::BuildResult(v)
    }
}
impl From<MockResponse> for BuildResult {
    fn from(value: MockResponse) -> Self {
        value.unwrap_build_result()
    }
}
impl From<KeyedBuildResults> for MockResponse {
    fn from(v: KeyedBuildResults) -> Self {
        MockResponse::KeyedBuildResults(v)
    }
}
impl From<MockResponse> for KeyedBuildResults {
    fn from(value: MockResponse) -> Self {
        value.unwrap_keyed_build_results()
    }
}
impl From<Bytes> for MockResponse {
    fn from(v: Bytes) -> Self {
        MockResponse::Bytes(v)
    }
}
impl From<MockResponse> for Bytes {
    fn from(value: MockResponse) -> Self {
        value.unwrap_bytes()
    }
}
impl From<Option<UnkeyedValidPathInfo>> for MockResponse {
    fn from(v: Option<UnkeyedValidPathInfo>) -> Self {
        MockResponse::UnkeyedValidPathInfo(v)
    }
}
impl From<MockResponse> for Option<UnkeyedValidPathInfo> {
    fn from(value: MockResponse) -> Self {
        value.unwrap_unkeyed_valid_path_info()
    }
}
impl From<ValidPathInfo> for MockResponse {
    fn from(v: ValidPathInfo) -> Self {
        MockResponse::ValidPathInfo(v)
    }
}
impl From<MockResponse> for ValidPathInfo {
    fn from(value: MockResponse) -> Self {
        value.unwrap_valid_path_info()
    }
}
impl From<QueryMissingResult> for MockResponse {
    fn from(v: QueryMissingResult) -> Self {
        MockResponse::QueryMissingResult(v)
    }
}
impl From<MockResponse> for QueryMissingResult {
    fn from(value: MockResponse) -> Self {
        value.unwrap_query_missing_result()
    }
}

impl From<BTreeMap<DaemonPath, StorePath>> for MockResponse {
    fn from(v: BTreeMap<DaemonPath, StorePath>) -> Self {
        MockResponse::GCRoots(v)
    }
}
impl From<MockResponse> for BTreeMap<DaemonPath, StorePath> {
    fn from(value: MockResponse) -> Self {
        value.unwrap_gc_roots()
    }
}

impl From<CollectGarbageResponse> for MockResponse {
    fn from(v: CollectGarbageResponse) -> Self {
        MockResponse::CollectGarbageResponse(v)
    }
}
impl From<MockResponse> for CollectGarbageResponse {
    fn from(value: MockResponse) -> Self {
        value.unwrap_collect_garbage_response()
    }
}

impl From<Option<StorePath>> for MockResponse {
    fn from(v: Option<StorePath>) -> Self {
        MockResponse::OptStorePath(v)
    }
}
impl From<MockResponse> for Option<StorePath> {
    fn from(value: MockResponse) -> Self {
        value.unwrap_optional_store_path()
    }
}

impl From<BTreeMap<OutputName, Option<StorePath>>> for MockResponse {
    fn from(v: BTreeMap<OutputName, Option<StorePath>>) -> Self {
        MockResponse::OutputMap(v)
    }
}
impl From<MockResponse> for BTreeMap<OutputName, Option<StorePath>> {
    fn from(value: MockResponse) -> Self {
        value.unwrap_output_map()
    }
}

impl From<BTreeSet<Realisation>> for MockResponse {
    fn from(v: BTreeSet<Realisation>) -> Self {
        MockResponse::Realisations(v)
    }
}
impl From<MockResponse> for BTreeSet<Realisation> {
    fn from(value: MockResponse) -> Self {
        value.unwrap_realisations()
    }
}

impl From<BTreeSet<OutputName>> for MockResponse {
    fn from(v: BTreeSet<OutputName>) -> Self {
        MockResponse::OutputNames(v)
    }
}
impl From<MockResponse> for BTreeSet<OutputName> {
    fn from(value: MockResponse) -> Self {
        value.unwrap_output_names()
    }
}

pub trait MockReporter {
    fn unexpected_operation(
        &mut self,
        expected: MockOperation,
        actual: MockRequest,
    ) -> impl ResultLog<Output = DaemonResult<MockResponse>> + Send;
    fn invalid_operation(
        &mut self,
        expected: MockOperation,
        actual: MockRequest,
    ) -> impl ResultLog<Output = DaemonResult<MockResponse>> + Send;
    fn extra_operation(
        &mut self,
        actual: MockRequest,
    ) -> impl ResultLog<Output = DaemonResult<MockResponse>> + Send;
    fn unread_operation(&mut self, operation: LogOperation) -> DaemonResult<()>;
}

impl MockReporter for () {
    fn unexpected_operation(
        &mut self,
        expected: MockOperation,
        actual: MockRequest,
    ) -> impl ResultLog<Output = DaemonResult<MockResponse>> {
        ResultProcess {
            stream: empty(),
            result: ready(
                Err(DaemonError::custom(format!(
                    "Unexpected operation {} expected {}",
                    actual.operation(),
                    expected.operation()
                )))
                .with_operation(actual.operation()),
            ),
        }
    }

    fn invalid_operation(
        &mut self,
        expected: MockOperation,
        actual: MockRequest,
    ) -> impl ResultLog<Output = DaemonResult<MockResponse>> {
        ResultProcess {
            stream: empty(),
            result: ready(
                Err(DaemonError::custom(format!(
                    "Invalid operation {:?} expected {:?}",
                    actual,
                    expected.request()
                )))
                .with_operation(actual.operation()),
            ),
        }
    }

    fn extra_operation(
        &mut self,
        actual: MockRequest,
    ) -> impl ResultLog<Output = DaemonResult<MockResponse>> {
        ResultProcess {
            stream: empty(),
            result: ready(
                Err(DaemonError::custom(format!("Extra operation {actual:?}")))
                    .with_operation(actual.operation()),
            ),
        }
    }

    fn unread_operation(&mut self, operation: LogOperation) -> DaemonResult<()> {
        Err(DaemonError::custom(format!(
            "store dropped with {operation:?} operation still unread"
        )))
    }
}

pub enum ReporterError {
    Unexpected(MockOperation, MockRequest),
    Invalid(MockOperation, MockRequest),
    Extra(MockRequest),
    Unread(LogOperation),
}

impl fmt::Display for ReporterError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ReporterError::Unexpected(expected, actual) => {
                write!(
                    f,
                    "Unexpected operation {} expected {}",
                    actual.operation(),
                    expected.operation()
                )
            }
            ReporterError::Invalid(expected, actual) => {
                write!(
                    f,
                    "Invalid operation {:?} expected {:?}",
                    actual,
                    expected.request()
                )
            }
            ReporterError::Extra(actual) => {
                write!(f, "Extra operation {actual:?}")
            }
            ReporterError::Unread(operation) => {
                write!(f, "store dropped with {operation:?} operation still unread")
            }
        }
    }
}

#[derive(Clone)]
pub struct ChannelReporter(futures::channel::mpsc::UnboundedSender<ReporterError>);
impl Drop for ChannelReporter {
    fn drop(&mut self) {
        self.0.close_channel();
    }
}
impl MockReporter for ChannelReporter {
    fn unexpected_operation(
        &mut self,
        expected: MockOperation,
        actual: MockRequest,
    ) -> impl ResultLog<Output = DaemonResult<MockResponse>> {
        let op = actual.operation();
        let report = ReporterError::Unexpected(expected, actual);
        let ret = Err(DaemonError::custom(&report)).with_operation(op);
        self.0.unbounded_send(report).unwrap();
        ResultProcess {
            stream: empty(),
            result: ready(ret),
        }
    }

    fn invalid_operation(
        &mut self,
        expected: MockOperation,
        actual: MockRequest,
    ) -> impl ResultLog<Output = DaemonResult<MockResponse>> {
        let op = actual.operation();
        let report = ReporterError::Invalid(expected, actual);
        let ret = Err(DaemonError::custom(&report)).with_operation(op);
        self.0.unbounded_send(report).unwrap();
        ResultProcess {
            stream: empty(),
            result: ready(ret),
        }
    }

    fn extra_operation(
        &mut self,
        actual: MockRequest,
    ) -> impl ResultLog<Output = DaemonResult<MockResponse>> {
        let op = actual.operation();
        let report = ReporterError::Extra(actual);
        let ret = Err(DaemonError::custom(&report)).with_operation(op);
        self.0.unbounded_send(report).unwrap();
        ResultProcess {
            stream: empty(),
            result: ready(ret),
        }
    }

    fn unread_operation(&mut self, operation: LogOperation) -> DaemonResult<()> {
        self.0
            .unbounded_send(ReporterError::Unread(operation))
            .unwrap();
        Ok(())
    }
}

#[derive(Debug, Clone)]
#[cfg_attr(any(test, feature = "test"), derive(Arbitrary))]
#[cfg_attr(any(test, feature = "test"), arbitrary(args = MockOperationParams))]
pub struct LogOperation {
    #[cfg_attr(any(test, feature = "test"), strategy(any_with::<MockOperation>(*args)))]
    pub operation: MockOperation,
    pub logs: VecDeque<LogMessage>,
}

#[cfg(any(test, feature = "test"))]
pub async fn check_logs<S>(
    mut expected: VecDeque<LogMessage>,
    mut actual: S,
) -> Result<(), TestCaseError>
where
    S: Stream<Item = LogMessage> + Unpin,
{
    while let Some(entry) = actual.next().await {
        pretty_prop_assert_eq!(Some(entry), expected.pop_front());
    }

    prop_assert_eq!(expected.len(), 0, "expected logs {:#?}", expected);
    Ok(())
}

impl LogOperation {
    #[cfg(any(test, feature = "test"))]
    pub async fn check_operation<S: DaemonStore>(self, mut client: S) -> Result<(), TestCaseError> {
        let expected = self.operation.response();
        let request = self.operation.request();
        let actual_log = request.get_response(&mut client);
        let response = expected.map_err(|err| err.to_string());
        let log = actual_log.map_err(|err| err.to_string());
        let mut log = pin!(log);
        let logs: VecDeque<LogMessage> = log.as_mut().collect().await;
        let res = log.await;
        pretty_prop_assert_eq!(res, response);
        pretty_prop_assert_eq!(logs, self.logs);
        Ok(())
    }
}

pin_project! {
    pub struct LogResult<Fut> {
        logs: VecDeque<LogMessage>,
        #[pin]
        result: Fut,
    }
}

impl<Fut> Stream for LogResult<Fut> {
    type Item = LogMessage;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        Poll::Ready(self.project().logs.pop_front())
    }
}

impl<Fut, T, E> Future for LogResult<Fut>
where
    Fut: Future<Output = Result<T, E>>,
{
    type Output = Result<T, E>;

    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        self.project().result.poll(cx)
    }
}

pub trait LogBuild {
    fn add_to_builder<R>(
        self,
        logs: VecDeque<LogMessage>,
        builder: &mut Builder<R>,
    ) -> &mut Builder<R>;
}

impl LogBuild for () {
    fn add_to_builder<R>(
        self,
        logs: VecDeque<LogMessage>,
        builder: &mut Builder<R>,
    ) -> &mut Builder<R> {
        builder.handshake_logs = logs;
        builder
    }
}

impl LogBuild for MockOperation {
    fn add_to_builder<R>(
        self,
        logs: VecDeque<LogMessage>,
        builder: &mut Builder<R>,
    ) -> &mut Builder<R> {
        builder.add_operation(LogOperation {
            operation: self,
            logs,
        });
        builder
    }
}

pub struct LogBuilder<'b, R, O> {
    owner: &'b mut Builder<R>,
    operation: O,
    logs: VecDeque<LogMessage>,
}

impl<R, O> LogBuilder<'_, R, O> {
    pub fn message<M: Into<Message>>(self, msg: M) -> Self {
        let msg = msg.into();
        self.add_log(LogMessage::Message(msg))
    }

    pub fn start_activity(self, act: Activity) -> Self {
        self.add_log(LogMessage::StartActivity(act))
    }

    pub fn stop_activity(self, id: u64) -> Self {
        self.add_log(LogMessage::StopActivity(StopActivity { id }))
    }

    pub fn result(self, result: ActivityResult) -> Self {
        self.add_log(LogMessage::Result(result))
    }

    pub fn add_log(mut self, log: LogMessage) -> Self {
        self.logs.push_back(log);
        self
    }
}

impl<'b, R: Clone, O: LogBuild> LogBuilder<'b, R, O> {
    pub fn build(self) -> &'b mut Builder<R> {
        self.operation.add_to_builder(self.logs, self.owner)
    }
}

pub struct Builder<R> {
    trusted_client: TrustLevel,
    handshake_logs: VecDeque<LogMessage>,
    ops: VecDeque<LogOperation>,
    reporter: R,
}

impl<R> Builder<R> {
    pub fn handshake(&mut self) -> LogBuilder<R, ()> {
        LogBuilder {
            owner: self,
            operation: (),
            logs: VecDeque::new(),
        }
    }

    pub fn add_handshake_log(&mut self, msg: LogMessage) {
        self.handshake_logs.push_back(msg);
    }

    pub fn set_options(
        &mut self,
        options: &super::ClientOptions,
        response: DaemonResult<()>,
    ) -> LogBuilder<R, MockOperation> {
        self.build_operation(MockOperation::SetOptions(options.clone(), response))
    }

    pub fn is_valid_path(
        &mut self,
        path: &StorePath,
        response: DaemonResult<bool>,
    ) -> LogBuilder<R, MockOperation> {
        self.build_operation(MockOperation::IsValidPath(path.clone(), response))
    }

    pub fn query_valid_paths(
        &mut self,
        paths: &StorePathSet,
        substitute: bool,
        response: DaemonResult<StorePathSet>,
    ) -> LogBuilder<R, MockOperation> {
        self.build_operation(MockOperation::QueryValidPaths(
            QueryValidPathsRequest {
                paths: paths.clone(),
                substitute,
            },
            response,
        ))
    }

    pub fn query_path_info(
        &mut self,
        path: &StorePath,
        response: DaemonResult<Option<UnkeyedValidPathInfo>>,
    ) -> LogBuilder<R, MockOperation> {
        self.build_operation(MockOperation::QueryPathInfo(path.clone(), response))
    }

    pub fn nar_from_path(
        &mut self,
        path: &StorePath,
        response: DaemonResult<Bytes>,
    ) -> LogBuilder<R, MockOperation> {
        self.build_operation(MockOperation::NarFromPath(path.clone(), response))
    }

    pub fn build_paths(
        &mut self,
        paths: &[DerivedPath],
        mode: BuildMode,
        response: DaemonResult<()>,
    ) -> LogBuilder<R, MockOperation> {
        self.build_operation(MockOperation::BuildPaths(
            BuildPathsRequest {
                paths: paths.to_vec(),
                mode,
            },
            response,
        ))
    }

    pub fn build_paths_with_results(
        &mut self,
        paths: &[DerivedPath],
        mode: BuildMode,
        response: DaemonResult<KeyedBuildResults>,
    ) -> LogBuilder<R, MockOperation> {
        self.build_operation(MockOperation::BuildPathsWithResults(
            BuildPathsRequest {
                paths: paths.to_vec(),
                mode,
            },
            response,
        ))
    }

    pub fn build_derivation(
        &mut self,
        drv: &BasicDerivation,
        mode: BuildMode,
        response: DaemonResult<BuildResult>,
    ) -> LogBuilder<R, MockOperation> {
        self.build_operation(MockOperation::BuildDerivation(
            BuildDerivationRequest {
                drv: drv.clone(),
                mode,
            },
            response,
        ))
    }

    pub fn query_missing(
        &mut self,
        paths: &[DerivedPath],
        response: DaemonResult<QueryMissingResult>,
    ) -> LogBuilder<R, MockOperation> {
        self.build_operation(MockOperation::QueryMissing(paths.to_vec(), response))
    }

    pub fn add_to_store_nar(
        &mut self,
        info: &ValidPathInfo,
        repair: bool,
        dont_check_sigs: bool,
        contents: Bytes,
        response: DaemonResult<()>,
    ) -> LogBuilder<R, MockOperation> {
        self.build_operation(MockOperation::AddToStoreNar(
            AddToStoreNarRequest {
                path_info: info.clone(),
                repair,
                dont_check_sigs,
            },
            contents,
            response,
        ))
    }

    pub fn add_multiple_to_store(
        &mut self,
        repair: bool,
        dont_check_sigs: bool,
        contents: Vec<(ValidPathInfo, Bytes)>,
        response: DaemonResult<()>,
    ) -> LogBuilder<R, MockOperation> {
        self.build_operation(MockOperation::AddMultipleToStore(
            AddMultipleToStoreRequest {
                repair,
                dont_check_sigs,
            },
            contents,
            response,
        ))
    }

    pub fn query_all_valid_paths(
        &mut self,
        response: DaemonResult<StorePathSet>,
    ) -> LogBuilder<R, MockOperation> {
        self.build_operation(MockOperation::QueryAllValidPaths(response))
    }

    pub fn query_referrers(
        &mut self,
        path: &StorePath,
        response: DaemonResult<StorePathSet>,
    ) -> LogBuilder<R, MockOperation> {
        self.build_operation(MockOperation::QueryReferrers(path.clone(), response))
    }

    pub fn ensure_path(
        &mut self,
        path: &StorePath,
        response: DaemonResult<()>,
    ) -> LogBuilder<R, MockOperation> {
        self.build_operation(MockOperation::EnsurePath(path.clone(), response))
    }

    pub fn add_temp_root(
        &mut self,
        path: &StorePath,
        response: DaemonResult<()>,
    ) -> LogBuilder<R, MockOperation> {
        self.build_operation(MockOperation::AddTempRoot(path.clone(), response))
    }

    pub fn add_indirect_root(
        &mut self,
        path: &DaemonPath,
        response: DaemonResult<()>,
    ) -> LogBuilder<R, MockOperation> {
        self.build_operation(MockOperation::AddIndirectRoot(path.clone(), response))
    }

    pub fn find_roots(
        &mut self,
        response: DaemonResult<BTreeMap<DaemonPath, StorePath>>,
    ) -> LogBuilder<R, MockOperation> {
        self.build_operation(MockOperation::FindRoots(response))
    }

    pub fn collect_garbage(
        &mut self,
        action: GCAction,
        paths_to_delete: &StorePathSet,
        ignore_liveness: bool,
        max_freed: u64,
        response: DaemonResult<CollectGarbageResponse>,
    ) -> LogBuilder<R, MockOperation> {
        let mut actual_req = CollectGarbageRequest::default();
        actual_req.action = action;
        actual_req.paths_to_delete = paths_to_delete.clone();
        actual_req.ignore_liveness = ignore_liveness;
        actual_req.max_freed = max_freed;
        self.build_operation(MockOperation::CollectGarbage(actual_req, response))
    }

    pub fn query_path_from_hash_part(
        &mut self,
        hash: &StorePathHash,
        response: DaemonResult<Option<StorePath>>,
    ) -> LogBuilder<R, MockOperation> {
        self.build_operation(MockOperation::QueryPathFromHashPart(*hash, response))
    }

    pub fn query_substitutable_paths(
        &mut self,
        paths: &StorePathSet,
        response: DaemonResult<StorePathSet>,
    ) -> LogBuilder<R, MockOperation> {
        self.build_operation(MockOperation::QuerySubstitutablePaths(
            paths.clone(),
            response,
        ))
    }

    pub fn query_valid_derivers(
        &mut self,
        path: &StorePath,
        response: DaemonResult<StorePathSet>,
    ) -> LogBuilder<R, MockOperation> {
        self.build_operation(MockOperation::QueryValidDerivers(path.clone(), response))
    }

    pub fn optimise_store(&mut self, response: DaemonResult<()>) -> LogBuilder<R, MockOperation> {
        self.build_operation(MockOperation::OptimiseStore(response))
    }

    pub fn verify_store(
        &mut self,
        check_contents: bool,
        repair: bool,
        response: DaemonResult<bool>,
    ) -> LogBuilder<R, MockOperation> {
        self.build_operation(MockOperation::VerifyStore(
            VerifyStoreRequest {
                check_contents,
                repair,
            },
            response,
        ))
    }

    pub fn add_signatures(
        &mut self,
        path: &StorePath,
        signatures: &[Signature],
        response: DaemonResult<()>,
    ) -> LogBuilder<R, MockOperation> {
        self.build_operation(MockOperation::AddSignatures(
            AddSignaturesRequest {
                path: path.clone(),
                signatures: signatures.to_vec(),
            },
            response,
        ))
    }

    pub fn query_derivation_output_map(
        &mut self,
        path: &StorePath,
        response: DaemonResult<BTreeMap<OutputName, Option<StorePath>>>,
    ) -> LogBuilder<R, MockOperation> {
        self.build_operation(MockOperation::QueryDerivationOutputMap(
            path.clone(),
            response,
        ))
    }

    pub fn register_drv_output(
        &mut self,
        realisation: &Realisation,
        response: DaemonResult<()>,
    ) -> LogBuilder<R, MockOperation> {
        self.build_operation(MockOperation::RegisterDrvOutput(
            realisation.clone(),
            response,
        ))
    }

    pub fn query_realisation(
        &mut self,
        output_id: &DrvOutput,
        response: DaemonResult<BTreeSet<Realisation>>,
    ) -> LogBuilder<R, MockOperation> {
        self.build_operation(MockOperation::QueryRealisation(output_id.clone(), response))
    }

    pub fn add_build_log(
        &mut self,
        path: &StorePath,
        log: Bytes,
        response: DaemonResult<()>,
    ) -> LogBuilder<R, MockOperation> {
        self.build_operation(MockOperation::AddBuildLog(path.clone(), log, response))
    }

    pub fn add_perm_root(
        &mut self,
        path: &StorePath,
        gc_root: &DaemonPath,
        response: DaemonResult<DaemonPath>,
    ) -> LogBuilder<R, MockOperation> {
        self.build_operation(MockOperation::AddPermRoot(
            AddPermRootRequest {
                store_path: path.clone(),
                gc_root: gc_root.clone(),
            },
            response,
        ))
    }

    pub fn sync_with_gc(&mut self, response: DaemonResult<()>) -> LogBuilder<R, MockOperation> {
        self.build_operation(MockOperation::SyncWithGC(response))
    }

    pub fn query_derivation_outputs(
        &mut self,
        path: &StorePath,
        response: DaemonResult<StorePathSet>,
    ) -> LogBuilder<R, MockOperation> {
        self.build_operation(MockOperation::QueryDerivationOutputs(
            path.clone(),
            response,
        ))
    }

    pub fn query_derivation_output_names(
        &mut self,
        path: &StorePath,
        response: DaemonResult<BTreeSet<OutputName>>,
    ) -> LogBuilder<R, MockOperation> {
        self.build_operation(MockOperation::QueryDerivationOutputNames(
            path.clone(),
            response,
        ))
    }

    pub fn add_ca_to_store(
        &mut self,
        name: &str,
        cam: ContentAddressMethodAlgorithm,
        refs: &StorePathSet,
        repair: bool,
        content: Bytes,
        response: DaemonResult<ValidPathInfo>,
    ) -> LogBuilder<R, MockOperation> {
        self.build_operation(MockOperation::AddToStore(
            AddToStoreRequest25 {
                name: name.to_string(),
                cam,
                refs: refs.clone(),
                repair,
            },
            content,
            response,
        ))
    }

    fn build_operation(&mut self, operation: MockOperation) -> LogBuilder<R, MockOperation> {
        LogBuilder {
            owner: self,
            operation,
            logs: VecDeque::new(),
        }
    }

    pub fn add_operation(&mut self, operation: LogOperation) -> &mut Self {
        self.ops.push_back(operation);
        self
    }

    pub fn channel_reporter(
        &self,
    ) -> (
        Builder<ChannelReporter>,
        mpsc::UnboundedReceiver<ReporterError>,
    ) {
        let (sender, receiver) = mpsc::unbounded();
        (self.set_reporter(ChannelReporter(sender)), receiver)
    }

    pub fn set_reporter<R2>(&self, reporter: R2) -> Builder<R2> {
        Builder {
            trusted_client: self.trusted_client,
            handshake_logs: self.handshake_logs.clone(),
            ops: self.ops.clone(),
            reporter,
        }
    }
}

impl<R> Builder<R>
where
    R: MockReporter + Clone,
{
    pub fn build(&self) -> MockStore<R> {
        MockStore {
            trusted_client: self.trusted_client,
            handshake_logs: self.handshake_logs.clone(),
            ops: self.ops.clone(),
            reporter: self.reporter.clone(),
        }
    }
}

impl Builder<()> {
    pub fn new() -> Self {
        Builder {
            trusted_client: TrustLevel::Unknown,
            ops: Default::default(),
            handshake_logs: Default::default(),
            reporter: (),
        }
    }
}

impl Default for Builder<()> {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug)]
pub struct MockStore<R>
where
    R: MockReporter,
{
    trusted_client: TrustLevel,
    handshake_logs: VecDeque<LogMessage>,
    ops: VecDeque<LogOperation>,
    reporter: R,
}

impl<R> MockStore<R>
where
    R: MockReporter,
{
    fn check_operation<O>(
        &mut self,
        actual: MockRequest,
    ) -> impl ResultLog<Output = DaemonResult<O>> + Send + '_
    where
        MockResponse: Into<O>,
        O: 'static,
    {
        let response = match self.ops.pop_front() {
            None => Either::Left(Either::Left(self.reporter.extra_operation(actual))),
            Some(LogOperation {
                operation: expected,
                logs,
            }) => {
                if expected.operation() == actual.operation() {
                    if actual != expected.request() {
                        Either::Right(Either::Left(
                            self.reporter.invalid_operation(expected, actual),
                        ))
                    } else {
                        Either::Right(Either::Right(LogResult {
                            logs,
                            result: ready(expected.response()),
                        }))
                    }
                } else {
                    Either::Left(Either::Right(
                        self.reporter.unexpected_operation(expected, actual),
                    ))
                }
            }
        };
        response.map_ok(|v| v.into())
    }
}

impl MockStore<()> {
    pub fn new() -> MockStore<()> {
        Default::default()
    }

    pub fn builder() -> Builder<()> {
        Builder::default()
    }
}

impl Default for MockStore<()> {
    fn default() -> Self {
        Self::builder().build()
    }
}

impl<R> Drop for MockStore<R>
where
    R: MockReporter,
{
    fn drop(&mut self) {
        // No need to panic again
        if thread::panicking() {
            return;
        }
        for op in self.ops.drain(..) {
            self.reporter.unread_operation(op).unwrap();
        }
    }
}

impl<R> HandshakeDaemonStore for MockStore<R>
where
    R: MockReporter + Send + 'static,
{
    type Store = Self;

    fn handshake(mut self) -> impl ResultLog<Output = DaemonResult<Self::Store>> {
        let logs = take(&mut self.handshake_logs);
        ResultProcess {
            stream: iter(logs),
            result: ready(Ok(self)),
        }
    }
}

impl<R> DaemonStore for MockStore<R>
where
    R: MockReporter + Send,
{
    fn trust_level(&self) -> TrustLevel {
        self.trusted_client
    }

    fn set_options<'a>(
        &'a mut self,
        options: &'a super::ClientOptions,
    ) -> impl super::ResultLog<Output = DaemonResult<()>> + 'a {
        let actual = MockRequest::SetOptions(options.clone());
        self.check_operation(actual)
    }

    fn is_valid_path<'a>(
        &'a mut self,
        path: &'a StorePath,
    ) -> impl super::ResultLog<Output = DaemonResult<bool>> + 'a {
        let actual = MockRequest::IsValidPath(path.clone());
        self.check_operation(actual)
    }

    fn query_valid_paths<'a>(
        &'a mut self,
        paths: &'a StorePathSet,
        substitute: bool,
    ) -> impl super::ResultLog<Output = DaemonResult<StorePathSet>> + 'a {
        let actual = MockRequest::QueryValidPaths(QueryValidPathsRequest {
            paths: paths.clone(),
            substitute,
        });
        self.check_operation(actual)
    }

    fn query_path_info<'a>(
        &'a mut self,
        path: &'a StorePath,
    ) -> impl super::logger::ResultLog<Output = DaemonResult<Option<UnkeyedValidPathInfo>>> + 'a
    {
        let actual = MockRequest::QueryPathInfo(path.clone());
        self.check_operation(actual)
    }

    fn nar_from_path<'a>(
        &'a mut self,
        path: &'a StorePath,
    ) -> impl super::logger::ResultLog<Output = DaemonResult<impl AsyncBufRead + use<R>>> + 'a {
        let actual = MockRequest::NarFromPath(path.clone());
        self.check_operation(actual)
            .and_then(move |bytes: Bytes| async move { Ok(Cursor::new(bytes)) })
    }

    fn build_paths<'a>(
        &'a mut self,
        paths: &'a [DerivedPath],
        mode: BuildMode,
    ) -> impl ResultLog<Output = DaemonResult<()>> + 'a {
        let actual = MockRequest::BuildPaths(BuildPathsRequest {
            paths: paths.to_vec(),
            mode,
        });
        self.check_operation(actual)
    }

    fn build_paths_with_results<'a>(
        &'a mut self,
        drvs: &'a [DerivedPath],
        mode: BuildMode,
    ) -> impl ResultLog<Output = DaemonResult<Vec<super::wire::types2::KeyedBuildResult>>> + Send + 'a
    {
        let actual = MockRequest::BuildPathsWithResults(BuildPathsRequest {
            paths: drvs.to_vec(),
            mode,
        });
        self.check_operation(actual)
    }

    fn build_derivation<'a>(
        &'a mut self,
        drv: &'a BasicDerivation,
        mode: BuildMode,
    ) -> impl ResultLog<Output = DaemonResult<BuildResult>> + 'a {
        let actual = MockRequest::BuildDerivation(BuildDerivationRequest {
            drv: drv.clone(),
            mode,
        });
        self.check_operation(actual)
    }

    fn query_missing<'a>(
        &'a mut self,
        paths: &'a [DerivedPath],
    ) -> impl ResultLog<Output = DaemonResult<QueryMissingResult>> + 'a {
        let actual = MockRequest::QueryMissing(paths.to_vec());
        self.check_operation(actual)
    }

    fn add_to_store_nar<'s, 'r, 'i, AR>(
        &'s mut self,
        info: &'i ValidPathInfo,
        mut source: AR,
        repair: bool,
        dont_check_sigs: bool,
    ) -> Pin<Box<dyn ResultLog<Output = DaemonResult<()>> + Send + 'r>>
    where
        AR: AsyncBufRead + Send + Unpin + 'r,
        's: 'r,
        'i: 'r,
    {
        Box::pin(FutureResult::new(async move {
            let actual_req = AddToStoreNarRequest {
                path_info: info.clone(),
                repair,
                dont_check_sigs,
            };
            let mut actual_nar = Vec::new();
            source.read_to_end(&mut actual_nar).await?;
            let actual = MockRequest::AddToStoreNar(actual_req.clone(), actual_nar.clone().into());
            Ok(self.check_operation(actual))
        }))
    }

    fn add_multiple_to_store<'s, 'i, 'r, S, SR>(
        &'s mut self,
        repair: bool,
        dont_check_sigs: bool,
        stream: S,
    ) -> Pin<Box<dyn ResultLog<Output = DaemonResult<()>> + Send + 'r>>
    where
        S: Stream<Item = Result<AddToStoreItem<SR>, DaemonError>> + Send + 'i,
        SR: tokio::io::AsyncBufRead + Send + Unpin + 'i,
        's: 'r,
        'i: 'r,
    {
        async move {
            let actual_req = AddMultipleToStoreRequest {
                repair,
                dont_check_sigs,
            };
            trace!("Size of raw stream {}", size_of_val(&stream));
            let fut = stream
                .and_then(|mut info| async move {
                    let mut nar = Vec::new();
                    info.reader.read_to_end(&mut nar).await?;
                    Ok((info.info, nar.into()))
                })
                .try_collect();
            trace!("Size of stream {}", size_of_val(&fut));
            let actual_infos = fut.await?;
            let actual = MockRequest::AddMultipleToStore(actual_req.clone(), actual_infos);
            Ok(self.check_operation(actual))
        }
        .future_result()
        .boxed_result()
    }

    fn query_all_valid_paths(
        &mut self,
    ) -> impl ResultLog<Output = DaemonResult<StorePathSet>> + Send + '_ {
        let actual = MockRequest::QueryAllValidPaths;
        self.check_operation(actual)
    }

    fn query_referrers<'a>(
        &'a mut self,
        path: &'a StorePath,
    ) -> impl ResultLog<Output = DaemonResult<StorePathSet>> + Send + 'a {
        let actual = MockRequest::QueryReferrers(path.clone());
        self.check_operation(actual)
    }

    fn ensure_path<'a>(
        &'a mut self,
        path: &'a StorePath,
    ) -> impl ResultLog<Output = DaemonResult<()>> + Send + 'a {
        let actual = MockRequest::EnsurePath(path.clone());
        self.check_operation(actual)
    }

    fn add_temp_root<'a>(
        &'a mut self,
        path: &'a StorePath,
    ) -> impl ResultLog<Output = DaemonResult<()>> + Send + 'a {
        let actual = MockRequest::AddTempRoot(path.clone());
        self.check_operation(actual)
    }

    fn add_indirect_root<'a>(
        &'a mut self,
        path: &'a DaemonPath,
    ) -> impl ResultLog<Output = DaemonResult<()>> + Send + 'a {
        let actual = MockRequest::AddIndirectRoot(path.clone());
        self.check_operation(actual)
    }

    fn find_roots(
        &mut self,
    ) -> impl ResultLog<Output = DaemonResult<BTreeMap<DaemonPath, StorePath>>> + Send + '_ {
        let actual = MockRequest::FindRoots;
        self.check_operation(actual)
    }

    fn collect_garbage<'a>(
        &'a mut self,
        action: GCAction,
        paths_to_delete: &'a StorePathSet,
        ignore_liveness: bool,
        max_freed: u64,
    ) -> impl ResultLog<Output = DaemonResult<CollectGarbageResponse>> + Send + 'a {
        let mut actual_req = CollectGarbageRequest::default();
        actual_req.action = action;
        actual_req.paths_to_delete = paths_to_delete.clone();
        actual_req.ignore_liveness = ignore_liveness;
        actual_req.max_freed = max_freed;
        let actual = MockRequest::CollectGarbage(actual_req);
        self.check_operation(actual)
    }

    fn query_path_from_hash_part<'a>(
        &'a mut self,
        hash: &'a StorePathHash,
    ) -> impl ResultLog<Output = DaemonResult<Option<StorePath>>> + Send + 'a {
        let actual = MockRequest::QueryPathFromHashPart(*hash);
        self.check_operation(actual)
    }

    fn query_substitutable_paths<'a>(
        &'a mut self,
        paths: &'a StorePathSet,
    ) -> impl ResultLog<Output = DaemonResult<StorePathSet>> + Send + 'a {
        let actual = MockRequest::QuerySubstitutablePaths(paths.clone());
        self.check_operation(actual)
    }

    fn query_valid_derivers<'a>(
        &'a mut self,
        path: &'a StorePath,
    ) -> impl ResultLog<Output = DaemonResult<StorePathSet>> + Send + 'a {
        let actual = MockRequest::QueryValidDerivers(path.clone());
        self.check_operation(actual)
    }

    fn optimise_store(&mut self) -> impl ResultLog<Output = DaemonResult<()>> + Send + '_ {
        let actual = MockRequest::OptimiseStore;
        self.check_operation(actual)
    }

    fn verify_store(
        &mut self,
        check_contents: bool,
        repair: bool,
    ) -> impl ResultLog<Output = DaemonResult<bool>> + Send + '_ {
        let actual = MockRequest::VerifyStore(VerifyStoreRequest {
            check_contents,
            repair,
        });
        self.check_operation(actual)
    }

    fn add_signatures<'a>(
        &'a mut self,
        path: &'a StorePath,
        signatures: &'a [Signature],
    ) -> impl ResultLog<Output = DaemonResult<()>> + Send + 'a {
        let actual = MockRequest::AddSignatures(AddSignaturesRequest {
            path: path.clone(),
            signatures: signatures.to_vec(),
        });
        self.check_operation(actual)
    }

    fn query_derivation_output_map<'a>(
        &'a mut self,
        path: &'a StorePath,
    ) -> impl ResultLog<Output = DaemonResult<BTreeMap<OutputName, Option<StorePath>>>> + Send + 'a
    {
        let actual = MockRequest::QueryDerivationOutputMap(path.clone());
        self.check_operation(actual)
    }

    fn register_drv_output<'a>(
        &'a mut self,
        realisation: &'a Realisation,
    ) -> impl ResultLog<Output = DaemonResult<()>> + Send + 'a {
        let actual = MockRequest::RegisterDrvOutput(realisation.clone());
        self.check_operation(actual)
    }

    fn query_realisation<'a>(
        &'a mut self,
        output_id: &'a DrvOutput,
    ) -> impl ResultLog<Output = DaemonResult<BTreeSet<Realisation>>> + Send + 'a {
        let actual = MockRequest::QueryRealisation(output_id.clone());
        self.check_operation(actual)
    }

    fn add_build_log<'s, 'r, 'p, S>(
        &'s mut self,
        path: &'p StorePath,
        mut source: S,
    ) -> Pin<Box<dyn ResultLog<Output = DaemonResult<()>> + Send + 'r>>
    where
        S: AsyncBufRead + Send + Unpin + 'r,
        's: 'r,
        'p: 'r,
    {
        async move {
            let mut actual_log = Vec::new();
            source.read_to_end(&mut actual_log).await?;
            let actual = MockRequest::AddBuildLog(path.clone(), actual_log.clone().into());
            Ok(self.check_operation(actual))
        }
        .future_result()
        .boxed_result()
    }

    fn add_perm_root<'a>(
        &'a mut self,
        path: &'a StorePath,
        gc_root: &'a DaemonPath,
    ) -> impl ResultLog<Output = DaemonResult<DaemonPath>> + Send + 'a {
        let actual = MockRequest::AddPermRoot(AddPermRootRequest {
            store_path: path.clone(),
            gc_root: gc_root.clone(),
        });
        self.check_operation(actual)
    }

    fn sync_with_gc(&mut self) -> impl ResultLog<Output = DaemonResult<()>> + Send + '_ {
        let actual = MockRequest::SyncWithGC;
        self.check_operation(actual)
    }

    fn query_derivation_outputs<'a>(
        &'a mut self,
        path: &'a StorePath,
    ) -> impl ResultLog<Output = DaemonResult<StorePathSet>> + Send + 'a {
        let actual = MockRequest::QueryDerivationOutputs(path.clone());
        self.check_operation(actual)
    }

    fn query_derivation_output_names<'a>(
        &'a mut self,
        path: &'a StorePath,
    ) -> impl ResultLog<Output = DaemonResult<BTreeSet<OutputName>>> + Send + 'a {
        let actual = MockRequest::QueryDerivationOutputNames(path.clone());
        self.check_operation(actual)
    }

    fn add_ca_to_store<'a, 'r, S>(
        &'a mut self,
        name: &'a str,
        cam: ContentAddressMethodAlgorithm,
        refs: &'a StorePathSet,
        repair: bool,
        mut source: S,
    ) -> Pin<Box<dyn ResultLog<Output = DaemonResult<ValidPathInfo>> + Send + 'r>>
    where
        S: AsyncBufRead + Send + Unpin + 'r,
        'a: 'r,
    {
        async move {
            let actual_req = AddToStoreRequest25 {
                name: name.to_string(),
                cam,
                refs: refs.clone(),
                repair,
            };
            let mut actual_content = Vec::new();
            source.read_to_end(&mut actual_content).await?;
            let actual = MockRequest::AddToStore(actual_req, actual_content.clone().into());
            Ok(self.check_operation(actual))
        }
        .future_result()
        .boxed_result()
    }

    async fn shutdown(&mut self) -> DaemonResult<()> {
        let mut res = Ok(());
        for op in self.ops.drain(..) {
            if let Err(err) = self.reporter.unread_operation(op) {
                if res.is_ok() {
                    res = Err(err);
                }
            }
        }
        res
    }
}

#[cfg(any(test, feature = "test"))]
pub mod arbitrary {
    use std::ops::RangeBounds;

    use proptest::prelude::*;

    use crate::daemon::wire::types2::KeyedBuildResult;
    use crate::daemon::{ClientOptions, ProtocolVersion};
    use crate::store_path::{StorePath, StorePathSet};
    use crate::test::arbitrary::archive::arb_nar_contents;
    use crate::test::arbitrary::daemon::{arb_nar_contents_items, field_after};
    use crate::test::arbitrary::helpers::Union;

    use super::*;

    prop_compose! {
        fn arb_mock_set_options()(options in any::<ClientOptions>()) -> MockOperation {
            MockOperation::SetOptions(options, Ok(()))
        }
    }
    prop_compose! {
        fn arb_mock_is_valid_path()(
            path in any::<StorePath>(),
            result in proptest::bool::ANY) -> MockOperation {
            MockOperation::IsValidPath(path, Ok(result))
        }
    }

    prop_compose! {
        fn arb_mock_query_valid_paths(version: ProtocolVersion)(
            paths in any::<StorePathSet>(),
            substitute in field_after(version, 27, proptest::bool::ANY),
            result in any::<StorePathSet>()) -> MockOperation {
            MockOperation::QueryValidPaths(QueryValidPathsRequest {
                paths, substitute
            }, Ok(result))
        }
    }

    prop_compose! {
        fn arb_mock_query_path_info()(
            path in any::<StorePath>(),
            result in any::<Option<UnkeyedValidPathInfo>>()) -> MockOperation {
            MockOperation::QueryPathInfo(path, Ok(result))
        }
    }
    prop_compose! {
        fn arb_mock_nar_from_path()(
            path in any::<StorePath>(),
            result in arb_nar_contents(20, 20, 3)) -> MockOperation {
            MockOperation::NarFromPath(path, Ok(result))
        }
    }
    prop_compose! {
        fn arb_mock_build_paths()(
            paths in any::<Vec<DerivedPath>>(),
            mode in any::<BuildMode>()) -> MockOperation {
            MockOperation::BuildPaths(BuildPathsRequest { paths, mode }, Ok(()))
        }
    }
    prop_compose! {
        fn arb_mock_build_paths_with_results(version: ProtocolVersion)(
            results in any_with::<Vec<KeyedBuildResult>>((Default::default(), version)),
            mode in any::<BuildMode>()) -> MockOperation {
            let paths = results.iter().map(|r| r.path.clone()).collect();
            MockOperation::BuildPathsWithResults(BuildPathsRequest { paths, mode }, Ok(results))
        }
    }

    prop_compose! {
        fn arb_mock_build_derivation(version: ProtocolVersion)(
            drv in any::<BasicDerivation>(),
            mode in any::<BuildMode>(),
            result in any_with::<BuildResult>(version)) -> MockOperation {
            MockOperation::BuildDerivation(BuildDerivationRequest { drv, mode }, Ok(result))
        }
    }
    prop_compose! {
        fn arb_mock_query_missing()(
            paths in any::<Vec<DerivedPath>>(),
            result in any::<QueryMissingResult>()) -> MockOperation {
            MockOperation::QueryMissing(paths, Ok(result))
        }
    }
    prop_compose! {
        fn arb_mock_add_to_store_nar()(
            path_info in any::<ValidPathInfo>(),
            repair in proptest::bool::ANY,
            dont_check_sigs in proptest::bool::ANY,
            content in arb_nar_contents(20, 20, 3)) -> MockOperation {
            MockOperation::AddToStoreNar(AddToStoreNarRequest {
                path_info, repair, dont_check_sigs
            }, content, Ok(()))
        }
    }
    prop_compose! {
        fn arb_mock_add_multiple_to_store()(
            repair in proptest::bool::ANY,
            dont_check_sigs in proptest::bool::ANY,
            infos in arb_nar_contents_items()) -> MockOperation {
            MockOperation::AddMultipleToStore(AddMultipleToStoreRequest {
                repair, dont_check_sigs
            }, infos, Ok(()))
        }
    }

    #[derive(Debug, Clone, Copy)]
    pub struct MockOperationParams {
        pub version: ProtocolVersion,
        pub allow_options: bool,
    }

    impl Default for MockOperationParams {
        fn default() -> Self {
            Self {
                version: Default::default(),
                allow_options: true,
            }
        }
    }

    impl Arbitrary for MockOperation {
        type Parameters = MockOperationParams;
        type Strategy = Union<BoxedStrategy<Self>>;

        fn arbitrary_with(args: Self::Parameters) -> Self::Strategy {
            let mut ret = Union::new([
                arb_mock_is_valid_path().boxed(),
                arb_mock_query_valid_paths(args.version).boxed(),
                arb_mock_query_path_info().boxed(),
                arb_mock_nar_from_path().boxed(),
                arb_mock_build_paths().boxed(),
                arb_mock_build_derivation(args.version).boxed(),
                arb_mock_add_to_store_nar().boxed(),
            ]);
            if args.allow_options {
                ret = ret.or(arb_mock_set_options().boxed());
            }
            if Operation::BuildPathsWithResults
                .versions()
                .contains(&args.version)
            {
                ret = ret.or(arb_mock_build_paths_with_results(args.version).boxed());
            }
            if Operation::AddMultipleToStore
                .versions()
                .contains(&args.version)
            {
                ret = ret.or(arb_mock_add_multiple_to_store().boxed());
            }
            ret
        }
    }
}

#[cfg(test)]
mod unittests {
    use super::*;

    #[tokio::test]
    async fn check_reporter() {
        let path = "00000000000000000000000000000000-_".parse().unwrap();
        let mut mock = MockStore::builder()
            .is_valid_path(&path, Ok(true))
            .build()
            .build();
        mock.is_valid_path(&path).await.unwrap();
    }

    #[tokio::test]
    #[should_panic(expected = "operation still unread")]
    async fn check_unsent_reporter() {
        let path = "00000000000000000000000000000000-_".parse().unwrap();
        let _mock = MockStore::builder()
            .is_valid_path(&path, Ok(true))
            .build()
            .build();
    }

    #[tokio::test]
    async fn check_channel_reporter() {
        let path = "00000000000000000000000000000000-_".parse().unwrap();
        let (mock, mut reporter) = MockStore::builder()
            .is_valid_path(&path, Ok(true))
            .build()
            .channel_reporter();
        let _test = async move {
            let mut mock = mock.build();
            mock.is_valid_path(&path).await.unwrap();
        }
        .await;
        if let Some(err) = reporter.next().await {
            panic!("{}", err);
        }
    }

    #[tokio::test]
    async fn check_unsent_channel_reporter_no_report() {
        let path = "00000000000000000000000000000000-_".parse().unwrap();
        let (mock, _reporter) = MockStore::builder()
            .is_valid_path(&path, Ok(true))
            .build()
            .channel_reporter();
        let _mock = mock.build();
    }

    #[tokio::test]
    #[should_panic(expected = "channel reported: store dropped with LogOperation")]
    async fn check_unsent_channel_reporter() {
        let path = "00000000000000000000000000000000-_".parse().unwrap();
        let (mock, mut reporter) = MockStore::builder()
            .is_valid_path(&path, Ok(true))
            .build()
            .is_valid_path(&path, Ok(true))
            .build()
            .channel_reporter();
        let _test = async move {
            let mut mock = mock.build();
            mock.is_valid_path(&path).await.unwrap();
        }
        .await;
        if let Some(err) = reporter.next().await {
            panic!("channel reported: {err}");
        }
    }
}
