use std::collections::VecDeque;
use std::future::{ready, Future};
use std::io::Cursor;
use std::mem::take;
use std::pin::{pin, Pin};
use std::task::Poll;
use std::{fmt, thread};

use arbitrary::MockOperationParams;
use bytes::Bytes;
use futures::channel::mpsc;
use futures::future::Either;
use futures::stream::empty;
use futures::stream::{iter, TryStreamExt};
use futures::Stream;
#[cfg(any(test, feature = "test"))]
use futures::StreamExt as _;
use pin_project_lite::pin_project;
#[cfg(any(test, feature = "test"))]
use proptest::prelude::*;
#[cfg(any(test, feature = "test"))]
use proptest::prop_assert_eq;
#[cfg(any(test, feature = "test"))]
use test_strategy::Arbitrary;
use tokio::io::{AsyncBufRead, AsyncReadExt as _};
use tracing::trace;

use super::logger::{
    Activity, ActivityResult, FutureResult, LogMessage, ResultLogExt as _, ResultProcess,
};
use super::types::AddToStoreItem;
use super::wire::types::Operation;
use super::wire::types2::{
    AddMultipleToStoreRequest, AddToStoreNarRequest, BuildDerivationRequest, BuildMode,
    BuildPathsRequest, BuildResult, KeyedBuildResults, QueryMissingResult, QueryValidPathsRequest,
    ValidPathInfo,
};
use super::{
    ClientOptions, DaemonError, DaemonResult, DaemonResultExt, DaemonStore, DaemonString,
    HandshakeDaemonStore, ResultLog, TrustLevel, UnkeyedValidPathInfo,
};
use crate::derivation::BasicDerivation;
use crate::derived_path::DerivedPath;
#[cfg(any(test, feature = "test"))]
use crate::pretty_prop_assert_eq;
use crate::store_path::{StorePath, StorePathSet};

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
        }
    }
}

enum ResponseResultLog<F1, F2, F3, F4, F5, F6, F7, F8, F9, F10, F11, F12> {
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
}

impl<F1, F2, F3, F4, F5, F6, F7, F8, F9, F10, F11, F12>
    ResponseResultLog<F1, F2, F3, F4, F5, F6, F7, F8, F9, F10, F11, F12>
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
            }
        }
    }
}

impl<F1, F2, F3, F4, F5, F6, F7, F8, F9, F10, F11, F12> Stream
    for ResponseResultLog<F1, F2, F3, F4, F5, F6, F7, F8, F9, F10, F11, F12>
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
        }
    }
}
impl<F1, F2, F3, F4, F5, F6, F7, F8, F9, F10, F11, F12, R> Future
    for ResponseResultLog<F1, F2, F3, F4, F5, F6, F7, F8, F9, F10, F11, F12>
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
    ValidPathInfo(Option<UnkeyedValidPathInfo>),
    QueryMissingResult(QueryMissingResult),
}

impl MockResponse {
    pub fn unwrap_empty(self) {
        match self {
            Self::Empty => (),
            _ => panic!("Unexpected response {:?}", self),
        }
    }

    pub fn unwrap_bool(self) -> bool {
        match self {
            Self::Bool(val) => val,
            _ => panic!("Unexpected response {:?}", self),
        }
    }

    pub fn unwrap_store_path_set(self) -> StorePathSet {
        match self {
            Self::StorePathSet(val) => val,
            _ => panic!("Unexpected response {:?}", self),
        }
    }

    pub fn unwrap_build_result(self) -> BuildResult {
        match self {
            Self::BuildResult(val) => val,
            _ => panic!("Unexpected response {:?}", self),
        }
    }

    pub fn unwrap_keyed_build_results(self) -> KeyedBuildResults {
        match self {
            Self::KeyedBuildResults(val) => val,
            _ => panic!("Unexpected response {:?}", self),
        }
    }

    pub fn unwrap_bytes(self) -> Bytes {
        match self {
            Self::Bytes(val) => val,
            _ => panic!("Unexpected response {:?}", self),
        }
    }

    pub fn unwrap_valid_path_info(self) -> Option<UnkeyedValidPathInfo> {
        match self {
            Self::ValidPathInfo(val) => val,
            _ => panic!("Unexpected response {:?}", self),
        }
    }

    pub fn unwrap_query_missing_result(self) -> QueryMissingResult {
        match self {
            Self::QueryMissingResult(val) => val,
            _ => panic!("Unexpected response {:?}", self),
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
        MockResponse::ValidPathInfo(v)
    }
}
impl From<MockResponse> for Option<UnkeyedValidPathInfo> {
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
                Err(DaemonError::custom(format!("Extra operation {:?}", actual)))
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
                write!(f, "Extra operation {:?}", actual)
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
#[arbitrary(args = MockOperationParams)]
pub struct LogOperation {
    #[any(*args)]
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
    pub fn message<M: Into<DaemonString>>(self, msg: M) -> Self {
        let msg = msg.into();
        self.add_log(LogMessage::Next(msg))
    }

    pub fn start_activity(self, act: Activity) -> Self {
        self.add_log(LogMessage::StartActivity(act))
    }

    pub fn stop_activity(self, act: u64) -> Self {
        self.add_log(LogMessage::StopActivity(act))
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
    ) -> impl super::logger::ResultLog<Output = DaemonResult<impl AsyncBufRead + 'a>> + 'a {
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
    ) -> impl ResultLog<Output = DaemonResult<()>> + Send + 'r
    where
        AR: tokio::io::AsyncRead + Send + Unpin + 'r,
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
    ) -> impl ResultLog<Output = DaemonResult<()>> + Send + 'r
    where
        S: Stream<Item = Result<AddToStoreItem<SR>, DaemonError>> + Send + 'i,
        SR: tokio::io::AsyncBufRead + Send + Unpin + 'i,
        's: 'r,
        'i: 'r,
    {
        FutureResult::new(async move {
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
        })
    }

    fn query_all_valid_paths(
        &mut self,
    ) -> impl ResultLog<Output = DaemonResult<StorePathSet>> + Send + '_ {
        let actual = MockRequest::QueryAllValidPaths;
        self.check_operation(actual)
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
            panic!("channel reported: {}", err);
        }
    }
}
