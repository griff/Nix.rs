use std::future::ready;
use std::io::Cursor;
use std::mem::take;
use std::pin::pin;
use std::task::Poll;
use std::{collections::VecDeque, future::Future};
use std::{fmt, thread};

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
use proptest::prelude::TestCaseError;
#[cfg(any(test, feature = "test"))]
use proptest::{prop_assert, prop_assert_eq};
use tokio::io::{AsyncBufRead, AsyncReadExt as _};
use tracing::trace;

use super::logger::{
    Activity, ActivityResult, FutureResult, LogMessage, ResultLogExt as _, ResultProcess,
};
use super::types::AddToStoreItem;
use super::wire::types::Operation;
use super::wire::types2::{
    AddMultipleToStoreRequest, AddToStoreNarRequest, BasicDerivation, BuildDerivationRequest,
    BuildMode, BuildPathsRequest, BuildResult, DerivedPath, KeyedBuildResults, QueryMissingResult,
    QueryValidPathsRequest, ValidPathInfo,
};
use super::{
    ClientOptions, DaemonError, DaemonErrorKind, DaemonResult, DaemonResultExt, DaemonStore,
    DaemonString, HandshakeDaemonStore, ResultLog, TrustLevel, UnkeyedValidPathInfo,
};
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
            Self::BuildDerivation(_, result) => result.clone().map(|value| value.into()),
            Self::QueryMissing(_, result) => result.clone().map(|value| value.into()),
            Self::AddToStoreNar(_, _, result) => result.clone().map(|value| value.into()),
            Self::AddMultipleToStore(_, _, result) => result.clone().map(|value| value.into()),
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
            Self::SetOptions(options) => Either::Left(Either::Left(Either::Left(Either::Left(
                store.set_options(options).map_ok(|value| value.into()),
            )))),
            Self::IsValidPath(path) => Either::Left(Either::Left(Either::Left(Either::Right(
                store.is_valid_path(path).map_ok(From::from),
            )))),
            Self::QueryValidPaths(request) => {
                Either::Left(Either::Left(Either::Right(Either::Left(
                    store
                        .query_valid_paths(&request.paths, request.substitute)
                        .map_ok(From::from),
                ))))
            }
            Self::QueryPathInfo(path) => Either::Left(Either::Left(Either::Right(Either::Right(
                store.query_path_info(path).map_ok(From::from),
            )))),
            Self::NarFromPath(path) => Either::Left(Either::Right(Either::Left(Either::Left(
                store.nar_from_path(path).and_then(|reader| async move {
                    let mut reader = pin!(reader);
                    let mut out = Vec::new();
                    reader.read_to_end(&mut out).await?;
                    Ok(From::from(Bytes::from(out)))
                }),
            )))),
            Self::BuildPaths(request) => Either::Left(Either::Right(Either::Left(Either::Right(
                store
                    .build_paths(&request.paths, request.mode)
                    .map_ok(From::from),
            )))),
            Self::BuildDerivation(request) => {
                Either::Left(Either::Right(Either::Right(Either::Left(
                    store
                        .build_derivation(&request.drv_path, &request.drv, request.build_mode)
                        .map_ok(From::from),
                ))))
            }
            Self::QueryMissing(paths) => Either::Left(Either::Right(Either::Right(Either::Right(
                store.query_missing(paths).map_ok(From::from),
            )))),
            Self::AddToStoreNar(request, source) => Either::Right(Either::Left(Either::Left(
                store
                    .add_to_store_nar(
                        &request.path_info,
                        Cursor::new(source),
                        request.repair,
                        request.dont_check_sigs,
                    )
                    .map_ok(|value| value.into()),
            ))),
            Self::AddMultipleToStore(request, stream) => {
                Either::Right(Either::Left(Either::Right(
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
                )))
            }
            Self::QueryAllValidPaths => Either::Right(Either::Right(
                store.query_all_valid_paths().map_ok(From::from),
            )),
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
    fn unread_operation(&mut self, operation: LogOperation);
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
                Err(DaemonErrorKind::Custom(format!(
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
                Err(DaemonErrorKind::Custom(format!(
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
                Err(DaemonErrorKind::Custom(format!(
                    "Extra operation {:?}",
                    actual
                )))
                .with_operation(actual.operation()),
            ),
        }
    }

    fn unread_operation(&mut self, _operation: LogOperation) {
        //panic!("store dropped with {operation:?} operation still unread");
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
        let ret = Err(DaemonErrorKind::Custom(report.to_string())).with_operation(op);
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
        let ret = Err(DaemonErrorKind::Custom(report.to_string())).with_operation(op);
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
        let ret = Err(DaemonErrorKind::Custom(report.to_string())).with_operation(op);
        self.0.unbounded_send(report).unwrap();
        ResultProcess {
            stream: empty(),
            result: ready(ret),
        }
    }

    fn unread_operation(&mut self, operation: LogOperation) {
        self.0
            .unbounded_send(ReporterError::Unread(operation))
            .unwrap();
    }
}

#[derive(Debug, Clone)]
pub struct LogOperation {
    operation: MockOperation,
    logs: VecDeque<LogMessage>,
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
        prop_assert_eq!(Some(entry), expected.pop_front());
    }
    prop_assert!(expected.is_empty(), "expected logs {:?}", expected);
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
        check_logs(self.logs, log.as_mut()).await?;
        let res = log.await;
        prop_assert_eq!(res, response);
        /*
        match self.operation {
            MockOperation::SetOptions(options, response) => {
                let response = response.map(MockResponse::from);
                let log = client.set_options(&options).map_ok(MockResponse::from);

                let response = response.map_err(|err| err.to_string());
                let log = log.map_err(|err| err.to_string());
                let mut log = pin!(log);
                check_logs(self.logs, log.as_mut()).await?;
                let res = log.await;
                prop_assert_eq!(res, response);
            }
            MockOperation::IsValidPath(path, response) => {
                let response = response.map(MockResponse::from);
                let log = client.is_valid_path(&path).map_ok(MockResponse::from);

                let response = response.map_err(|err| err.to_string());
                let log = log.map_err(|err| err.to_string());
                let mut log = pin!(log);
                check_logs(self.logs, log.as_mut()).await?;
                let res = log.await;
                prop_assert_eq!(res, response);
            }
            MockOperation::QueryValidPaths(request, response) => {
                let response = response.map(MockResponse::from);
                let log = client
                    .query_valid_paths(&request.paths, request.substitute)
                    .map_ok(MockResponse::from);

                let response = response.map_err(|err| err.to_string());
                let log = log.map_err(|err| err.to_string());
                let mut log = pin!(log);
                check_logs(self.logs, log.as_mut()).await?;
                let res = log.await;
                prop_assert_eq!(res, response);
            }
            MockOperation::QueryPathInfo(path, response) => {
                let response = response.map(MockResponse::from);
                let log = client.query_path_info(&path).map_ok(MockResponse::from);

                let response = response.map_err(|err| err.to_string());
                let log = log.map_err(|err| err.to_string());
                let mut log = pin!(log);
                check_logs(self.logs, log.as_mut()).await?;
                let res = log.await;
                prop_assert_eq!(res, response);
            }
            MockOperation::NarFromPath(path, response) => {
                let response = response.map(MockResponse::from);
                let (mut reader, writer) = simplex(DEFAULT_BUF_SIZE);
                let log = client
                    .nar_from_path(&path, writer)
                    .and_then(|_| async move {
                        let mut buf = Vec::new();
                        reader.read_to_end(&mut buf).await?;
                        Ok(MockResponse::from(Bytes::from(buf)))
                    });

                let response = response.map_err(|err| err.to_string());
                let log = log.map_err(|err| err.to_string());
                let mut log = pin!(log);
                check_logs(self.logs, log.as_mut()).await?;
                let res = log.await;
                prop_assert_eq!(res, response);
            }
            MockOperation::BuildPaths(request, response) => {
                let response = response.map(MockResponse::from);
                let log = client
                    .build_paths(&request.paths, request.mode)
                    .map_ok(MockResponse::from);

                let response = response.map_err(|err| err.to_string());
                let log = log.map_err(|err| err.to_string());
                let mut log = pin!(log);
                check_logs(self.logs, log.as_mut()).await?;
                let res = log.await;
                prop_assert_eq!(res, response);
            }
            MockOperation::BuildDerivation(request, response) => {
                let response = response.map(MockResponse::from);
                let log = client
                    .build_derivation(&request.drv_path, &request.drv, request.build_mode)
                    .map_ok(MockResponse::from);

                let response = response.map_err(|err| err.to_string());
                let log = log.map_err(|err| err.to_string());
                let mut log = pin!(log);
                check_logs(self.logs, log.as_mut()).await?;
                let res = log.await;
                prop_assert_eq!(res, response);
            }
            MockOperation::QueryMissing(request, response) => {
                let response = response.map(MockResponse::from);
                let log = client.query_missing(&request).map_ok(MockResponse::from);

                let response = response.map_err(|err| err.to_string());
                let log = log.map_err(|err| err.to_string());
                let mut log = pin!(log);
                check_logs(self.logs, log.as_mut()).await?;
                let res = log.await;
                prop_assert_eq!(res, response);
            }
            MockOperation::AddToStoreNar(request, nar, response) => {
                let response = response.map(MockResponse::from);
                let log = client
                    .add_to_store_nar(
                        &request.path_info,
                        Cursor::new(&nar[..]),
                        request.repair,
                        request.dont_check_sigs,
                    )
                    .map_ok(MockResponse::from);

                let response = response.map_err(|err| err.to_string());
                let log = log.map_err(|err| err.to_string());
                let mut log = pin!(log);
                check_logs(self.logs, log.as_mut()).await?;
                let res = log.await;
                prop_assert_eq!(res, response);
            }
        }
        */
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

pub struct LogBuilder<'b, R> {
    owner: &'b mut Builder<R>,
    operation: MockOperation,
    logs: VecDeque<LogMessage>,
}

impl<R> LogBuilder<'_, R> {
    pub fn message<M: Into<DaemonString>>(mut self, msg: M) -> Self {
        let msg = msg.into();
        self.logs.push_back(LogMessage::Next(msg));
        self
    }

    pub fn start_activity(mut self, act: Activity) -> Self {
        self.logs.push_back(LogMessage::StartActivity(act));
        self
    }

    pub fn stop_activity(mut self, act: u64) -> Self {
        self.logs.push_back(LogMessage::StopActivity(act));
        self
    }

    pub fn result(mut self, result: ActivityResult) -> Self {
        self.logs.push_back(LogMessage::Result(result));
        self
    }
}

impl<'b, R: Clone> LogBuilder<'b, R> {
    pub fn build(self) -> &'b mut Builder<R> {
        self.owner.add_operation(LogOperation {
            operation: self.operation,
            logs: self.logs,
        });
        self.owner
    }
}

pub struct Builder<R> {
    trusted_client: TrustLevel,
    handshake_logs: VecDeque<LogMessage>,
    ops: VecDeque<LogOperation>,
    reporter: R,
}

impl<R> Builder<R> {
    pub fn set_options(
        &mut self,
        options: &super::ClientOptions,
        response: DaemonResult<()>,
    ) -> LogBuilder<R> {
        self.build_operation(MockOperation::SetOptions(options.clone(), response))
    }

    pub fn is_valid_path(
        &mut self,
        path: &StorePath,
        response: DaemonResult<bool>,
    ) -> LogBuilder<R> {
        self.build_operation(MockOperation::IsValidPath(path.clone(), response))
    }

    pub fn query_valid_paths(
        &mut self,
        paths: &StorePathSet,
        substitute: bool,
        response: DaemonResult<StorePathSet>,
    ) -> LogBuilder<R> {
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
    ) -> LogBuilder<R> {
        self.build_operation(MockOperation::QueryPathInfo(path.clone(), response))
    }

    pub fn nar_from_path(
        &mut self,
        path: &StorePath,
        response: DaemonResult<Bytes>,
    ) -> LogBuilder<R> {
        self.build_operation(MockOperation::NarFromPath(path.clone(), response))
    }

    pub fn build_paths(
        &mut self,
        paths: &[DerivedPath],
        mode: BuildMode,
        response: DaemonResult<()>,
    ) -> LogBuilder<R> {
        self.build_operation(MockOperation::BuildPaths(
            BuildPathsRequest {
                paths: paths.to_vec(),
                mode,
            },
            response,
        ))
    }

    pub fn build_derivation(
        &mut self,
        drv_path: &StorePath,
        drv: &BasicDerivation,
        build_mode: BuildMode,
        response: DaemonResult<BuildResult>,
    ) -> LogBuilder<R> {
        self.build_operation(MockOperation::BuildDerivation(
            BuildDerivationRequest {
                drv_path: drv_path.clone(),
                drv: drv.clone(),
                build_mode,
            },
            response,
        ))
    }

    pub fn query_missing(
        &mut self,
        paths: &[DerivedPath],
        response: DaemonResult<QueryMissingResult>,
    ) -> LogBuilder<R> {
        self.build_operation(MockOperation::QueryMissing(paths.to_vec(), response))
    }

    pub fn add_to_store_nar(
        &mut self,
        info: &ValidPathInfo,
        repair: bool,
        dont_check_sigs: bool,
        contents: Bytes,
        response: DaemonResult<()>,
    ) -> LogBuilder<R> {
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
    ) -> LogBuilder<R> {
        self.build_operation(MockOperation::AddMultipleToStore(
            AddMultipleToStoreRequest {
                repair,
                dont_check_sigs,
            },
            contents,
            response,
        ))
    }

    fn build_operation(&mut self, operation: MockOperation) -> LogBuilder<R> {
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
            self.reporter.unread_operation(op);
        }
    }
}

/*
impl<R> MockStore<R> {
    fn assert_set_options(
        trusted_client: TrustLevel,
        options: &super::ClientOptions,
        response: DaemonResult<()>
    ) -> MockStore {
        let store_dir = Default::default();
        let expected = MockOperation::SetOptions(options.clone(), response);
        MockStore {
            trusted_client: TrustLevel::Unknown,
            store_dir,
            expected,
            response,
            actual: None,
        }
    }

    pub fn assert_is_valid_path(path: &StorePath, response: Result<bool, DaemonError>) -> MockStore {
        let store_dir = Default::default();
        let expected = Request::IsValidPath(path.clone());
        let response = response.map(|e| e.into());
        MockStore {
            trusted_client: TrustLevel::Unknown,
            store_dir,
            expected,
            response,
            actual: None,
        }
    }

    pub fn assert_query_valid_paths(
        trusted_client: TrustLevel,
        paths: &StorePathSet,
        substitute: bool,
        response: DaemonResult<StorePathSet>,
    ) -> MockStore {
        let store_dir = Default::default();
        let expected = Request::QueryValidPaths(QueryValidPathsRequest {
            paths: paths.clone(),
            substitute,
        });
        let response = response.map(|e| e.into());
        MockStore {
            trusted_client,
            store_dir,
            expected,
            response,
            actual: None,
        }
    }

    pub fn assert_query_path_info(
        trusted_client: TrustLevel,
        path: &StorePath,
        response: DaemonResult<Option<UnkeyedValidPathInfo>>,
    ) -> MockStore {
        let store_dir = Default::default();
        let expected = Request::QueryPathInfo(path.clone());
        let response = response.map(|e| e.into());
        MockStore {
            trusted_client,
            store_dir,
            expected,
            response,
            actual: None,
        }
    }

    pub fn assert_nar_from_path(
        trusted_client: TrustLevel,
        path: &StorePath,
        response: DaemonResult<Bytes>,
    ) -> MockStore {
        let store_dir = Default::default();
        let expected = Request::NarFromPath(path.clone());
        let response = response.map(|e| e.into());
        MockStore {
            trusted_client,
            store_dir,
            expected,
            response,
            actual: None,
        }
    }

    pub fn assert_build_derivation(
        trusted_client: TrustLevel,
        drv_path: &StorePath,
        drv: &BasicDerivation,
        build_mode: BuildMode,
        response: Result<BuildResult, DaemonError>,
    ) -> MockStore {
        let store_dir = Default::default();
        let expected = Request::BuildDerivation(BuildDerivationRequest {
            drv_path: drv_path.clone(),
            drv: drv.clone(),
            build_mode,
        });
        let response = response.map(|e| e.into());
        MockStore {
            trusted_client,
            store_dir,
            expected,
            response,
            actual: None,
        }
    }
    pub fn assert_build_paths(
        trusted_client: TrustLevel,
        paths: &[DerivedPath],
        mode: BuildMode,
        response: Result<(), DaemonError>,
    ) -> MockStore {
        let store_dir = Default::default();
        let expected = Request::BuildPaths(BuildPathsRequest {
            paths: paths.into(),
            mode,
        });
        let response = response.map(|e| e.into());
        MockStore {
            trusted_client,
            store_dir,
            expected,
            response,
            actual: None,
        }
    }
    pub fn assert_add_to_store(
        trusted_client: TrustLevel,
        info: &ValidPathInfo,
        repair: bool,
        dont_check_sigs: bool,
        source: Bytes,
        response: Result<(), DaemonError>,
    ) -> MockStore {
        let store_dir = Default::default();
        let expected = Request::AddToStoreNar(AddToStoreNarRequest {
            info: info.clone(),
            repair,
            dont_check_sigs,
            source,
        });
        let response = response.map(|e| e.into());
        MockStore {
            trusted_client,
            store_dir,
            expected,
            response,
            actual: None,
        }
    }
    pub fn assert_query_closure(
        paths: &StorePathSet,
        include_outputs: bool,
        response: Result<StorePathSet, DaemonError>,
    ) -> MockStore {
        let store_dir = Default::default();
        let expected = Request::QueryClosure {
            paths: paths.clone(),
            include_outputs,
        };
        let response = response.map(|e| e.into());
        MockStore {
            trusted_client: TrustLevel::Unknown,
            store_dir,
            expected,
            response,
            actual: None,
        }
    }

    pub fn assert_add_multiple_to_store(
        trusted_client: TrustLevel,
        source: Bytes,
        repair: bool,
        check_sigs: bool,
        response: Result<(), DaemonError>,
    ) -> MockStore {
        let store_dir = Default::default();
        let expected = Request::AddMultipleToStore {
            source,
            repair,
            check_sigs,
        };
        let response = response.map(|e| e.into());
        MockStore {
            trusted_client,
            store_dir,
            expected,
            response,
            actual: None,
        }
    }
    pub fn assert_query_missing(
        targets: &[DerivedPath],
        response: Result<QueryMissingResult, DaemonError>,
    ) -> MockStore {
        let store_dir = Default::default();
        let expected = Request::QueryMissing(targets.into());
        let response = response.map(|e| e.into());
        MockStore {
            trusted_client: TrustLevel::Unknown,
            store_dir,
            expected,
            response,
            actual: None,
        }
    }

    pub fn prop_assert_eq(self) -> Result<(), TestCaseError> {
        ::proptest::prop_assert_eq!(self.expected, self.actual.unwrap());
        Ok(())
    }

    pub fn assert_eq(self) {
        ::pretty_assertions::assert_eq!(self.expected, self.actual.unwrap());
    }
}
 */

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
        let actual = MockRequest::BuildPaths(BuildPathsRequest {
            paths: drvs.to_vec(),
            mode,
        });
        self.check_operation(actual)
    }

    fn build_derivation<'a>(
        &'a mut self,
        drv_path: &'a StorePath,
        drv: &'a super::wire::types2::BasicDerivation,
        build_mode: BuildMode,
    ) -> impl ResultLog<Output = DaemonResult<BuildResult>> + 'a {
        let actual = MockRequest::BuildDerivation(BuildDerivationRequest {
            drv_path: drv_path.clone(),
            drv: drv.clone(),
            build_mode,
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
}
/*
impl DaemonStore for AssertStore {
    fn store_dir(&self) -> StoreDir {
        self.store_dir.clone()
    }

    fn is_trusted_client(&self) -> Option<TrustedFlag> {
        self.trusted_client
    }

    async fn set_options(&mut self) -> Result<(), Error> {
        Ok(())
    }

    async fn query_valid_paths(
        &mut self,
        paths: &StorePathSet,
        maybe_substitute: SubstituteFlag,
    ) -> Result<StorePathSet, Error> {
        let actual = Message::QueryValidPaths {
            paths: paths.clone(),
            maybe_substitute,
        };
        assert_eq!(None, self.actual.take(), "existing result");
        self.actual = Some(actual);
        match take(&mut self.response)? {
            MessageResponse::StorePathSet(set) => Ok(set),
            e => panic!("Invalid response {:?} for query_valid_paths", e),
        }
    }

    async fn query_path_info(&mut self, path: &StorePath) -> Result<Option<ValidPathInfo>, Error> {
        let actual = Message::QueryPathInfo(path.clone());
        assert_eq!(None, self.actual.take(), "existing result");
        self.actual = Some(actual);
        match take(&mut self.response)? {
            MessageResponse::ValidPathInfo(res) => Ok(res),
            e => panic!("Invalid response {:?} for query_path_info", e),
        }
    }

    async fn nar_from_path<W: AsyncWrite + Send + Unpin>(
        &mut self,
        path: &StorePath,
        mut sink: W,
    ) -> Result<(), Error> {
        let actual = Message::NarFromPath(path.clone());
        assert_eq!(None, self.actual.take(), "existing result");
        self.actual = Some(actual);
        match take(&mut self.response)? {
            MessageResponse::Bytes(set) => {
                sink.write_all(&set).await?;
                sink.flush().await?;
                Ok(())
            }
            e => panic!("Invalid response {:?} for nar_from_path", e),
        }
    }

    async fn add_to_store<R: AsyncRead + Send + Unpin>(
        &mut self,
        info: &ValidPathInfo,
        mut source: R,
        repair: RepairFlag,
        check_sigs: CheckSignaturesFlag,
    ) -> Result<(), Error> {
        let mut buf = Vec::new();
        source.read_to_end(&mut buf).await?;
        let actual = Message::AddToStore {
            info: info.clone(),
            source: buf.into(),
            repair,
            check_sigs,
        };
        assert_eq!(None, self.actual.take(), "existing result");
        self.actual = Some(actual);
        match take(&mut self.response)? {
            MessageResponse::Empty => Ok(()),
            e => panic!("Invalid response {:?} for add_to_store", e),
        }
    }
    async fn build_derivation(
        &mut self,
        drv_path: &StorePath,
        drv: &BasicDerivation,
        build_mode: BuildMode,
    ) -> Result<BuildResult, Error> {
        let settings = BuildSettings::default();
        let actual = Message::BuildDerivation {
            drv_path: drv_path.clone(),
            drv: drv.clone(),
            build_mode,
            settings,
        };
        assert_eq!(None, self.actual.take(), "existing result");
        self.actual = Some(actual);
        match take(&mut self.response)? {
            MessageResponse::BuildResult(res) => Ok(res),
            e => panic!("Invalid response {:?} for build_derivation", e),
        }
    }
    async fn build_paths(
        &mut self,
        drv_paths: &[DerivedPath],
        build_mode: BuildMode,
    ) -> Result<(), Error> {
        let actual = Message::BuildPaths {
            drv_paths: drv_paths.into(),
            build_mode,
            settings: BuildSettings::default(),
        };
        assert_eq!(None, self.actual.take(), "existing result");
        self.actual = Some(actual);
        match take(&mut self.response)? {
            MessageResponse::Empty => Ok(()),
            e => panic!("Invalid response {:?} for build_paths", e),
        }
    }

    async fn query_valid_paths_locked(
        &mut self,
        paths: &StorePathSet,
        lock: bool,
        maybe_substitute: SubstituteFlag,
    ) -> Result<StorePathSet, Error> {
        let actual = Message::LegacyQueryValidPaths {
            paths: paths.clone(),
            lock,
            maybe_substitute,
        };
        assert_eq!(None, self.actual.take(), "existing result");
        self.actual = Some(actual);
        match take(&mut self.response)? {
            MessageResponse::StorePathSet(set) => Ok(set),
            e => panic!("Invalid response {:?} for legacy_query_valid_paths", e),
        }
    }

    async fn is_valid_path(&mut self, path: &StorePath) -> Result<bool, Error> {
        let actual = Message::IsValidPath(path.clone());
        assert_eq!(None, self.actual.take(), "existing result");
        self.actual = Some(actual);
        match take(&mut self.response)? {
            MessageResponse::Bool(res) => Ok(res),
            e => panic!("Invalid response {:?} for is_valid_path", e),
        }
    }

    async fn add_multiple_to_store<R: AsyncRead + Send + Unpin>(
        &mut self,
        mut source: R,
        repair: RepairFlag,
        check_sigs: CheckSignaturesFlag,
    ) -> Result<(), Error> {
        let mut buf = Vec::new();
        source.read_to_end(&mut buf).await?;
        let actual = Message::AddMultipleToStore {
            source: buf.into(),
            repair,
            check_sigs,
        };
        assert_eq!(None, self.actual.take(), "existing result");
        self.actual = Some(actual);
        match take(&mut self.response)? {
            MessageResponse::Empty => Ok(()),
            e => panic!("Invalid response {:?} for add_multiple_to_store", e),
        }
    }

    async fn query_missing(
        &mut self,
        targets: &[DerivedPath],
    ) -> Result<QueryMissingResult, Error> {
        let actual = Message::QueryMissing(targets.into());
        assert_eq!(None, self.actual.take(), "existing result");
        self.actual = Some(actual);
        match take(&mut self.response)? {
            MessageResponse::QueryMissingResult(res) => Ok(res),
            e => panic!("Invalid response {:?} for query_missing", e),
        }
    }
}
 */
