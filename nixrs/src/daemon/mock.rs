use std::future::ready;
use std::io::Cursor;
use std::mem::take;
use std::{collections::VecDeque, future::Future};
use std::{fmt, thread};

use bytes::Bytes;
use futures::channel::mpsc;
#[cfg(any(test, feature = "test"))]
use proptest::prelude::TestCaseError;
use tokio::io::{AsyncWrite, AsyncWriteExt as _};

use super::logger::{Activity, ActivityResult, LogMessage, LoggerResult, LoggerResultExt as _};
use super::wire::types::Operation;
use super::wire::types2::{BuildResult, QueryMissingResult, QueryValidPathsRequest};
use super::{
    ClientOptions, DaemonError, DaemonErrorKind, DaemonResult, DaemonResultExt, DaemonStore,
    DaemonString, HandshakeDaemonStore, TrustLevel, UnkeyedValidPathInfo,
};
use crate::store_path::{StorePath, StorePathSet};

#[derive(Debug, Clone)]
pub enum MockOperation {
    SetOptions(ClientOptions, DaemonResult<()>),
    IsValidPath(StorePath, DaemonResult<bool>),
    QueryValidPaths(QueryValidPathsRequest, DaemonResult<StorePathSet>),
    QueryPathInfo(StorePath, DaemonResult<Option<UnkeyedValidPathInfo>>),
    NarFromPath(StorePath, DaemonResult<Bytes>),
}

impl MockOperation {
    pub fn request(&self) -> MockRequest {
        match self {
            Self::SetOptions(request, _) => MockRequest::SetOptions(request.clone()),
            Self::IsValidPath(request, _) => MockRequest::IsValidPath(request.clone()),
            Self::QueryValidPaths(request, _) => MockRequest::QueryValidPaths(request.clone()),
            Self::QueryPathInfo(request, _) => MockRequest::QueryPathInfo(request.clone()),
            Self::NarFromPath(request, _) => MockRequest::NarFromPath(request.clone()),
        }
    }

    pub fn operation(&self) -> Operation {
        match self {
            Self::SetOptions(_, _) => Operation::SetOptions,
            Self::IsValidPath(_, _) => Operation::IsValidPath,
            Self::QueryValidPaths(_, _) => Operation::QueryValidPaths,
            Self::QueryPathInfo(_, _) => Operation::QueryPathInfo,
            Self::NarFromPath(_, _) => Operation::NarFromPath,
        }
    }
}

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub enum MockRequest {
    SetOptions(ClientOptions),
    IsValidPath(StorePath),
    QueryValidPaths(QueryValidPathsRequest),
    QueryPathInfo(StorePath),
    NarFromPath(StorePath),
}

impl MockRequest {
    pub fn operation(&self) -> Operation {
        match self {
            Self::SetOptions(_) => Operation::SetOptions,
            Self::IsValidPath(_) => Operation::IsValidPath,
            Self::QueryValidPaths(_) => Operation::QueryValidPaths,
            Self::QueryPathInfo(_) => Operation::QueryPathInfo,
            Self::NarFromPath(_) => Operation::NarFromPath,
        }
    }
}

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub enum MockResponse {
    Empty,
    Bool(bool),
    StorePathSet(StorePathSet),
    BuildResult(BuildResult),
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
impl From<bool> for MockResponse {
    fn from(val: bool) -> Self {
        MockResponse::Bool(val)
    }
}

impl From<StorePathSet> for MockResponse {
    fn from(v: StorePathSet) -> Self {
        MockResponse::StorePathSet(v)
    }
}
impl From<BuildResult> for MockResponse {
    fn from(v: BuildResult) -> Self {
        MockResponse::BuildResult(v)
    }
}
impl From<Bytes> for MockResponse {
    fn from(v: Bytes) -> Self {
        MockResponse::Bytes(v)
    }
}
impl From<Option<UnkeyedValidPathInfo>> for MockResponse {
    fn from(v: Option<UnkeyedValidPathInfo>) -> Self {
        MockResponse::ValidPathInfo(v)
    }
}
impl From<QueryMissingResult> for MockResponse {
    fn from(v: QueryMissingResult) -> Self {
        MockResponse::QueryMissingResult(v)
    }
}

pub trait MockReporter {
    fn unexpected_operation(
        &mut self,
        expected: MockOperation,
        actual: MockRequest,
    ) -> impl LoggerResult<MockResponse, DaemonError>;
    fn invalid_operation(
        &mut self,
        expected: MockOperation,
        actual: MockRequest,
    ) -> impl LoggerResult<MockResponse, DaemonError>;
    fn extra_operation(
        &mut self,
        actual: MockRequest,
    ) -> impl LoggerResult<MockResponse, DaemonError>;
    fn unread_operation(&mut self, operation: LogOperation);
}

impl MockReporter for () {
    fn unexpected_operation(
        &mut self,
        expected: MockOperation,
        actual: MockRequest,
    ) -> impl LoggerResult<MockResponse, DaemonError> {
        Err(DaemonErrorKind::Custom(format!(
            "Unexpected operation {} expected {}",
            actual.operation(),
            expected.operation()
        )))
        .with_operation(actual.operation())
    }

    fn invalid_operation(
        &mut self,
        expected: MockOperation,
        actual: MockRequest,
    ) -> impl LoggerResult<MockResponse, DaemonError> {
        Err(DaemonErrorKind::Custom(format!(
            "Invalid operation {:?} expected {:?}",
            actual,
            expected.request()
        )))
        .with_operation(actual.operation())
    }

    fn extra_operation(
        &mut self,
        actual: MockRequest,
    ) -> impl LoggerResult<MockResponse, DaemonError> {
        Err(DaemonErrorKind::Custom(format!(
            "Extra operation {:?}",
            actual
        )))
        .with_operation(actual.operation())
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
    ) -> impl LoggerResult<MockResponse, DaemonError> {
        let op = actual.operation();
        let report = ReporterError::Unexpected(expected, actual);
        let ret = Err(DaemonErrorKind::Custom(report.to_string())).with_operation(op);
        self.0.unbounded_send(report).unwrap();
        ret
    }

    fn invalid_operation(
        &mut self,
        expected: MockOperation,
        actual: MockRequest,
    ) -> impl LoggerResult<MockResponse, DaemonError> {
        let op = actual.operation();
        let report = ReporterError::Invalid(expected, actual);
        let ret = Err(DaemonErrorKind::Custom(report.to_string())).with_operation(op);
        self.0.unbounded_send(report).unwrap();
        ret
    }

    fn extra_operation(
        &mut self,
        actual: MockRequest,
    ) -> impl LoggerResult<MockResponse, DaemonError> {
        let op = actual.operation();
        let report = ReporterError::Extra(actual);
        let ret = Err(DaemonErrorKind::Custom(report.to_string())).with_operation(op);
        self.0.unbounded_send(report).unwrap();
        ret
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

impl LogOperation {
    #[cfg(any(test, feature = "test"))]
    pub async fn check_operation<S: DaemonStore>(
        mut self,
        mut client: S,
    ) -> Result<(), TestCaseError> {
        use proptest::{prop_assert, prop_assert_eq};

        match self.operation {
            MockOperation::SetOptions(options, response) => {
                let mut log = client.set_options(&options);
                while let Some(entry) = log.next().await {
                    prop_assert_eq!(Some(entry.unwrap()), self.logs.pop_front());
                }
                prop_assert!(self.logs.is_empty(), "expected logs {:?}", self.logs);
                let res = log.result().await;
                prop_assert_eq!(
                    res.map_err(|e| e.to_string()),
                    response.map_err(|e| e.to_string())
                );
            }
            MockOperation::IsValidPath(path, response) => {
                let mut log = client.is_valid_path(&path);
                while let Some(entry) = log.next().await {
                    prop_assert_eq!(Some(entry.unwrap()), self.logs.pop_front());
                }
                prop_assert!(self.logs.is_empty(), "expected logs {:?}", self.logs);
                let res = log.result().await;
                prop_assert_eq!(
                    res.map_err(|e| e.to_string()),
                    response.map_err(|e| e.to_string())
                );
            }
            MockOperation::QueryValidPaths(request, response) => {
                let mut log = client.query_valid_paths(&request.paths, request.substitute);
                while let Some(entry) = log.next().await {
                    prop_assert_eq!(Some(entry.unwrap()), self.logs.pop_front());
                }
                prop_assert!(self.logs.is_empty(), "expected logs {:?}", self.logs);
                let res = log.result().await;
                prop_assert_eq!(
                    res.map_err(|e| e.to_string()),
                    response.map_err(|e| e.to_string())
                );
            }
            MockOperation::QueryPathInfo(path, response) => {
                let mut log = client.query_path_info(&path);
                while let Some(entry) = log.next().await {
                    prop_assert_eq!(Some(entry.unwrap()), self.logs.pop_front());
                }
                prop_assert!(self.logs.is_empty(), "expected logs {:?}", self.logs);
                let res = log.result().await;
                prop_assert_eq!(
                    res.map_err(|e| e.to_string()),
                    response.map_err(|e| e.to_string())
                );
            }
            MockOperation::NarFromPath(path, response) => {
                let mut out = Vec::new();
                let mut log = client.nar_from_path(&path, Cursor::new(&mut out));
                while let Some(entry) = log.next().await {
                    prop_assert_eq!(Some(entry.unwrap()), self.logs.pop_front());
                }
                prop_assert!(self.logs.is_empty(), "expected logs {:?}", self.logs);
                log.result().await?;
                let bytes = response?;
                prop_assert_eq!(out, bytes);
            }
        }
        Ok(())
    }
}

pub struct LogResult<Fut> {
    logs: VecDeque<LogMessage>,
    result: Fut,
}

impl<Fut, T, E> LoggerResult<T, E> for LogResult<Fut>
where
    Fut: Future<Output = Result<T, E>> + Send,
    T: 'static,
    E: 'static,
{
    async fn next(&mut self) -> Option<Result<LogMessage, E>> {
        self.logs.pop_front().map(Result::Ok)
    }

    async fn result(self) -> Result<T, E> {
        self.result.await
    }
}

enum FourWay<R1, R2, R3, R4> {
    R1(R1),
    R2(R2),
    R3(R3),
    R4(R4),
}

impl<R1, R2, R3, R4, T, E> LoggerResult<T, E> for FourWay<R1, R2, R3, R4>
where
    R1: LoggerResult<T, E>,
    R2: LoggerResult<T, E>,
    R3: LoggerResult<T, E>,
    R4: LoggerResult<T, E>,
    T: 'static,
    E: 'static,
{
    async fn next(&mut self) -> Option<Result<LogMessage, E>> {
        match self {
            Self::R1(r1) => r1.next().await,
            Self::R2(r2) => r2.next().await,
            Self::R3(r3) => r3.next().await,
            Self::R4(r4) => r4.next().await,
        }
    }

    async fn result(self) -> Result<T, E> {
        match self {
            Self::R1(r1) => r1.result().await,
            Self::R2(r2) => r2.result().await,
            Self::R3(r3) => r3.result().await,
            Self::R4(r4) => r4.result().await,
        }
    }
}

pub struct LogBuilder<'b, R> {
    owner: &'b mut Builder<R>,
    operation: MockOperation,
    logs: VecDeque<LogMessage>,
}

impl<'b, R> LogBuilder<'b, R> {
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
        LogBuilder {
            owner: self,
            operation: MockOperation::SetOptions(options.clone(), response),
            logs: VecDeque::new(),
        }
    }

    pub fn is_valid_path(
        &mut self,
        path: &StorePath,
        response: DaemonResult<bool>,
    ) -> LogBuilder<R> {
        LogBuilder {
            owner: self,
            operation: MockOperation::IsValidPath(path.clone(), response),
            logs: VecDeque::new(),
        }
    }

    pub fn query_valid_paths(
        &mut self,
        paths: &StorePathSet,
        substitute: bool,
        response: DaemonResult<StorePathSet>,
    ) -> LogBuilder<R> {
        let paths = paths.clone();
        LogBuilder {
            owner: self,
            operation: MockOperation::QueryValidPaths(
                QueryValidPathsRequest { paths, substitute },
                response,
            ),
            logs: VecDeque::new(),
        }
    }

    pub fn query_path_info(
        &mut self,
        path: &StorePath,
        response: DaemonResult<Option<UnkeyedValidPathInfo>>,
    ) -> LogBuilder<R> {
        let path = path.clone();
        LogBuilder {
            owner: self,
            operation: MockOperation::QueryPathInfo(path.clone(), response),
            logs: VecDeque::new(),
        }
    }

    pub fn nar_from_path(
        &mut self,
        path: &StorePath,
        response: DaemonResult<Bytes>,
    ) -> LogBuilder<R> {
        LogBuilder {
            owner: self,
            operation: MockOperation::NarFromPath(path.clone(), response),
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

    fn handshake(mut self) -> impl LoggerResult<Self::Store, DaemonError> {
        let logs = take(&mut self.handshake_logs);
        logs.map_ok(|_| self)
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
    ) -> impl super::logger::LoggerResult<(), DaemonError> + 'a {
        let actual = MockRequest::SetOptions(options.clone());
        match self.ops.pop_front() {
            None => FourWay::R1(
                self.reporter
                    .extra_operation(actual)
                    .map_ok(|resp| resp.unwrap_empty()),
            ),
            Some(LogOperation {
                operation: MockOperation::SetOptions(req, result),
                logs,
            }) => {
                if req != *options {
                    FourWay::R2(
                        self.reporter
                            .invalid_operation(MockOperation::SetOptions(req, result), actual)
                            .map_ok(|resp| resp.unwrap_empty()),
                    )
                } else {
                    FourWay::R3(LogResult {
                        logs,
                        result: ready(result),
                    })
                }
            }
            Some(expected) => FourWay::R4(
                self.reporter
                    .unexpected_operation(expected.operation, actual)
                    .map_ok(|resp| resp.unwrap_empty()),
            ),
        }
    }

    fn is_valid_path<'a>(
        &'a mut self,
        path: &'a StorePath,
    ) -> impl super::logger::LoggerResult<bool, DaemonError> + 'a {
        let actual = MockRequest::IsValidPath(path.clone());
        match self.ops.pop_front() {
            None => FourWay::R1(
                self.reporter
                    .extra_operation(actual)
                    .map_ok(|resp| resp.unwrap_bool()),
            ),
            Some(LogOperation {
                operation: MockOperation::IsValidPath(req, result),
                logs,
            }) => {
                if req != *path {
                    FourWay::R2(
                        self.reporter
                            .invalid_operation(MockOperation::IsValidPath(req, result), actual)
                            .map_ok(|resp| resp.unwrap_bool()),
                    )
                } else {
                    FourWay::R3(LogResult {
                        logs,
                        result: ready(result),
                    })
                }
            }
            Some(expected) => FourWay::R4(
                self.reporter
                    .unexpected_operation(expected.operation, actual)
                    .map_ok(|resp| resp.unwrap_bool()),
            ),
        }
    }

    fn query_valid_paths<'a>(
        &'a mut self,
        paths: &'a StorePathSet,
        substitute: bool,
    ) -> impl super::logger::LoggerResult<StorePathSet, DaemonError> + 'a {
        let actual = MockRequest::QueryValidPaths(QueryValidPathsRequest {
            paths: paths.clone(),
            substitute,
        });
        match self.ops.pop_front() {
            None => FourWay::R1(
                self.reporter
                    .extra_operation(actual)
                    .map_ok(|resp| resp.unwrap_store_path_set()),
            ),
            Some(LogOperation {
                operation: MockOperation::QueryValidPaths(req, result),
                logs,
            }) => {
                if req.paths != *paths || req.substitute != substitute {
                    FourWay::R2(
                        self.reporter
                            .invalid_operation(MockOperation::QueryValidPaths(req, result), actual)
                            .map_ok(|resp| resp.unwrap_store_path_set()),
                    )
                } else {
                    FourWay::R3(LogResult {
                        logs,
                        result: ready(result),
                    })
                }
            }
            Some(expected) => FourWay::R4(
                self.reporter
                    .unexpected_operation(expected.operation, actual)
                    .map_ok(|resp| resp.unwrap_store_path_set()),
            ),
        }
    }

    fn query_path_info<'a>(
        &'a mut self,
        path: &'a StorePath,
    ) -> impl super::logger::LoggerResult<Option<UnkeyedValidPathInfo>, DaemonError> + 'a {
        let actual = MockRequest::QueryPathInfo(path.clone());
        match self.ops.pop_front() {
            None => FourWay::R1(
                self.reporter
                    .extra_operation(actual)
                    .map_ok(|resp| resp.unwrap_valid_path_info()),
            ),
            Some(LogOperation {
                operation: MockOperation::QueryPathInfo(req, result),
                logs,
            }) => {
                if req != *path {
                    FourWay::R2(
                        self.reporter
                            .invalid_operation(MockOperation::QueryPathInfo(req, result), actual)
                            .map_ok(|resp| resp.unwrap_valid_path_info()),
                    )
                } else {
                    FourWay::R3(LogResult {
                        logs,
                        result: ready(result),
                    })
                }
            }
            Some(expected) => FourWay::R4(
                self.reporter
                    .unexpected_operation(expected.operation, actual)
                    .map_ok(|resp| resp.unwrap_valid_path_info()),
            ),
        }
    }

    fn nar_from_path<'a, 'p, 'r, W>(
        &'a mut self,
        path: &'p StorePath,
        mut sink: W,
    ) -> impl super::logger::LoggerResult<(), DaemonError> + 'r
    where
        W: AsyncWrite + Unpin + Send + 'r,
        'a: 'r,
        'p: 'r,
    {
        let actual = MockRequest::NarFromPath(path.clone());
        let result = match self.ops.pop_front() {
            None => FourWay::R1(
                self.reporter
                    .extra_operation(actual)
                    .map_ok(|resp| resp.unwrap_bytes()),
            ),
            Some(LogOperation {
                operation: MockOperation::NarFromPath(req, result),
                logs,
            }) => {
                if req != *path {
                    FourWay::R2(
                        self.reporter
                            .invalid_operation(MockOperation::NarFromPath(req, result), actual)
                            .map_ok(|resp| resp.unwrap_bytes()),
                    )
                } else {
                    FourWay::R3(LogResult {
                        logs,
                        result: ready(result),
                    })
                }
            }
            Some(expected) => FourWay::R4(
                self.reporter
                    .unexpected_operation(expected.operation, actual)
                    .map_ok(|resp| resp.unwrap_bytes()),
            ),
        };
        result.and_then(move |bytes| async move {
            eprintln!("Writing NAR");
            sink.write_all(&bytes).await?;
            sink.shutdown().await?;
            eprintln!("Writen NAR");
            Ok(())
        })
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
