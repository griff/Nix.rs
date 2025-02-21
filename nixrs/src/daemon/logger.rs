use std::fmt;
use std::future::Future;
use std::io::Cursor;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{ready, Context, Poll};

use async_stream::stream;
use futures::Stream;
#[cfg(feature = "nixrs-derive")]
use nixrs_derive::{NixDeserialize, NixSerialize};
use num_enum::{FromPrimitive, IntoPrimitive, TryFromPrimitive};
use pin_project_lite::pin_project;
#[cfg(any(test, feature = "test"))]
use proptest_derive::Arbitrary;
use tokio::io::{AsyncBufRead, AsyncBufReadExt as _, AsyncRead, AsyncWrite, AsyncWriteExt as _};
use tokio::sync::oneshot;

use super::de::{NixDeserialize, NixRead};
use super::ser::{NixWrite, NixWriter};
use super::wire::logger::{IgnoredErrorType, RawLogMessage, RawLogMessageType};
use super::wire::IgnoredZero;
use super::{DaemonError, DaemonErrorKind, DaemonInt, DaemonResult, DaemonString, RemoteError};
#[cfg(feature = "nixrs-derive")]
use crate::daemon::ser::NixSerialize;

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, FromPrimitive, IntoPrimitive, Default,
)]
#[cfg_attr(any(test, feature = "test"), derive(Arbitrary))]
#[cfg_attr(feature = "nixrs-derive", derive(NixDeserialize, NixSerialize))]
#[cfg_attr(feature = "nixrs-derive", nix(from = "u16", into = "u16"))]
#[repr(u16)]
pub enum Verbosity {
    #[default]
    Error = 0,
    Warn = 1,
    Notice = 2,
    Info = 3,
    Talkative = 4,
    Chatty = 5,
    Debug = 6,
    #[catch_all]
    Vomit = 7,
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, TryFromPrimitive, IntoPrimitive,
)]
#[cfg_attr(feature = "nixrs-derive", derive(NixDeserialize, NixSerialize))]
#[cfg_attr(feature = "nixrs-derive", nix(try_from = "u16", into = "u16"))]
#[repr(u16)]
pub enum ActivityType {
    Unknown = 0,
    CopyPath = 100,
    FileTransfer = 101,
    Realise = 102,
    CopyPaths = 103,
    Builds = 104,
    Build = 105,
    OptimiseStore = 106,
    VerifyPaths = 107,
    Substitute = 108,
    QueryPathInfo = 109,
    PostBuildHook = 110,
    BuildWaiting = 111,
    FetchTree = 112,
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, TryFromPrimitive, IntoPrimitive,
)]
#[cfg_attr(feature = "nixrs-derive", derive(NixDeserialize, NixSerialize))]
#[cfg_attr(feature = "nixrs-derive", nix(try_from = "u16", into = "u16"))]
#[repr(u16)]
pub enum ResultType {
    FileLinked = 100,
    BuildLogLine = 101,
    UntrustedPath = 102,
    CorruptedPath = 103,
    SetPhase = 104,
    Progress = 105,
    SetExpected = 106,
    PostBuildLogLine = 107,
    FetchStatus = 108,
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, TryFromPrimitive, IntoPrimitive,
)]
#[cfg_attr(feature = "nixrs-derive", derive(NixDeserialize, NixSerialize))]
#[cfg_attr(feature = "nixrs-derive", nix(try_from = "u16", into = "u16"))]
#[repr(u16)]
pub enum FieldType {
    Int = 0,
    String = 1,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, NixDeserialize, NixSerialize)]
pub struct TraceLine {
    _have_pos: IgnoredZero,
    pub hint: DaemonString, // If logger is JSON, invalid UTF-8 is replaced with U+FFFD
}

fn default_exit_status() -> DaemonInt {
    1
}

#[derive(Debug)]
#[cfg_attr(feature = "nixrs-derive", derive(NixDeserialize, NixSerialize))]
pub struct LogError {
    #[cfg_attr(feature = "nixrs-derive", nix(version = "26.."))]
    _ty: IgnoredErrorType,
    #[cfg_attr(feature = "nixrs-derive", nix(version = "26.."))]
    pub level: Verbosity,
    #[cfg_attr(feature = "nixrs-derive", nix(version = "26.."))]
    _name: IgnoredErrorType,
    pub msg: DaemonString, // If logger is JSON, invalid UTF-8 is replaced with U+FFFD
    #[cfg_attr(
        feature = "nixrs-derive",
        nix(version = "..=25", default = "default_exit_status")
    )]
    pub exit_status: DaemonInt,
    #[cfg_attr(feature = "nixrs-derive", nix(version = "26.."))]
    _have_pos: IgnoredZero,
    #[cfg_attr(feature = "nixrs-derive", nix(version = "26.."))]
    pub traces: Vec<TraceLine>,
}

impl From<RemoteError> for LogError {
    fn from(value: RemoteError) -> Self {
        LogError {
            level: value.level,
            msg: value.msg,
            exit_status: value.exit_status,
            traces: value.traces,
            _ty: IgnoredErrorType,
            _name: IgnoredErrorType,
            _have_pos: IgnoredZero,
        }
    }
}

impl From<DaemonError> for LogError {
    fn from(value: DaemonError) -> Self {
        match value.kind().clone() {
            DaemonErrorKind::Remote(remote_error) => remote_error.into(),
            _ => {
                let msg = value.to_string().into_bytes().into();
                LogError {
                    msg,
                    level: Verbosity::Error,
                    exit_status: 1,
                    traces: Vec::new(),
                    _ty: IgnoredErrorType,
                    _name: IgnoredErrorType,
                    _have_pos: IgnoredZero,
                }
            }
        }
    }
}

impl From<LogError> for RemoteError {
    fn from(err: LogError) -> Self {
        RemoteError {
            level: err.level,
            msg: err.msg,
            traces: err.traces,
            exit_status: err.exit_status,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum LogMessage {
    Next(DaemonString),
    StartActivity(Activity),
    StopActivity(u64),
    Result(ActivityResult),
}

impl NixSerialize for LogMessage {
    async fn serialize<W>(&self, writer: &mut W) -> Result<(), W::Error>
    where
        W: super::ser::NixWrite,
    {
        match self {
            LogMessage::Next(msg) => {
                writer.write_value(&RawLogMessageType::Next).await?;
                writer.write_value(msg).await?;
            }
            LogMessage::StartActivity(act) => {
                if writer.version().minor() >= 20 {
                    writer
                        .write_value(&RawLogMessageType::StartActivity)
                        .await?;
                    writer.write_value(act).await?;
                } else {
                    writer.write_value(&RawLogMessageType::Next).await?;
                    writer.write_value(&act.text).await?;
                }
            }
            LogMessage::StopActivity(act) => {
                if writer.version().minor() >= 20 {
                    writer.write_value(&RawLogMessageType::StopActivity).await?;
                    writer.write_value(act).await?;
                }
            }
            LogMessage::Result(result) => {
                if writer.version().minor() >= 20 {
                    writer.write_value(&RawLogMessageType::Result).await?;
                    writer.write_value(result).await?;
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "nixrs-derive", derive(NixDeserialize, NixSerialize))]
pub struct Activity {
    pub act: u64,
    pub level: Verbosity,
    pub activity_type: ActivityType,
    pub text: DaemonString, // If logger is JSON, invalid UTF-8 is replaced with U+FFFD
    pub fields: Vec<Field>,
    pub parent: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "nixrs-derive", derive(NixDeserialize, NixSerialize))]
pub struct ActivityResult {
    pub act: u64,
    pub result_type: ResultType,
    pub fields: Vec<Field>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "nixrs-derive", derive(NixDeserialize, NixSerialize))]
#[cfg_attr(feature = "nixrs-derive", nix(tag = "FieldType"))]
pub enum Field {
    Int(u64),
    String(DaemonString),
}

pub trait ResultLog<T, E>: Stream<Item = LogMessage> + Future<Output = Result<T, E>> {}
impl<R, T, E> ResultLog<T, E> for R where
    R: Stream<Item = LogMessage> + Future<Output = Result<T, E>>
{
}

pub trait LocalLoggerResult<T, E> {
    fn next(&mut self) -> impl Future<Output = Option<Result<LogMessage, E>>> + '_;
    fn result(self) -> impl Future<Output = Result<T, E>>;
}

pin_project! {
    struct MapOkResult<L, F, T> {
        #[pin]
        result: L,
        mapper: Option<F>,
        value: PhantomData<T>,
    }
}
impl<L, F, T> Stream for MapOkResult<L, F, T>
where
    L: Stream<Item = LogMessage>,
{
    type Item = LogMessage;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.project().result.poll_next(cx)
    }
}
impl<L, F, T, T2, E> Future for MapOkResult<L, F, T>
where
    L: Future<Output = Result<T, E>>,
    F: FnOnce(T) -> T2 + Send,
    T: Send,
{
    type Output = Result<T2, E>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let me = self.project();
        Poll::Ready(ready!(me.result.poll(cx)).map(|value| (me.mapper.take().unwrap())(value)))
    }
}

pin_project! {
    struct MapErrResult<L, F, E> {
        #[pin]
        result: L,
        mapper: Option<F>,
        value: PhantomData<E>,
    }
}
impl<L, F, E> Stream for MapErrResult<L, F, E>
where
    L: Stream<Item = LogMessage>,
{
    type Item = LogMessage;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.project().result.poll_next(cx)
    }
}
impl<L, F, T, E, E2> Future for MapErrResult<L, F, E>
where
    L: Future<Output = Result<T, E>>,
    F: FnOnce(E) -> E2 + Send,
    E: Send,
{
    type Output = Result<T, E2>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let me = self.project();
        Poll::Ready(ready!(me.result.poll(cx)).map_err(|err| (me.mapper.take().unwrap())(err)))
    }
}

pin_project! {
    #[project = AndThenLogResultProj]
    enum AndThenLogResult<F, R> {
        First {
            mapper: Option<F>,
        },
        Second {
            #[pin]
            result: R
        },
    }
}

pin_project! {
    struct AndThenLog<L, F, R> {
        #[pin]
        stream: L,
        #[pin]
        result: AndThenLogResult<F, R>
    }
}

impl<L, F, R> Stream for AndThenLog<L, F, R>
where
    L: Stream<Item = LogMessage>,
{
    type Item = LogMessage;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.project().stream.poll_next(cx)
    }
}

impl<L, F, R, T, T2, E> Future for AndThenLog<L, F, R>
where
    L: Future<Output = Result<T, E>> + Stream<Item = LogMessage>,
    F: FnOnce(T) -> R,
    R: Future<Output = Result<T2, E>>,
{
    type Output = Result<T2, E>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        loop {
            let mut me = self.as_mut().project();
            match me.result.as_mut().project() {
                AndThenLogResultProj::First { mapper } => {
                    let res = ready!(me.stream.poll(cx))?;
                    let mapper = mapper.take().unwrap();
                    let result = (mapper)(res);
                    me.result.set(AndThenLogResult::Second { result });
                }
                AndThenLogResultProj::Second { result } => {
                    return result.poll(cx);
                }
            }
        }
    }
}

pub trait ResultLogExt<T, E> {
    fn map_ok<F, T2>(self, f: F) -> impl ResultLog<T2, E>
    where
        F: FnOnce(T) -> T2 + Send,
        T2: 'static;
    fn map_err<F, E2>(self, f: F) -> impl ResultLog<T, E2>
    where
        F: FnOnce(E) -> E2 + Send,
        E2: 'static;
    fn and_then<F, T2, Fut>(self, f: F) -> impl ResultLog<T2, E>
    where
        F: FnOnce(T) -> Fut + Send,
        Fut: Future<Output = Result<T2, E>> + Send,
        T2: 'static;
}

impl<L, T, E> ResultLogExt<T, E> for L
where
    L: ResultLog<T, E>,
    T: Send + 'static,
    E: Send + 'static,
{
    fn map_ok<F, T2>(self, f: F) -> impl ResultLog<T2, E>
    where
        F: FnOnce(T) -> T2 + Send,
        T2: 'static,
    {
        MapOkResult {
            result: self,
            mapper: Some(f),
            value: PhantomData,
        }
    }

    fn map_err<F, E2>(self, f: F) -> impl ResultLog<T, E2>
    where
        F: FnOnce(E) -> E2 + Send,
        E2: 'static,
    {
        MapErrResult {
            result: self,
            mapper: Some(f),
            value: PhantomData,
        }
    }
    fn and_then<F, T2, Fut>(self, f: F) -> impl ResultLog<T2, E>
    where
        F: FnOnce(T) -> Fut + Send,
        Fut: Future<Output = Result<T2, E>> + Send,
        T2: 'static,
    {
        AndThenLog {
            stream: self,
            result: AndThenLogResult::First { mapper: Some(f) },
        }
    }
}

pin_project! {
    #[project = FutureResultProj]
    #[derive(Default)]
    pub enum FutureResult<Fut, T, E> {
        Later {
            #[pin]
            fut: Fut,
        },
        ResolvedOk {
            #[pin]
            result: T
        },
        ResolvedErr {
            err: Option<E>,
        },
        #[default]
        Invalid,
    }
}

impl<Fut, T, E> FutureResult<Fut, T, E>
where
    Fut: Future<Output = Result<T, E>>,
{
    pub fn new(fut: Fut) -> Self {
        Self::Later { fut }
    }

    fn poll_resolved(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Pin<&mut T>, ()>> {
        match self.as_mut().project() {
            FutureResultProj::Later { fut } => match ready!(fut.poll(cx)) {
                Ok(result) => {
                    self.set(FutureResult::ResolvedOk { result });
                }
                Err(err) => {
                    self.set(FutureResult::ResolvedErr { err: Some(err) });
                }
            },
            FutureResultProj::ResolvedOk { result: _ } => {}
            FutureResultProj::ResolvedErr { err: _ } => {}
            FutureResultProj::Invalid => {}
        }
        match self.project() {
            FutureResultProj::Later { fut: _ } => unreachable!(),
            FutureResultProj::ResolvedOk { result } => Poll::Ready(Ok(result)),
            FutureResultProj::ResolvedErr { err: _ } => Poll::Ready(Err(())),
            FutureResultProj::Invalid => unreachable!(),
        }
    }
}

impl<Fut, R, E> Stream for FutureResult<Fut, R, E>
where
    Fut: Future<Output = Result<R, E>> + Send,
    R: Stream<Item = LogMessage>,
    E: Send,
{
    type Item = LogMessage;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Ok(res) = ready!(self.poll_resolved(cx)) {
            res.poll_next(cx)
        } else {
            Poll::Ready(None)
        }
    }
}

impl<Fut, R, T, E> Future for FutureResult<Fut, R, E>
where
    Fut: Future<Output = Result<R, E>> + Send,
    R: Future<Output = Result<T, E>>,
    E: Send,
{
    type Output = Result<T, E>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if let Ok(res) = ready!(self.as_mut().poll_resolved(cx)) {
            res.poll(cx)
        } else {
            match self.project() {
                FutureResultProj::Later { fut: _ } => unreachable!(),
                FutureResultProj::ResolvedOk { result } => result.poll(cx),
                FutureResultProj::ResolvedErr { err } => {
                    Poll::Ready(Err(err.take().expect("Polling invalid FutureRessult")))
                }
                _ => panic!("Polling invalid FutureRessult"),
            }
        }
    }
}

pub trait ReadResult<R, W, SR, SW, T>: Sized {
    fn read_result(
        self,
        result: Result<(), DaemonError>,
        reader: R,
        writer: Option<W>,
        source: Option<SR>,
        sink: Option<SW>,
    ) -> impl Future<Output = DaemonResult<T>> + Send;
}

impl<R, W, SR, SW, T> ReadResult<R, W, SR, SW, T> for ()
where
    T: NixDeserialize,
    R: NixRead + AsyncRead + fmt::Debug + Unpin + Send,
    DaemonError: From<<R as NixRead>::Error>,
    W: Send,
    SR: Send,
    SW: Send,
{
    async fn read_result(
        self,
        result: Result<(), DaemonError>,
        mut reader: R,
        _writer: Option<W>,
        _source: Option<SR>,
        _sink: Option<SW>,
    ) -> DaemonResult<T> {
        match result {
            Err(err) => Err(err),
            Ok(_) => Ok(reader.read_value().await?),
        }
    }
}

pub struct ResultFn<F, FFut> {
    f: F,
    _res: PhantomData<fn(FFut)>,
}

impl<R, W, SR, SW, T, F, FFut> ReadResult<R, W, SR, SW, T> for ResultFn<F, FFut>
where
    F: FnOnce(Result<(), DaemonError>, R, Option<W>, Option<SR>, Option<SW>) -> FFut + Send,
    FFut: Future<Output = DaemonResult<T>> + Send,
    R: Send,
    W: Send,
    SR: Send,
    SW: Send,
{
    async fn read_result(
        self,
        result: Result<(), DaemonError>,
        reader: R,
        writer: Option<W>,
        source: Option<SR>,
        sink: Option<SW>,
    ) -> DaemonResult<T> {
        (self.f)(result, reader, writer, source, sink).await
    }
}

pub struct ProcessStderr<R, W, SR, SW, T, TFut> {
    result: Option<Result<(), DaemonError>>,
    reader: R,
    writer: Option<W>,
    source: Option<SR>,
    sink: Option<SW>,
    read_result: TFut,
    _result_type: PhantomData<T>,
}

impl<R, T> ProcessStderr<R, NixWriter<Cursor<Vec<u8>>>, Cursor<Vec<u8>>, Cursor<Vec<u8>>, T, ()> {
    pub fn new(reader: R) -> Self {
        ProcessStderr {
            reader,
            writer: None,
            source: None,
            sink: None,
            result: None,
            read_result: (),
            _result_type: PhantomData,
        }
    }
}

impl<R, W, SR, SW, T, TFut> ProcessStderr<R, W, SR, SW, T, TFut> {
    pub fn with_source<NW, NSR>(
        self,
        writer: NW,
        source: NSR,
    ) -> ProcessStderr<R, NW, NSR, SW, T, TFut> {
        ProcessStderr {
            result: self.result,
            reader: self.reader,
            writer: Some(writer),
            source: Some(source),
            sink: self.sink,
            read_result: self.read_result,
            _result_type: PhantomData,
        }
    }
    /*
    pub fn with_sink<NSW>(self, sink: NSW) -> ProcessStderr<R, W, SR, NSW, T, TFut> {
        ProcessStderr {
            result: self.result,
            reader: self.reader,
            writer: self.writer,
            source: self.source,
            sink: Some(sink),
            read_result: self.read_result,
            _result_type: PhantomData,
        }
    }
     */

    pub fn result_fn<F, FFut>(self, f: F) -> ProcessStderr<R, W, SR, SW, T, ResultFn<F, FFut>>
    where
        F: FnOnce(Result<(), DaemonError>, R, Option<W>, Option<SR>, Option<SW>) -> FFut,
        FFut: Future<Output = DaemonResult<T>>,
    {
        ProcessStderr {
            result: self.result,
            reader: self.reader,
            writer: self.writer,
            source: self.source,
            sink: self.sink,
            read_result: ResultFn {
                f,
                _res: PhantomData,
            },
            _result_type: PhantomData,
        }
    }
}

impl<R, W, SR, SW, T, TFut> ProcessStderr<R, W, SR, SW, T, TFut>
where
    W: NixWrite + AsyncWrite + fmt::Debug + Unpin + Send,
    DaemonError: From<<W as NixWrite>::Error>,
    SR: AsyncBufRead + Unpin + Send,
{
    async fn process_read(&mut self, mut len: usize) -> Result<(), DaemonError> {
        if let Some(source) = self.source.as_mut() {
            let buf = source.fill_buf().await?;
            let writer = self.writer.as_mut().unwrap();
            if buf.len() > len {
                writer.write_slice(&buf[..len]).await?;
            } else {
                len = buf.len();
                writer.write_slice(buf).await?;
            }
            source.consume(len);
            Ok(())
        } else {
            Err(DaemonErrorKind::NoSourceForLoggerRead.into())
        }
    }
}

pin_project! {
    pub struct ResultProcess<S, R> {
        #[pin]
        pub stream: S,
        #[pin]
        pub result: R,
    }
}

impl<S, R> Stream for ResultProcess<S, R>
where
    S: Stream<Item = LogMessage>,
{
    type Item = LogMessage;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        self.project().stream.poll_next(cx)
    }
}

impl<S, R, T, E> Future for ResultProcess<S, R>
where
    S: Stream<Item = LogMessage>,
    R: Future<Output = Result<T, E>>,
{
    type Output = Result<T, E>;

    fn poll(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        let mut me = self.project();
        while ready!(me.stream.as_mut().poll_next(cx)).is_some() {}
        me.result.poll(cx)
    }
}

pin_project! {
    pub struct DriveResult<R, D, E> {
        #[pin]
        pub result: R,
        #[pin]
        pub driver: D,
        pub driving: bool,
        pub drive_err: Option<E>,
    }
}

impl<R, D, E> Stream for DriveResult<R, D, E>
where
    R: Stream<Item = LogMessage>,
    D: Future<Output = Result<(), E>>,
{
    type Item = LogMessage;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let me = self.project();
        if *me.driving {
            if let Poll::Ready(res) = me.driver.poll(cx) {
                *me.driving = false;
                *me.drive_err = res.err();
            }
        }
        if me.drive_err.is_some() {
            return Poll::Ready(None);
        }
        me.result.poll_next(cx)
    }
}

impl<R, D, T, E> Future for DriveResult<R, D, E>
where
    R: Future<Output = Result<T, E>>,
    D: Future<Output = Result<(), E>>,
{
    type Output = Result<T, E>;

    fn poll(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        let me = self.project();
        if *me.driving {
            if let Poll::Ready(res) = me.driver.poll(cx) {
                *me.driving = false;
                *me.drive_err = res.err();
            }
        }
        if let Some(err) = me.drive_err.take() {
            return Poll::Ready(Err(err));
        }
        me.result.poll(cx)
    }
}

impl<R, W, SR, SW, T, TFut> ProcessStderr<R, W, SR, SW, T, TFut>
where
    R: NixRead + AsyncRead + fmt::Debug + Unpin + Send,
    W: NixWrite + AsyncWrite + fmt::Debug + Unpin + Send,
    DaemonError: From<<W as NixWrite>::Error> + From<<R as NixRead>::Error>,
    SR: AsyncBufRead + Unpin + Send,
    SW: AsyncWrite + fmt::Debug + Unpin + Send,
    TFut: ReadResult<R, W, SR, SW, T> + Send,
    T: Send,
{
    pub fn stream(mut self) -> impl Stream<Item = LogMessage> + Future<Output = DaemonResult<T>> {
        let (sender, receiver) = oneshot::channel();
        ResultProcess {
            stream: stream! {
                loop {
                    let msg = self.reader.read_value::<RawLogMessage>().await;
                    match msg {
                        Ok(RawLogMessage::Next(msg)) => {
                            yield LogMessage::Next(msg);
                        }
                        Ok(RawLogMessage::Result(result)) => {
                            yield LogMessage::Result(result);
                        }
                        Ok(RawLogMessage::StartActivity(act)) => {
                            yield LogMessage::StartActivity(act);
                        }
                        Ok(RawLogMessage::StopActivity(act)) => {
                            yield LogMessage::StopActivity(act);
                        }
                        Ok(RawLogMessage::Read(len)) => {
                            if let Err(err) = self.process_read(len).await {
                                self.result = Some(Err(err));
                                break;
                            }
                        }
                        Ok(RawLogMessage::Write(buf)) => {
                            if let Some(sink) = self.sink.as_mut() {
                                if let Err(err) = sink.write_all(&buf).await {
                                    self.result = Some(Err(DaemonError::from(err)));
                                    break;
                                }
                            }
                        }
                        Ok(RawLogMessage::Last) => {
                            let value = self.reader.read_value().await;
                            self.result = Some(value.map_err(|err| err.into()));
                            break;
                        }
                        Ok(RawLogMessage::Error(err)) => {
                            self.result = Some(Err(err.into()));
                            break;
                        }
                        Err(err) => {
                            self.result = Some(Err(DaemonError::from(err)));
                            break;
                        }
                    }
                }
                let _ = sender.send(self);
            },
            result: async {
                let process = receiver.await.unwrap();
                process
                    .read_result
                    .read_result(
                        process.result.unwrap(),
                        process.reader,
                        process.writer,
                        process.source,
                        process.sink,
                    )
                    .await
            },
        }
    }
}
