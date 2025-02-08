use std::collections::VecDeque;
use std::fmt;
use std::future::{ready, Future};
use std::io::Cursor;
use std::marker::PhantomData;

#[cfg(feature = "nixrs-derive")]
use nixrs_derive::{NixDeserialize, NixSerialize};
use num_enum::{FromPrimitive, IntoPrimitive, TryFromPrimitive};
#[cfg(any(test, feature = "test"))]
use proptest_derive::Arbitrary;
use tokio::io::{AsyncBufRead, AsyncBufReadExt as _, AsyncRead, AsyncWrite, AsyncWriteExt as _};

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

/// Credit embr and gorgon
pub trait LoggerResult<T, E>: Send {
    fn next<'s>(&'s mut self) -> impl Future<Output = Option<Result<LogMessage, E>>> + Send + 's;
    fn result(self) -> impl Future<Output = Result<T, E>> + Send;
}

struct MapOkResult<L, F, T> {
    result: L,
    mapper: F,
    value: PhantomData<T>,
}

impl<L, F, T, T2, E> LoggerResult<T2, E> for MapOkResult<L, F, T>
where
    L: LoggerResult<T, E>,
    F: FnOnce(T) -> T2 + Send,
    T: Send,
    E: 'static,
    T2: 'static,
{
    async fn next(&mut self) -> Option<Result<LogMessage, E>> {
        self.result.next().await
    }

    async fn result(self) -> Result<T2, E> {
        let original = self.result.result().await?;
        Ok((self.mapper)(original))
    }
}

struct MapErrResult<L, F, E> {
    result: L,
    mapper: F,
    value: PhantomData<E>,
}

impl<L, F, T, E2, E> LoggerResult<T, E2> for MapErrResult<L, F, E>
where
    L: LoggerResult<T, E>,
    F: Fn(E) -> E2 + Send,
    T: 'static,
    E: Send,
    E2: 'static,
{
    async fn next(&mut self) -> Option<Result<LogMessage, E2>> {
        match self.result.next().await {
            None => None,
            Some(Ok(ret)) => Some(Ok(ret)),
            Some(Err(err)) => {
                Some(Err((self.mapper)(err)))
            }
        }
    }

    async fn result(self) -> Result<T, E2> {
        match self.result.result().await {
            Ok(res) => Ok(res),
            Err(err) => Err((self.mapper)(err))
        }
    }
}
struct AndThenResult<L, F, T, Fut> {
    result: L,
    mapper: F,
    value: PhantomData<(T, Fut)>,
}

impl<L, F, T, T2, E, Fut> LoggerResult<T2, E> for AndThenResult<L, F, T, Fut>
where
    L: LoggerResult<T, E>,
    F: FnOnce(T) -> Fut + Send,
    Fut: Future<Output = Result<T2, E>> + Send,
    T: Send,
    T2: 'static,
    E: 'static,
{
    async fn next(&mut self) -> Option<Result<LogMessage, E>> {
        self.result.next().await
    }

    async fn result(self) -> Result<T2, E> {
        let original = self.result.result().await?;
        (self.mapper)(original).await
    }
}

pub trait LoggerResultExt<T, E> {
    fn map_ok<F, T2>(self, f: F) -> impl LoggerResult<T2, E>
    where
        F: FnOnce(T) -> T2 + Send,
        T2: 'static;
    fn map_err<F, E2>(self, f: F) -> impl LoggerResult<T, E2>
    where
        F: Fn(E) -> E2 + Send,
        E2: 'static;
    fn and_then<F, T2, Fut>(self, f: F) -> impl LoggerResult<T2, E>
    where
        F: FnOnce(T) -> Fut + Send,
        Fut: Future<Output = Result<T2, E>> + Send,
        T2: 'static;
}

impl<L, T, E> LoggerResultExt<T, E> for L
where
    L: LoggerResult<T, E>,
    T: Send + 'static,
    E: Send + 'static,
{
    fn map_ok<F, T2>(self, f: F) -> impl LoggerResult<T2, E>
    where
        F: FnOnce(T) -> T2 + Send,
        T2: 'static,
    {
        MapOkResult {
            result: self,
            mapper: f,
            value: PhantomData,
        }
    }

    fn map_err<F, E2>(self, f: F) -> impl LoggerResult<T, E2>
    where
        F: Fn(E) -> E2 + Send,
        E2: 'static,
    {
        MapErrResult {
            result: self,
            mapper: f,
            value: PhantomData,
        }
    }

    fn and_then<F, T2, Fut>(self, f: F) -> impl LoggerResult<T2, E>
    where
        F: FnOnce(T) -> Fut + Send,
        Fut: Future<Output = Result<T2, E>> + Send,
        T2: 'static,
    {
        AndThenResult {
            result: self,
            mapper: f,
            value: PhantomData,
        }
    }
}

impl<E: Send + 'static> LoggerResult<(), E> for VecDeque<LogMessage> {
    fn next(&mut self) -> impl Future<Output = Option<Result<LogMessage, E>>> + Send {
        ready(self.pop_front().map(Ok))
    }

    fn result(self) -> impl Future<Output = Result<(), E>> {
        ready(Ok(()))
    }
}

impl<T, E> LoggerResult<T, E> for Result<T, E>
    where E: Send,
          T: Send,
{
    fn next(&mut self) -> impl Future<Output=Option<Result<LogMessage, E>>> + Send {
        ready(None)
    }

    fn result(self) -> impl Future<Output=Result<T, E>> + Send {
        ready(self)
    }
}

#[derive(Default)]
pub enum FutureResult<Fut, T, E> {
    Later {
        fut: Fut,
    },
    Resolved {
        result: Result<T, E>,
    },
    #[default]
    Invalid,
}

impl<Fut, T, E> FutureResult<Fut, T, E>
where
    Fut: Future<Output = Result<T, E>>,
{
    pub fn new(fut: Fut) -> Self {
        Self::Later { fut }
    }

    async fn resolved(&mut self) -> Result<&mut T, &mut E> {
        let this = std::mem::take(self);
        let result = match this {
            FutureResult::Invalid => panic!("Resolving invalid FutureResult"),
            FutureResult::Later { fut } => fut.await,
            FutureResult::Resolved { result } => result,
        };
        *self = FutureResult::Resolved { result };
        match self {
            FutureResult::Resolved { result } => result.as_mut(),
            _ => unreachable!(),
        }
    }
}

impl<Fut, R, T, E> LoggerResult<T, E> for FutureResult<Fut, R, E>
where
    Fut: Future<Output = Result<R, E>> + Send,
    R: LoggerResult<T, E>,
    E: Clone + Send,
    T: 'static,
{
    async fn next(&mut self) -> Option<Result<LogMessage, E>> {
        match self.resolved().await {
            Ok(r) => r.next().await,
            Err(_err) => None,
        }
    }

    async fn result(mut self) -> Result<T, E> {
        let _ = self.resolved().await;
        match self {
            FutureResult::Resolved { result: Err(err) } => Err(err),
            FutureResult::Resolved { result: Ok(result) } => result.result().await,
            _ => panic!("Invalid state"),
        }
    }
}

/*
impl<F, R, T, E> LoggerResult<T, E> for F
    where F: Future<Output = Result<R, E>>,
          R: LoggerResult<T, E>,
{
    async fn next(&mut self) -> Option<Result<LogMessage, E>> {
        poll_fn(|cx| self.poll(cx)).await?;
    }

    async fn value(&mut self) -> Result<T,E> {
        todo!()
    }
}
*/

trait ReadResult<R, W, SR, SW, T>: Sized {
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
    /*
    pub fn with_source<NW, NSR>(self, writer: NW, source: NSR) -> ProcessStderr<R, NW, NSR, SW, T, TFut> {
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
    SR: AsyncBufRead + fmt::Debug + Unpin + Send,
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

impl<R, W, SR, SW, T, TFut> LoggerResult<T, DaemonError> for ProcessStderr<R, W, SR, SW, T, TFut>
where
    R: NixRead + AsyncRead + fmt::Debug + Unpin + Send,
    W: NixWrite + AsyncWrite + fmt::Debug + Unpin + Send,
    DaemonError: From<<W as NixWrite>::Error> + From<<R as NixRead>::Error>,
    SR: AsyncBufRead + fmt::Debug + Unpin + Send,
    SW: AsyncWrite + fmt::Debug + Unpin + Send,
    TFut: ReadResult<R, W, SR, SW, T> + Send,
    T: Send,
{
    async fn next(&mut self) -> Option<Result<LogMessage, DaemonError>> {
        if self.result.is_some() {
            return None;
        }
        loop {
            eprintln!("Client reading message!");
            let msg = self.reader.read_value::<RawLogMessage>().await;
            eprintln!("Client read message {:?}", msg);
            match msg {
                Ok(RawLogMessage::Next(msg)) => {
                    return Some(Ok(LogMessage::Next(msg)));
                }
                Ok(RawLogMessage::Result(result)) => {
                    return Some(Ok(LogMessage::Result(result)));
                }
                Ok(RawLogMessage::StartActivity(act)) => {
                    return Some(Ok(LogMessage::StartActivity(act)));
                }
                Ok(RawLogMessage::StopActivity(act)) => {
                    return Some(Ok(LogMessage::StopActivity(act)));
                }
                Ok(RawLogMessage::Read(len)) => {
                    if let Err(err) = self.process_read(len).await {
                        return Some(Err(err));
                    }
                }
                Ok(RawLogMessage::Write(buf)) => {
                    if let Some(sink) = self.sink.as_mut() {
                        if let Err(err) = sink.write_all(&buf).await {
                            return Some(Err(err.into()));
                        }
                    }
                }
                Ok(RawLogMessage::Last) => {
                    let value = self.reader.read_value().await;
                    self.result = Some(value.map_err(|err| err.into()));
                    return None;
                }
                Ok(RawLogMessage::Error(err)) => {
                    self.result = Some(Err(err.into()));
                    return None;
                }
                Err(err) => {
                    return Some(Err(err.into()));
                }
            }
        }
    }

    async fn result(mut self) -> Result<T, DaemonError> {
        while let Some(msg) = self.next().await {
            msg?;
        }
        self.read_result
            .read_result(
                self.result.unwrap(),
                self.reader,
                self.writer,
                self.source,
                self.sink,
            )
            .await
    }
}
