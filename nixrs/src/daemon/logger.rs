use std::future::Future;
use std::marker::PhantomData;
use std::pin::{Pin, pin};
use std::task::{Context, Poll, ready};

use futures::channel::mpsc;
use futures::future::{MaybeDone, maybe_done};
use futures::stream::{Empty, empty};
use futures::{Sink, SinkExt, Stream, StreamExt};
use pin_project_lite::pin_project;
use tokio::io::{AsyncBufRead, AsyncRead};

use super::{DaemonError, DaemonResult};
use crate::daemon::wire::types::Operation;
use crate::log::LogMessage;

pub trait ResultLog: Stream<Item = LogMessage> + Future {}
impl<RL> ResultLog for RL where RL: Stream<Item = LogMessage> + Future {}

pin_project! {
    #[derive(Clone)]
    pub struct LogSender {
        #[pin]
        inner: mpsc::Sender<LogMessage>,
    }
}
impl Sink<LogMessage> for LogSender {
    type Error = DaemonError;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(ready!(self.project().inner.poll_ready(cx)).map_err(DaemonError::custom))
    }

    fn start_send(self: Pin<&mut Self>, item: LogMessage) -> Result<(), Self::Error> {
        self.project()
            .inner
            .start_send(item)
            .map_err(DaemonError::custom)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(ready!(self.project().inner.poll_flush(cx)).map_err(DaemonError::custom))
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(ready!(self.project().inner.poll_close(cx)).map_err(DaemonError::custom))
    }
}

pub fn make_result<F, Fut, T>(f: F) -> impl ResultLog<Output = DaemonResult<T>>
where
    F: FnOnce(LogSender) -> Fut,
    Fut: Future<Output = DaemonResult<T>>,
{
    let (inner, receiver) = mpsc::channel(1);
    let mut sender = LogSender { inner };
    let fut = f(sender.clone());
    <Fut as futures::FutureExt>::then(fut, move |ret| async move {
        let _ = sender.close().await;
        ret
    })
    .with_logs(receiver)
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

pub trait ResultLogExt: ResultLog {
    fn map_ok<F, E, T, T2>(self, f: F) -> impl ResultLog<Output = Result<T2, E>>
    where
        Self: ResultLog<Output = Result<T, E>>,
        F: FnOnce(T) -> T2 + Send,
        T: Send,
        T2: 'static;
    fn map_err<F, E, T, E2>(self, f: F) -> impl ResultLog<Output = Result<T, E2>>
    where
        Self: ResultLog<Output = Result<T, E>>,
        F: FnOnce(E) -> E2 + Send,
        E: Send,
        E2: 'static;
    fn and_then<F, E, T, T2, Fut>(self, f: F) -> impl ResultLog<Output = Result<T2, E>>
    where
        Self: ResultLog<Output = Result<T, E>>,
        F: FnOnce(T) -> Fut + Send,
        Fut: Future<Output = Result<T2, E>> + Send,
        T2: 'static;
    fn fill_operation<T>(self, op: Operation) -> impl ResultLog<Output = Result<T, DaemonError>>
    where
        Self: ResultLog<Output = Result<T, DaemonError>>;
    fn boxed_result<'a>(self) -> Pin<Box<dyn ResultLog<Output = Self::Output> + Send + 'a>>
    where
        Self: Send + 'a;
    fn boxed_local_result<'a>(self) -> Pin<Box<dyn ResultLog<Output = Self::Output> + 'a>>
    where
        Self: 'a;
    fn forward_logs<S, T, E, E2>(self, sink: S) -> impl Future<Output = Self::Output>
    where
        Self: ResultLog<Output = Result<T, E>>,
        S: Sink<LogMessage, Error = E2>,
        E: From<E2>;
}

impl<L> ResultLogExt for L
where
    L: ResultLog,
{
    fn map_ok<F, E, T, T2>(self, f: F) -> impl ResultLog<Output = Result<T2, E>>
    where
        Self: ResultLog<Output = Result<T, E>>,
        F: FnOnce(T) -> T2 + Send,
        T: Send,
        T2: 'static,
    {
        MapOkResult {
            result: self,
            mapper: Some(f),
            value: PhantomData,
        }
    }

    fn map_err<F, E, T, E2>(self, f: F) -> impl ResultLog<Output = Result<T, E2>>
    where
        Self: ResultLog<Output = Result<T, E>>,
        F: FnOnce(E) -> E2 + Send,
        E: Send,
        E2: 'static,
    {
        MapErrResult {
            result: self,
            mapper: Some(f),
            value: PhantomData,
        }
    }
    fn and_then<F, E, T, T2, Fut>(self, f: F) -> impl ResultLog<Output = Result<T2, E>>
    where
        Self: ResultLog<Output = Result<T, E>>,
        F: FnOnce(T) -> Fut + Send,
        Fut: Future<Output = Result<T2, E>> + Send,
        T2: 'static,
    {
        AndThenLog {
            stream: self,
            result: AndThenLogResult::First { mapper: Some(f) },
        }
    }

    fn fill_operation<T>(self, op: Operation) -> impl ResultLog<Output = Result<T, DaemonError>>
    where
        Self: ResultLog<Output = Result<T, DaemonError>>,
    {
        self.map_err(move |err| err.fill_operation(op))
    }
    fn boxed_result<'a>(self) -> Pin<Box<dyn ResultLog<Output = Self::Output> + Send + 'a>>
    where
        Self: Send + 'a,
    {
        Box::pin(self)
    }
    fn boxed_local_result<'a>(self) -> Pin<Box<dyn ResultLog<Output = Self::Output> + 'a>>
    where
        Self: 'a,
    {
        Box::pin(self)
    }

    async fn forward_logs<S, T, E, E2>(self, sink: S) -> Self::Output
    where
        Self: ResultLog<Output = Result<T, E>>,
        S: Sink<LogMessage, Error = E2>,
        E: From<E2>,
    {
        let mut res = pin!(self);
        let mut try_res = (&mut res).map(Ok);
        let mut sink = pin!(sink);
        sink.send_all(&mut try_res).await?;
        res.await
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

impl<Fut, R, E> FutureResult<Fut, R, E>
where
    Fut: Future<Output = Result<R, E>>,
{
    pub fn new(fut: Fut) -> Self {
        Self::Later { fut }
    }

    fn poll_resolved(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Pin<&mut R>, ()>> {
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
    Fut: Future<Output = Result<R, E>>,
    R: Stream<Item = LogMessage>,
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
    Fut: Future<Output = Result<R, E>>,
    R: Future<Output = Result<T, E>>,
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

pub trait FutureResultExt: Future {
    /// Create a [ResultLog] with an empty log from a [Future]
    ///
    /// This takes any [Future] and makes it into a [ResultLog] implementation
    /// that just returns [Option::None] for the [Stream] part while
    /// [Future::Output] is used as the output for the [ResultLog].
    ///
    /// ```rust
    /// # use std::future::{ready, Future};
    /// # use std::pin::pin;
    /// # use futures::stream::StreamExt as _;
    /// use nixrs::daemon::FutureResultExt as _;
    /// # tokio_test::block_on(async {
    /// let result = ready(12).empty_logs();
    /// let mut rp = pin!(result);
    /// assert!(rp.next().await.is_none());
    /// assert_eq!(rp.await, 12);
    /// # })
    /// ```
    fn empty_logs(self) -> impl ResultLog<Output = Self::Output>;

    /// Create a [ResultLog] with the provided logs and with the output from
    /// the [Future]
    ///
    ///
    fn with_logs<S>(self, logs: S) -> impl ResultLog<Output = Self::Output>
    where
        S: Stream<Item = LogMessage>;

    /// Flattens a [Future] that outputs a [ResultLog] to a [ResultLog]
    ///
    /// ```rust
    /// # use std::future::{ready, Future};
    /// # use std::pin::pin;
    /// # use futures::stream::StreamExt as _;
    /// # use nixrs::daemon::DaemonResult;
    /// use nixrs::daemon::FutureResultExt as _;
    /// # tokio_test::block_on(async {
    /// let result = async {
    ///     Ok(ready(Ok(12) as DaemonResult<i32>).empty_logs())
    /// }.future_result();
    /// let mut rp = pin!(result);
    /// assert!(rp.next().await.is_none());
    /// assert_eq!(rp.await.unwrap(), 12);
    /// # })
    /// ```
    fn future_result<R, T, E>(self) -> impl ResultLog<Output = Result<T, E>>
    where
        Self: Future<Output = Result<R, E>>,
        R: ResultLog<Output = Result<T, E>>;
}

impl<F> FutureResultExt for F
where
    F: Future,
{
    fn empty_logs(self) -> impl ResultLog<Output = Self::Output> {
        ResultProcess::empty(self)
    }

    fn with_logs<S>(self, logs: S) -> impl ResultLog<Output = Self::Output>
    where
        S: Stream<Item = LogMessage>,
    {
        ResultProcess {
            stream: logs,
            result: maybe_done(self),
        }
    }

    fn future_result<R, T, E>(self) -> impl ResultLog<Output = Result<T, E>>
    where
        Self: Future<Output = Result<R, E>>,
        R: ResultLog<Output = Result<T, E>>,
    {
        FutureResult::new(self)
    }
}

pin_project! {
    pub struct ResultProcess<S, R: Future> {
        #[pin]
        pub stream: S,
        #[pin]
        pub result: MaybeDone<R>,
    }
}

impl<R: Future> ResultProcess<Empty<LogMessage>, R> {
    pub fn empty(result: R) -> Self {
        Self {
            stream: empty(),
            result: maybe_done(result),
        }
    }
}

impl<S, R: Future> Stream for ResultProcess<S, R>
where
    S: Stream<Item = LogMessage>,
{
    type Item = LogMessage;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let me = self.project();
        let _ = me.result.poll(cx);
        me.stream.poll_next(cx)
    }
}

impl<S, R> Future for ResultProcess<S, R>
where
    S: Stream<Item = LogMessage>,
    R: Future,
{
    type Output = R::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        let mut me = self.project();
        loop {
            let _ = me.result.as_mut().poll(cx);
            if ready!(me.stream.as_mut().poll_next(cx)).is_none() {
                break;
            }
        }
        ready!(me.result.as_mut().poll(cx));
        Poll::Ready(me.result.take_output().unwrap())
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

impl<R, D, E> DriveResult<R, D, E>
where
    D: Future<Output = Result<(), E>>,
{
    pub fn new(result: R, driver: D) -> Self {
        Self {
            result,
            driver,
            driving: true,
            drive_err: None,
        }
    }

    fn drive(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) {
        let me = self.project();
        if *me.driving
            && let Poll::Ready(res) = me.driver.poll(cx)
        {
            *me.driving = false;
            *me.drive_err = res.err();
        }
    }
    fn take_drive(mut self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Result<(), E> {
        self.as_mut().drive(cx);
        let me = self.project();
        if let Some(err) = me.drive_err.take() {
            Err(err)
        } else {
            Ok(())
        }
    }
}

impl<R, D, E> Stream for DriveResult<R, D, E>
where
    R: Stream<Item = LogMessage>,
    D: Future<Output = Result<(), E>>,
{
    type Item = LogMessage;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        self.as_mut().drive(cx);
        let me = self.project();
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

    fn poll(mut self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        self.as_mut().take_drive(cx)?;
        self.project().result.poll(cx)
    }
}

impl<R, D> AsyncRead for DriveResult<R, D, std::io::Error>
where
    R: AsyncRead,
    D: Future<Output = Result<(), std::io::Error>>,
{
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        self.as_mut().take_drive(cx)?;
        self.project().result.poll_read(cx, buf)
    }
}

impl<R, D> AsyncBufRead for DriveResult<R, D, std::io::Error>
where
    R: AsyncBufRead,
    D: Future<Output = Result<(), std::io::Error>>,
{
    fn poll_fill_buf(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<std::io::Result<&[u8]>> {
        self.as_mut().take_drive(cx)?;
        self.project().result.poll_fill_buf(cx)
    }

    fn consume(self: Pin<&mut Self>, amt: usize) {
        self.project().result.consume(amt);
    }
}
