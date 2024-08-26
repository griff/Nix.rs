use std::collections::BTreeMap;
use std::fmt;
use std::future::Future;
use std::io::{self, Cursor};
use std::pin::Pin;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::task::Poll;

use bytes::{Buf, Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::sync::{mpsc, oneshot};
use tracing::field::Visit;
use tracing::span;
use tracing::{debug, error, instrument, trace, Event, Subscriber};
use tracing_futures::WithSubscriber;
use tracing_subscriber::prelude::*;
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::Layer;
use tracing_subscriber::{layer, registry};

use super::{
    get_protocol_major, get_protocol_minor, DaemonStore, TrustedFlag, WorkerProtoOp,
    PROTOCOL_VERSION, STDERR_ERROR, STDERR_LAST, STDERR_NEXT, STDERR_READ, STDERR_RESULT,
    STDERR_START_ACTIVITY, STDERR_STOP_ACTIVITY, WORKER_MAGIC_1, WORKER_MAGIC_2,
};
use crate::hash;
use crate::io::{AsyncSink, AsyncSource, FramedSource, TakenStream, Taker};
use crate::path_info::ValidPathInfo;
use crate::signature::{ParseSignatureError, SignatureSet};
use crate::store::activity::{ActivityResult, LoggerField, LoggerFieldType, StartActivity};
use crate::store::error::Verbosity;
use crate::store::settings::{get_mut_settings, BuildSettings, WithSettings};
use crate::store::{
    BasicDerivation, BuildMode, CheckSignaturesFlag, DerivedPath, DrvOutputs, Error,
    StorePathWithOutputs, SubstituteFlag,
};
use crate::store_path::{StoreDir, StorePath};
use crate::tracing::ParentLayer;

#[derive(Debug, Clone)]
struct ActiveVerbosity(Arc<AtomicU64>);

impl Default for ActiveVerbosity {
    fn default() -> Self {
        Self(Arc::new(AtomicU64::new(Verbosity::Info.into())))
    }
}

impl ActiveVerbosity {
    fn set(&self, level: Verbosity) {
        self.0.store(level.into(), Ordering::SeqCst)
    }

    fn get(&self) -> Verbosity {
        self.0.load(Ordering::SeqCst).into()
    }
}

struct OpCounter {
    dispatcher: tracing::Dispatch,
    op_count: Arc<AtomicU32>,
}

impl OpCounter {
    fn new() -> OpCounter {
        let dispatcher = tracing::dispatcher::get_default(|d| d.clone());
        let op_count = Arc::new(AtomicU32::new(0));
        OpCounter {
            dispatcher,
            op_count,
        }
    }
    fn report_op(&self, op: WorkerProtoOp) {
        tracing::dispatcher::with_default(&self.dispatcher, || {
            debug!("received daemon op '{}'", op);
        });
        let mut old = self.op_count.load(Ordering::Relaxed);
        loop {
            let new = old + 1;
            match self
                .op_count
                .compare_exchange_weak(old, new, Ordering::SeqCst, Ordering::Relaxed)
            {
                Ok(_) => break,
                Err(x) => old = x,
            }
        }
    }
}

impl Drop for OpCounter {
    fn drop(&mut self) {
        //_isInterrupted = false;
        let old = self.op_count.load(Ordering::Relaxed);
        tracing::dispatcher::with_default(&self.dispatcher, || {
            debug!("{} operations", old);
        });
    }
}

async fn send_command<W>(
    level: ActiveVerbosity,
    client_version: u64,
    writer: &mut W,
    cmd: TunnelCommand,
) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    match cmd {
        TunnelCommand::LogNext(msg) => {
            eprintln!("Log next: {}", msg);
            debug!("Log next: {}", msg);
            writer.write_u64_le(STDERR_NEXT).await?;
            writer.write_string(format!("{}\n", msg)).await?;
        }
        TunnelCommand::StartActivity(id, activity) => {
            eprintln!("!!!!!!!!!! start activity {}", id);
            debug!(id, "start activity {} {:?}", id, activity);
            if get_protocol_minor!(client_version) < 20 {
                if !activity.text.is_empty() && level.get() >= activity.level {
                    writer.write_u64_le(STDERR_NEXT).await?;
                    writer
                        .write_string(format!("{}...\n", activity.text))
                        .await?;
                }
                return Ok(());
            }
            writer.write_u64_le(STDERR_START_ACTIVITY).await?;
            writer.write_u64_le(activity.act).await?;
            writer.write_enum(activity.level).await?;
            writer.write_enum(activity.activity_type).await?;
            writer.write_str(&activity.text).await?;
            writer.write_usize(activity.fields.len()).await?;
            for field in activity.fields.iter() {
                match field {
                    LoggerField::Int(i) => {
                        writer.write_enum(LoggerFieldType::Int).await?;
                        writer.write_u64_le(*i).await?;
                    }
                    LoggerField::String(s) => {
                        writer.write_enum(LoggerFieldType::String).await?;
                        writer.write_str(s).await?;
                    }
                }
            }
            writer.write_u64_le(activity.parent).await?;
        }
        TunnelCommand::StopActivity(id) => {
            eprintln!("!!!!!!!! stop activity {}", id);
            debug!(id, "stop activity {}", id);
            if get_protocol_minor!(client_version) < 20 {
                return Ok(());
            }
            writer.write_u64_le(STDERR_STOP_ACTIVITY).await?;
            writer.write_u64_le(id).await?;
        }
        TunnelCommand::Result(result) => {
            eprintln!("!!!!!!!!! result {}, {:?}", result.act, result);
            debug!("result {}, {:?}", result.act, result);
            if get_protocol_minor!(client_version) < 20 {
                return Ok(());
            }
            writer.write_u64_le(STDERR_RESULT).await?;
            writer.write_u64_le(result.act).await?;
            writer.write_enum(result.result_type).await?;
            writer.write_usize(result.fields.len()).await?;
            for field in result.fields.iter() {
                match field {
                    LoggerField::Int(i) => {
                        writer.write_enum(LoggerFieldType::Int).await?;
                        writer.write_u64_le(*i).await?;
                    }
                    LoggerField::String(s) => {
                        writer.write_enum(LoggerFieldType::String).await?;
                        writer.write_str(s).await?;
                    }
                }
            }
        }
        TunnelCommand::Read(len) => {
            eprintln!("read {}", len);
            debug!(len, "read {}", len);
            writer.write_u64_le(STDERR_READ).await?;
            writer.write_usize(len).await?;
        }
        _ => unreachable!(),
    }
    Ok(())
}

async fn process_tunnel<S>(
    level: ActiveVerbosity,
    client_version: u64,
    taker: Taker<S>,
    mut receiver: mpsc::Receiver<TunnelCommand>,
) where
    S: AsyncWrite + Send + Unpin,
{
    let mut buf = Vec::new();
    let mut writer = None;

    while let Some(cmd) = receiver.recv().await {
        eprintln!("Got Command {:?}", cmd);
        match cmd {
            TunnelCommand::StartWork => {
                eprintln!("Start work");
                debug!("Start work");
                let mut s = taker.take();
                if let Err(err) = s.write_all(&buf).await {
                    error!("Cound not write data for tunnel start work {}", err);
                }
                if let Err(err) = s.flush().await {
                    error!("Cound not flush data for tunnel start work {}", err);
                }
                writer = Some(s);
            }
            TunnelCommand::StopWork(err, reply) => {
                eprintln!("Stop work");
                debug!("Stop work");
                let mut stream = writer.take().unwrap_or_else(|| taker.take());
                let res = async {
                    if let Some(err) = err {
                        stream.write_u64_le(STDERR_ERROR).await?;
                        stream.write_all(&err).await?;
                    } else {
                        stream.write_u64_le(STDERR_LAST).await?
                    }
                    Ok(()) as io::Result<()>
                }
                .await;
                if let Err(err) = res {
                    error!("Cound not write data for tunnel stop work {}", err);
                }
                drop(stream);
                let _ = reply.send(());
            }
            _ if writer.is_some() => {
                let mut s = writer.as_mut().unwrap();
                if let Err(err) = send_command(level.clone(), client_version, &mut s, cmd).await {
                    error!("Could not write tunnel command: {}", err);
                }
                if let Err(err) = s.flush().await {
                    error!("Could not flush tunnel command: {}", err);
                }
            }
            _ => {
                if let Err(err) = send_command(
                    level.clone(),
                    client_version,
                    &mut Cursor::new(&mut buf),
                    cmd,
                )
                .await
                {
                    error!("Could not write tunnel command: {}", err);
                }
            }
        }
    }
}

#[derive(Debug)]
enum TunnelCommand {
    StartWork,
    StopWork(Option<Vec<u8>>, oneshot::Sender<()>),
    LogNext(String),
    StartActivity(u64, StartActivity),
    StopActivity(u64),
    Result(ActivityResult),
    Read(usize),
}

fn format_event(a_level: ActiveVerbosity, event: &Event<'_>) -> Option<TunnelCommand> {
    let mut fmt = EventFormat::default();
    event.record(&mut fmt);
    let level = if let Some(lvl) = fmt.level {
        lvl
    } else {
        event.metadata().level().into()
    };
    if a_level.get() >= level {
        let message = if let Some(msg) = fmt.message {
            msg
        } else {
            format!("{:?}", event)
        };
        Some(TunnelCommand::LogNext(message))
    } else {
        None
    }
}

#[derive(Default)]
struct EventFormat {
    message: Option<String>,
    level: Option<Verbosity>,
}

impl Visit for EventFormat {
    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        if field.name() == "level" {
            self.level = Some(value.into())
        }
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        if field.name() == "message" {
            self.message = Some(value.to_string());
        }
    }

    fn record_debug(&mut self, _field: &tracing::field::Field, _value: &dyn std::fmt::Debug) {}
}

type ReadingFut<'r, R> = dyn Future<Output = io::Result<(Bytes, &'r mut R)>> + Send + 'r;

pub enum TunnelSourceOp<'r, R> {
    Available(&'r mut R, Bytes),
    Reading(Pin<Box<ReadingFut<'r, R>>>),
    Empty(&'r mut R),
    Invalid,
}

impl<'r, R> fmt::Debug for TunnelSourceOp<'r, R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Available(_, _) => f.debug_tuple("Available").finish(),
            Self::Reading(_) => f.debug_tuple("Reading").finish(),
            Self::Empty(_) => f.debug_tuple("Empty").finish(),
            Self::Invalid => write!(f, "Invalid"),
        }
    }
}

#[derive(Debug)]
pub struct TunnelSource<'r, R> {
    state: TunnelSourceOp<'r, R>,
    buffer: BytesMut,
    cut_off: usize,
    sender: mpsc::Sender<TunnelCommand>,
}

impl<'r, R> TunnelSource<'r, R> {
    fn with_capacity(
        reader: &'r mut R,
        sender: mpsc::Sender<TunnelCommand>,
        capacity: usize,
    ) -> TunnelSource<'r, R> {
        TunnelSource {
            state: TunnelSourceOp::Empty(reader),
            buffer: BytesMut::with_capacity(capacity),
            cut_off: capacity / 4,
            sender,
        }
    }
}

impl<'r, R> AsyncRead for TunnelSource<'r, R>
where
    R: AsyncRead + Unpin + Send + 'r,
{
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        loop {
            match std::mem::replace(&mut self.state, TunnelSourceOp::Invalid) {
                TunnelSourceOp::Available(reader, mut avail) => {
                    if buf.remaining() >= avail.len() {
                        buf.put_slice(&avail);
                        self.state = TunnelSourceOp::Empty(reader);
                    } else {
                        let n = avail.split_off(buf.remaining());
                        buf.put_slice(&n);
                        self.state = TunnelSourceOp::Available(reader, avail);
                    }
                    return Poll::Ready(Ok(()));
                }
                TunnelSourceOp::Reading(mut fut) => match Pin::new(&mut fut).poll(cx) {
                    Poll::Pending => {
                        self.state = TunnelSourceOp::Reading(fut);
                        return Poll::Pending;
                    }
                    Poll::Ready(Err(err)) => {
                        return Poll::Ready(Err(err));
                    }
                    Poll::Ready(Ok((avail, reader))) => {
                        self.state = TunnelSourceOp::Available(reader, avail);
                    }
                },
                TunnelSourceOp::Empty(reader) => {
                    let cut_off = self.cut_off;
                    if self.buffer.remaining() < cut_off {
                        self.buffer.reserve(cut_off);
                    }
                    let buffer = self.buffer.split_off(0);
                    let sender = self.sender.clone();
                    let fut = async move {
                        if sender
                            .send(TunnelCommand::Read(buffer.remaining()))
                            .await
                            .is_err()
                        {
                            return Err(io::ErrorKind::BrokenPipe.into());
                        }
                        let bytes = reader.read_bytes_buf(buffer).await?;
                        if bytes.is_empty() {
                            return Err(io::ErrorKind::Unsupported.into());
                        }
                        Ok((bytes, reader))
                    };
                    self.state = TunnelSourceOp::Reading(Box::pin(fut));
                }
                TunnelSourceOp::Invalid => panic!("TunnerSource is invalid"),
            }
        }
    }
}

struct TunnelLayer {
    level: ActiveVerbosity,
    sender: mpsc::Sender<TunnelCommand>,
}

impl TunnelLayer {
    fn new<S>(taker: Taker<S>, client_version: u64) -> (TunnelLayer, TunnelController)
    where
        S: AsyncWrite + Send + Unpin + 'static,
    {
        let (sender, receiver) = mpsc::channel(1000);
        let sender2 = sender.clone();
        let level = ActiveVerbosity::default();
        let level2 = level.clone();
        tokio::spawn(process_tunnel(
            level.clone(),
            client_version,
            taker,
            receiver,
        ));
        (
            TunnelLayer { level, sender },
            TunnelController {
                level: level2,
                sender: sender2,
                can_send_stderr: false,
                client_version,
            },
        )
    }
}

impl<S> Layer<S> for TunnelLayer
where
    for<'lookup> S: Subscriber + LookupSpan<'lookup>,
{
    fn on_new_span(&self, attrs: &span::Attributes<'_>, id: &span::Id, ctx: layer::Context<'_, S>) {
        if let Some(meta) = ctx.metadata(id) {
            eprintln!("Got span {}", meta.name());
            if meta.name() == crate::store::activity::ACTIVITY_NAME {
                eprintln!("Got activity span");
                if let Ok(activity) = attrs.try_into() {
                    eprintln!("!!!!!!!! Singind activity {} {:?}", id.into_u64(), activity);
                    if let Err(err) = self
                        .sender
                        .try_send(TunnelCommand::StartActivity(id.into_u64(), activity))
                    {
                        eprintln!("Activity start was dropped {err}")
                    }
                } else {
                    eprintln!("Activity start was missing fields")
                }
            }
        }
    }

    fn on_event(&self, event: &Event<'_>, ctx: layer::Context<'_, S>) {
        let meta = event.metadata();
        if meta.target() == crate::store::activity::RESULT_TARGET {
            let span_ref = ctx.event_span(event);
            if span_ref.is_none() {
                eprintln!("Activity result with no parent span");
                return;
            }
            let span = span_ref.unwrap();
            eprintln!("Result {}", span.name());
            let activity = if span.name() == crate::store::activity::ACTIVITY_NAME {
                Some(span)
            } else {
                span.scope()
                    .find(|p| p.name() == crate::store::activity::ACTIVITY_NAME)
            };
            if activity.is_none() {
                eprintln!("Activity result with no parent activity");
                return;
            }
            let parent = activity.unwrap();
            if let Ok(result) = ActivityResult::from_event(event, parent.id()) {
                if let Err(err) = self.sender.try_send(TunnelCommand::Result(result)) {
                    eprintln!("Activity result was dropped {err}")
                }
            } else {
                eprintln!("Activity result was missing fields")
            }
        } else if let Some(cmd) = format_event(self.level.clone(), event) {
            if let Err(err) = self.sender.try_send(cmd) {
                eprintln!("Event dropped {err}")
            }
        }
    }

    fn on_close(&self, id: span::Id, ctx: layer::Context<'_, S>) {
        if let Some(meta) = ctx.metadata(&id) {
            if meta.name() == crate::store::activity::ACTIVITY_NAME {
                if let Err(err) = self
                    .sender
                    .try_send(TunnelCommand::StopActivity(id.into_u64()))
                {
                    eprintln!("Activity stop was dropped {err}")
                }
            }
        }
    }
}

#[derive(Debug)]
struct TunnelController {
    level: ActiveVerbosity,
    can_send_stderr: bool,
    client_version: u64,
    sender: mpsc::Sender<TunnelCommand>,
}

impl TunnelController {
    pub fn sender(&self) -> mpsc::Sender<TunnelCommand> {
        self.sender.clone()
    }

    pub fn set_verbosity(&self, level: Verbosity) {
        self.level.set(level);
        get_mut_settings(|settings| {
            if let Some(settings) = settings {
                settings.verbosity = level;
            }
        });
    }

    /// start_work() means that we're starting an operation for which we
    /// want to send out stderr to the client.
    async fn start_work(&mut self) {
        debug!("start_work");
        self.can_send_stderr = true;
        self.sender.send(TunnelCommand::StartWork).await.unwrap();
        /*
               auto state(state_.lock());
               state->canSendStderr = true;

               for (auto & msg : state->pendingMsgs)
                   to(msg);

               state->pendingMsgs.clear();

               to.flush();
        */
    }

    async fn stop_work_err(&mut self, ex: &Error) {
        error!("stop_work_err {}", ex);
        let (s, r) = oneshot::channel();
        let mut buf = Cursor::new(Vec::new());
        if get_protocol_major!(self.client_version) >= 26 {
            ex.write(&mut buf).await.unwrap();
        } else {
            buf.write_string(ex.to_string()).await.unwrap();
            buf.write_u64_le(ex.exit_code()).await.unwrap();
        }
        self.sender
            .send(TunnelCommand::StopWork(Some(buf.into_inner()), s))
            .await
            .unwrap();
        r.await.unwrap();
        self.can_send_stderr = false;
    }

    /// stop_work() means that we're done; stop sending stderr to the
    /// client.
    async fn stop_work(&mut self) {
        debug!("stop_work");
        let (s, r) = oneshot::channel();
        self.sender
            .send(TunnelCommand::StopWork(None, s))
            .await
            .unwrap();
        r.await.unwrap();
        self.can_send_stderr = false;
        /*
               auto state(state_.lock());

               state->canSendStderr = false;

               if (!ex)
                   to << STDERR_LAST;
               else {
                   if (GET_PROTOCOL_MINOR(clientVersion) >= 26) {
                       to << STDERR_ERROR << *ex;
                   } else {
                       to << STDERR_ERROR << ex->what() << ex->status;
                   }
               }
        */
    }
}

#[instrument(skip(source, out, store))]
pub async fn run_server<S, R, W>(
    source: R,
    out: W,
    store: S,
    trusted: TrustedFlag,
    //recursive: RecursiveFlag,
) -> Result<(), Error>
where
    S: DaemonStore + fmt::Debug + Send,
    R: AsyncRead + fmt::Debug + Send + Unpin + 'static,
    W: AsyncWrite + fmt::Debug + Send + Unpin + 'static,
{
    let settings = BuildSettings::default();
    let fut = run_server_raw(source, out, store, trusted);
    fut.with_settings(settings).await
}

pub async fn run_server_raw<S, R, W>(
    mut source: R,
    mut out: W,
    mut store: S,
    trusted: TrustedFlag,
    //recursive: RecursiveFlag,
) -> Result<(), Error>
where
    S: DaemonStore + fmt::Debug + Send,
    R: AsyncRead + fmt::Debug + Send + Unpin + 'static,
    W: AsyncWrite + fmt::Debug + Send + Unpin + 'static,
{
    // Exchange the greeting.
    let magic = source.read_u64_le().await?;
    if magic != WORKER_MAGIC_1 {
        return Err(Error::DaemonProtocolMismatch);
    }
    out.write_u64_le(WORKER_MAGIC_2).await?;
    out.write_u64_le(PROTOCOL_VERSION).await?;
    out.flush().await?;
    let client_version = source.read_u64_le().await?;
    if client_version < 0x10a {
        return Err(Error::DaemonClientVersionTooOld);
    }
    let mut to = TakenStream::new(out);
    let op_count = OpCounter::new();
    let (tunnel_layer, mut tunnel_logger) = TunnelLayer::new(to.taker(), client_version);
    /*
    auto tunnelLogger = new TunnelLogger(to, clientVersion);
    auto prevLogger = nix::logger;
    // FIXME
    if (!recursive)
        logger = tunnelLogger;
    */

    let fut = async {
        if get_protocol_minor!(client_version) >= 14 && source.read_bool().await? {
            // Obsolete CPU affinity.
            source.read_u64_le().await?;
        }
        if get_protocol_minor!(client_version) >= 11 {
            // obsolete reserveSpace
            source.read_u64_le().await?;
        }
        if get_protocol_minor!(client_version) >= 33 {
            to.write_str("nix.rs 1.2.3").await?;
        }
        if get_protocol_minor!(client_version) >= 35 {
            // We and the underlying store both need to trust the client for
            // it to be trusted.
            let temp = if trusted.into() {
                store.is_trusted_client()
            } else {
                Some(TrustedFlag::NotTrusted)
            };
            match temp {
                None => to.write_u64_le(0).await?,
                Some(TrustedFlag::Trusted) => to.write_u64_le(1).await?,
                Some(TrustedFlag::NotTrusted) => to.write_u64_le(2).await?,
            }
        }

        /* Send startup error messages to the client. */
        tunnel_logger.start_work().await;

        let fut = Box::pin(async {
            tunnel_logger.stop_work().await;
            to.flush().await?;

            while let Ok(op) = source.read_enum::<WorkerProtoOp>().await {
                op_count.report_op(op);
                debug!("performing daemon worker op: {}", op);
                let fut = perform_op(
                    &mut tunnel_logger,
                    &mut store,
                    trusted,
                    client_version,
                    &mut source,
                    &mut to,
                    op,
                );
                if let Err(err) = fut.await {
                    /*
                        If we're not in a state where we can send replies, then
                        something went wrong processing the input of the
                        client.  This can happen especially if I/O errors occur
                        during addTextToStore() / importPath().  If that
                        happens, just send the error message and exit.
                    */
                    let error_allowed = tunnel_logger.can_send_stderr;
                    error!("Command error {} {:?}", error_allowed, err);
                    tunnel_logger.stop_work_err(&err).await;
                    if !error_allowed {
                        return Err(err);
                    }
                }
                debug!("Completed op {}", op);

                to.flush().await?;

                assert!(!tunnel_logger.can_send_stderr);
            }
            Ok(()) as Result<(), Error>
        });
        if let Err(err) = fut.await {
            eprintln!("Server error {:?}", err);
            tunnel_logger.stop_work_err(&err).await;
            to.flush().await?;
        }
        Ok(())
    };
    let sub = registry().with(tunnel_layer).with(ParentLayer::new());
    fut.with_subscriber(sub).await
}

async fn read_derived_paths<R>(
    store_dir: &StoreDir,
    mut source: R,
    client_versionn: u64,
) -> Result<Vec<DerivedPath>, Error>
where
    R: AsyncRead + Unpin,
{
    if get_protocol_minor!(client_versionn) >= 30 {
        let ret = source.read_parsed_coll(&store_dir).await?;
        Ok(ret)
    } else {
        let len = source.read_usize().await?;
        let mut ret = Vec::with_capacity(len);
        for _ in 0..len {
            let paths: Vec<StorePathWithOutputs> = source.read_parsed_coll(&store_dir).await?;
            for path in paths {
                ret.push(path.into());
            }
        }
        Ok(ret)
    }
}

#[instrument(skip(logger, store, from, to), fields(client.major=get_protocol_major!(client_version), client.minor=get_protocol_minor!(client_version)))]
async fn perform_op<S, R, W>(
    logger: &mut TunnelController,
    store: &mut S,
    trusted: TrustedFlag,
    client_version: u64,
    mut from: &mut R,
    mut to: W,
    op: WorkerProtoOp,
) -> Result<(), Error>
where
    S: DaemonStore + fmt::Debug + Send,
    R: AsyncRead + fmt::Debug + Send + Unpin + 'static,
    W: AsyncWrite + fmt::Debug + Send + Unpin,
{
    debug!(?op, "Perform op {}", op);
    let store_dir = store.store_dir();
    use WorkerProtoOp::*;
    match op {
        IsValidPath => {
            let path = from.read_parsed(&store_dir).await?;
            logger.start_work().await;
            let result = store.is_valid_path(&path).await?;
            logger.stop_work().await;
            to.write_bool(result).await?;
        }
        QueryValidPaths => {
            let paths = from.read_parsed_coll(&store_dir).await?;
            let mut substitute = SubstituteFlag::NoSubstitute;
            if get_protocol_minor!(client_version) >= 27 {
                substitute = from.read_flag().await?;
            }
            logger.start_work().await;
            if substitute.into() {
                store.substitute_paths(&paths).await?;
            }
            let res = store.query_valid_paths(&paths, substitute).await?;
            logger.stop_work().await;
            to.write_printed_coll(&store_dir, &res).await?;
        }
        // HasSubstitutes => {} // TODO
        // QuerySubstitutablePaths => {} // TODO
        // QueryPathHash => {} // TODO
        // QueryReferences | QueryReferrers | QueryValidDerivers | QueryDerivationOutputs => {} // TODO
        // QueryDerivationOutputNames => {} // TODO
        // QueryDerivationOutputMap => {} // TODO
        // QueryDeriver => {} // TODO
        // QueryPathFromHashPart => {} // TODO
        // AddToStore => {} // TODO
        AddMultipleToStore => {
            trace!("Add multiple");
            let repair = from.read_flag().await?;
            trace!(?repair, "Read repair flag {:?}", repair);
            let mut dont_check_sigs = from.read_bool().await?;
            trace!(dont_check_sigs, "Read dont_check_sigs {}", dont_check_sigs);
            if (!trusted).into() && dont_check_sigs {
                dont_check_sigs = false;
            }
            let check_sigs = (!dont_check_sigs).into();
            trace!("Starting work");
            logger.start_work().await;
            {
                trace!("Framed source");
                let mut source = FramedSource::new(&mut from);
                let res = store
                    .add_multiple_to_store(&mut source, repair, check_sigs)
                    .await;
                debug!("Done with add multiple");
                source.drain().await?;
                debug!("Drained frame source {:?}", res);
                res?
            }
            trace!("Stopping work");
            logger.stop_work().await;
            trace!("Op done");
        }
        // AddTextToStore => {} // TODO
        // ExportPath => {} // TODO
        // ImportPaths => {} // TODO
        BuildPaths => {
            let drv_paths = read_derived_paths(&store_dir, &mut from, client_version).await?;
            let mut build_mode = BuildMode::Normal;
            if get_protocol_minor!(client_version) >= 15 {
                build_mode = from.read_enum().await?;

                /*
                Repairing is not atomic, so disallowed for "untrusted"
                clients.

                FIXME: layer violation in this message: the daemon code (i.e.
                this file) knows whether a client/connection is trusted, but it
                does not know how the client was authenticated. The mechanism
                need not be getting the UID of the other end of a Unix Domain
                Socket.
                 */
                if build_mode == BuildMode::Repair && (!trusted).into() {
                    return Err(Error::RepairNotAllowed);
                }
            }
            logger.start_work().await;
            store.build_paths(&drv_paths, build_mode).await?;
            logger.stop_work().await;
            to.write_u64_le(1).await?;
        }
        // BuildPathsWithResults => {} // TODO
        BuildDerivation => {
            let drv_path: StorePath = from.read_parsed(&store_dir).await?;
            let drv =
                BasicDerivation::read_drv(&mut from, &store_dir, drv_path.name_from_drv()).await?;
            let build_mode = from.read_enum().await?;
            logger.start_work().await;

            let drv_type = drv.drv_type()?;

            /* Content-addressed derivations are trustless because their output paths
            are verified by their content alone, so any derivation is free to
            try to produce such a path.

            Input-addressed derivation output paths, however, are calculated
            from the derivation closure that produced them---even knowing the
            root derivation is not enough. That the output data actually came
            from those derivations is fundamentally unverifiable, but the daemon
            trusts itself on that matter. The question instead is whether the
            submitted plan has rights to the output paths it wants to fill, and
            at least the derivation closure proves that.

            It would have been nice if input-address algorithm merely depended
            on the build time closure, rather than depending on the derivation
            closure. That would mean input-addressed paths used at build time
            would just be trusted and not need their own evidence. This is in
            fact fine as the same guarantees would hold *inductively*: either
            the remote builder has those paths and already trusts them, or it
            needs to build them too and thus their evidence must be provided in
            turn.  The advantage of this variant algorithm is that the evidence
            for input-addressed paths which the remote builder already has
            doesn't need to be sent again.

            That said, now that we have floating CA derivations, it is better
            that people just migrate to those which also solve this problem, and
            others. It's the same migration difficulty with strictly more
            benefit.

            Lastly, do note that when we parse fixed-output content-addressed
            derivations, we throw out the precomputed output paths and just
            store the hashes, so there aren't two competing sources of truth an
            attacker could exploit. */
            if !(drv_type.is_ca() || trusted.into()) {
                return Err(Error::MissingPrivilegesToBuild);
            }

            /* Make sure that the non-input-addressed derivations that got this far
            are in fact content-addressed if we don't trust them. */
            assert!(drv_type.is_ca() || trusted.into());

            /* Recompute the derivation path when we cannot trust the original. */
            /*
            TODO
            if !trusted.into() {
                /* Recomputing the derivation path for input-address derivations
                makes it harder to audit them after the fact, since we need the
                original not-necessarily-resolved derivation to verify the drv
                derivation as adequate claim to the input-addressed output
                paths. */
                assert!(drv_type.is_ca());

                Derivation drv2;
                static_cast<BasicDerivation &>(drv2) = drv;
                drv_path = writeDerivation(*store, Derivation { drv2 });
            }
            */

            let res = store.build_derivation(&drv_path, &drv, build_mode).await?;
            logger.stop_work().await;
            to.write_enum(res.status).await?;
            to.write_string(res.error_msg).await?;
            if get_protocol_minor!(client_version) >= 29 {
                to.write_u64_le(res.times_built).await?;
                to.write_bool(res.is_non_deterministic).await?;
                to.write_time(res.start_time).await?;
                to.write_time(res.stop_time).await?;
            }
            if get_protocol_minor!(client_version) >= 28 {
                let mut built_outputs = DrvOutputs::new();
                for (_, realisation) in res.built_outputs {
                    built_outputs.insert(realisation.id.clone(), realisation);
                }
                to.write_usize(built_outputs.len()).await?;
                for (key, val) in built_outputs {
                    to.write_str(&key.to_string()).await?;
                    to.write_str(&val.to_json_string()?).await?;
                }
            }
        }

        // EnsurePath => {} // TODO
        // AddTempRoot => {} // TODO
        // AddIndirectRoot => {} // TODO
        // Obsolete.
        // SyncWithGC  => {} // TODO
        // FindRoots => {} // TODO
        // CollectGarbage => {} // TODO
        SetOptions => {
            let keep_failed = from.read_bool().await?;
            let keep_going = from.read_bool().await?;
            let try_fallback = from.read_bool().await?;
            let verbosity = from.read_enum().await?;
            let max_build_jobs = from.read_u64_le().await?;
            let max_silent_time = from.read_seconds().await?;
            from.read_u64_le().await?; // obsolete useBuildHook
            let build_verbosity: Verbosity = from.read_enum().await?;
            let verbose_build = build_verbosity == Verbosity::Error;
            from.read_u64_le().await?; // obsolete logType
            from.read_u64_le().await?; // obsolete printBuildTrace
            let build_cores = from.read_u64_le().await?;
            let use_substitutes = from.read_bool().await?;

            let mut unknown = BTreeMap::new();
            if get_protocol_minor!(client_version) >= 12 {
                let len = from.read_usize().await?;
                for _i in 0..len {
                    let name = from.read_string().await?;
                    let value = from.read_string().await?;
                    unknown.insert(name, value);
                }
            }

            logger.start_work().await;
            // if !recursive {
            logger.set_verbosity(verbosity);
            get_mut_settings(move |settings| {
                if let Some(settings) = settings {
                    settings.keep_failed = keep_failed;
                    settings.keep_going = keep_going;
                    settings.try_fallback = try_fallback;
                    settings.max_build_jobs = max_build_jobs;
                    settings.max_silent_time = max_silent_time;
                    settings.verbose_build = verbose_build;
                    settings.build_cores = build_cores;
                    settings.use_substitutes = use_substitutes;
                    return settings.set(unknown.clone());
                }
                Ok(())
            })?;
            // }
            logger.stop_work().await;
        }
        // QuerySubstitutablePathInfo => {} // TODO
        // QuerySubstitutablePathInfos => {} // TODO
        // QueryAllValidPaths => {} // TODO
        QueryPathInfo => {
            let path = from.read_parsed(&store_dir).await?;
            logger.start_work().await;
            if let Some(info) = store.query_path_info(&path).await? {
                logger.stop_work().await;
                if get_protocol_minor!(client_version) >= 17 {
                    to.write_u64_le(1).await?
                }
                info.write(&mut to, &store_dir, client_version, false)
                    .await?;
            } else {
                if get_protocol_minor!(client_version) < 18 {
                    return Err(Error::InvalidPath(store_dir.print_path(&path)));
                }
                logger.stop_work().await;
                assert!(get_protocol_minor!(client_version) >= 17);
                to.write_u64_le(0).await?
            }
        }
        // OptimiseStore => {} // TODO
        // VerifyStore => {} // TODO
        // AddSignatures => {} // TODO
        NarFromPath => {
            let path = from.read_parsed(&store_dir).await?;
            logger.start_work().await;
            logger.stop_work().await;
            store.nar_from_path(&path, &mut to).await?;
        }
        AddToStoreNar => {
            let path = from.read_parsed(&store_dir).await?;
            let deriver = from.read_string().await?;
            let deriver = if !deriver.is_empty() {
                Some(store_dir.parse_path(&deriver)?)
            } else {
                None
            };
            let nar_hash = from.read_string().await?;
            let nar_hash = hash::Hash::parse_any(&nar_hash, Some(hash::Algorithm::SHA256))?;
            let references = from.read_parsed_coll(&store_dir).await?;
            let registration_time = from.read_time().await?;
            let nar_size = from.read_u64_le().await?;
            let mut ultimate = from.read_bool().await?;
            let sigs: Vec<String> = from.read_string_coll().await?;
            let sigs = sigs
                .iter()
                .map(|s| s.parse())
                .collect::<Result<SignatureSet, ParseSignatureError>>()?;
            let ca_s = from.read_string().await?;
            let ca = if !ca_s.is_empty() {
                Some(ca_s.parse()?)
            } else {
                None
            };
            let repair = from.read_flag().await?;
            let mut dont_check_sigs = from.read_bool().await?;
            if (!trusted).into() && dont_check_sigs {
                dont_check_sigs = false;
            }
            if (!trusted).into() {
                ultimate = false;
            }
            let info = ValidPathInfo {
                path,
                deriver,
                nar_size,
                nar_hash,
                references,
                sigs,
                registration_time,
                ultimate,
                ca,
            };
            let check_sigs = if dont_check_sigs {
                CheckSignaturesFlag::NoCheckSigs
            } else {
                CheckSignaturesFlag::CheckSigs
            };

            if get_protocol_minor!(client_version) >= 23 {
                logger.start_work().await;
                {
                    let mut source = FramedSource::new(&mut from);
                    let res = store
                        .add_to_store(&info, &mut source, repair, check_sigs)
                        .await;
                    source.drain().await?;
                    res?
                }
                logger.stop_work().await;
            } else if get_protocol_minor!(client_version) >= 21 {
                let mut source = TunnelSource::with_capacity(&mut from, logger.sender(), 65_000);
                logger.start_work().await;
                // FIXME: race if addToStore doesn't read source?
                store
                    .add_to_store(&info, &mut source, repair, check_sigs)
                    .await?;
                logger.stop_work().await;
            } else {
                /*
                TeeSource tee { from, saved };
                ParseSink ether;
                parseDump(ether, tee);
                source = std::make_unique<StringSource>(saved.s);
                    */
                let mut source = tokio::io::AsyncReadExt::take(&mut from, info.nar_size);
                logger.start_work().await;
                // FIXME: race if addToStore doesn't read source?
                store
                    .add_to_store(&info, &mut source, repair, check_sigs)
                    .await?;
                logger.stop_work().await;
            }
        }
        QueryMissing => {
            let targets = read_derived_paths(&store_dir, &mut from, client_version).await?;
            logger.start_work().await;
            let result = store.query_missing(&targets).await?;
            logger.stop_work().await;
            to.write_printed_coll(&store_dir, &result.will_build)
                .await?;
            to.write_printed_coll(&store_dir, &result.will_substitute)
                .await?;
            to.write_printed_coll(&store_dir, &result.unknown).await?;
            to.write_u64_le(result.download_size).await?;
            to.write_u64_le(result.nar_size).await?;
        }
        // RegisterDrvOutput => {} // TODO
        // QueryRealisation => {} // TODO
        // AddBuildLog => {} // TODO
        QueryFailedPaths | ClearFailedPaths => return Err(Error::RemovedOperation(op)),
        _ => {
            // throw Error("invalid operation %1%", op);
            return Err(Error::InvalidOperation(op));
        }
    }
    Ok(())
}
