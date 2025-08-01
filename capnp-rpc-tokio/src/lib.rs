use std::fmt;
use std::pin::Pin;
use std::task;

use capnp::capability::{Client, FromClientHook};
use capnp::message::ReaderOptions;
use capnp_rpc::{rpc_twoparty_capnp, twoparty, Disconnector, RpcSystem};
use futures::io as fio;
use futures::AsyncReadExt as _;
use pin_project_lite::pin_project;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::watch,
};

mod private {
    pub trait Sealed {}
}

pub trait RpcSystemExt {
    fn builder() -> RpcSystemBuilder;
}

impl RpcSystemExt for RpcSystem<rpc_twoparty_capnp::Side> {
    fn builder() -> RpcSystemBuilder {
        RpcSystemBuilder::new()
    }
}

#[derive(Default)]
pub struct RpcSystemBuilder {
    bootstrap: Option<Client>,
    receiver_options: ReaderOptions,
}

impl RpcSystemBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn bootstrap<C>(mut self, client: C) -> Self
    where
        C: FromClientHook,
    {
        let hook = client.into_client_hook();
        self.bootstrap = Some(Client::new(hook));
        self
    }

    pub fn clear_bootstrap(mut self) -> Self {
        self.bootstrap = None;
        self
    }

    pub fn receiver_options(mut self, options: ReaderOptions) -> Self {
        self.receiver_options = options;
        self
    }

    /// Serve a given io stream as a server connection
    pub fn serve_connection<IO>(self, io: IO) -> ServerConnection
    where
        IO: AsyncRead + AsyncWrite + Unpin + 'static,
    {
        let (reader, writer) = tokio_util::compat::TokioAsyncReadCompatExt::compat(io).split();
        let network = twoparty::VatNetwork::new(
            fio::BufReader::new(reader),
            fio::BufWriter::new(writer),
            rpc_twoparty_capnp::Side::Server,
            self.receiver_options,
        );

        let rpc_system = RpcSystem::new(Box::new(network), self.bootstrap);
        let disconnector = rpc_system.get_disconnector();
        ServerConnection {
            rpc_system,
            disconnector,
        }
    }

    /// Connect to a given io stream as a client
    pub fn connect<IO>(self, io: IO) -> ClientConnection
    where
        IO: AsyncRead + AsyncWrite + Unpin + 'static,
    {
        let (reader, writer) = tokio_util::compat::TokioAsyncReadCompatExt::compat(io).split();
        let network = twoparty::VatNetwork::new(
            fio::BufReader::new(reader),
            fio::BufWriter::new(writer),
            rpc_twoparty_capnp::Side::Client,
            self.receiver_options,
        );

        let rpc_system = RpcSystem::new(Box::new(network), self.bootstrap);
        let disconnector = rpc_system.get_disconnector();
        ClientConnection {
            rpc_system,
            disconnector,
        }
    }
}

pin_project! {
    pub struct ServerConnection {
        #[pin]
        rpc_system: RpcSystem<rpc_twoparty_capnp::Side>,
        #[pin]
        disconnector: Disconnector<rpc_twoparty_capnp::Side>,
    }
}

impl ServerConnection {
    /// Return the client bootstrap interface
    pub fn client_bootstrap<T: FromClientHook>(&mut self) -> T {
        self.rpc_system.bootstrap(rpc_twoparty_capnp::Side::Client)
    }
}
impl private::Sealed for ServerConnection {}

impl GracefulConnection for ServerConnection {
    fn poll_graceful_shutdown(
        self: Pin<&mut Self>,
        cx: &mut task::Context,
    ) -> task::Poll<capnp::Result<()>> {
        self.project().disconnector.poll(cx)
    }
}

impl Future for ServerConnection {
    type Output = Result<(), capnp::Error>;
    fn poll(self: Pin<&mut Self>, cx: &mut task::Context) -> task::Poll<Self::Output> {
        self.project().rpc_system.poll(cx)
    }
}

pin_project! {
    pub struct ClientConnection {
        #[pin]
        rpc_system: RpcSystem<rpc_twoparty_capnp::Side>,
        #[pin]
        disconnector: Disconnector<rpc_twoparty_capnp::Side>,
    }
}

impl ClientConnection {
    /// Return the server bootstrap interface
    pub fn server_bootstrap<T: FromClientHook>(&mut self) -> T {
        self.rpc_system.bootstrap(rpc_twoparty_capnp::Side::Server)
    }
}
impl private::Sealed for ClientConnection {}

impl GracefulConnection for ClientConnection {
    fn poll_graceful_shutdown(
        self: Pin<&mut Self>,
        cx: &mut task::Context,
    ) -> task::Poll<capnp::Result<()>> {
        self.project().disconnector.poll(cx)
    }
}

impl Future for ClientConnection {
    type Output = Result<(), capnp::Error>;
    fn poll(self: Pin<&mut Self>, cx: &mut task::Context) -> task::Poll<Self::Output> {
        self.project().rpc_system.poll(cx)
    }
}

pub trait GracefulConnection: Future<Output = capnp::Result<()>> + private::Sealed {
    fn poll_graceful_shutdown(
        self: Pin<&mut Self>,
        cx: &mut task::Context,
    ) -> task::Poll<capnp::Result<()>>;
}

pub struct GracefulShutdown {
    tx: watch::Sender<()>,
}

pub struct Watcher {
    rx: watch::Receiver<()>,
}

impl GracefulShutdown {
    /// Create a new graceful shutdown helper.
    pub fn new() -> Self {
        let (tx, _) = watch::channel(());
        Self { tx }
    }

    pub fn watch<C: GracefulConnection>(&self, conn: C) -> impl Future<Output = capnp::Result<()>> {
        self.watcher().watch(conn)
    }
    pub fn watcher(&self) -> Watcher {
        let rx = self.tx.subscribe();
        Watcher { rx }
    }
    pub async fn shutdown(self) {
        let Self { tx } = self;
        let _ = tx.send(());
        tx.closed().await;
    }

    pub fn count(&self) -> usize {
        self.tx.receiver_count()
    }
}

impl Default for GracefulShutdown {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for GracefulShutdown {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GracefulShutdown").finish()
    }
}

impl Watcher {
    pub fn watch<C: GracefulConnection>(self, conn: C) -> impl Future<Output = capnp::Result<()>> {
        let Self { mut rx } = self;
        GracefulConnectionFuture::new(conn, async move {
            let _ = rx.changed().await;
            // hold onto the rx until the watched future is completed
            rx
        })
    }
}

pin_project! {
    struct GracefulConnectionFuture<C: GracefulConnection, F: Future> {
        #[pin]
        conn: C,
        #[pin]
        cancel: F,
        // If cancelled, this is held until the inner conn is done.
        cancelled_guard: Option<F::Output>,
    }
}

impl<C: GracefulConnection, F: Future> GracefulConnectionFuture<C, F> {
    fn new(conn: C, cancel: F) -> Self {
        Self {
            conn,
            cancel,
            cancelled_guard: None,
        }
    }
}

impl<C: GracefulConnection, F: Future> fmt::Debug for GracefulConnectionFuture<C, F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GracefulConnectionFuture").finish()
    }
}

impl<C, F> Future for GracefulConnectionFuture<C, F>
where
    C: GracefulConnection,
    F: Future,
{
    type Output = capnp::Result<()>;

    fn poll(self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> task::Poll<Self::Output> {
        let mut this = self.project();
        if this.cancelled_guard.is_none() {
            if let task::Poll::Ready(guard) = this.cancel.poll(cx) {
                *this.cancelled_guard = Some(guard);
            }
        }
        if this.cancelled_guard.is_some() {
            match (
                this.conn.as_mut().poll(cx)?,
                this.conn.as_mut().poll_graceful_shutdown(cx)?,
            ) {
                (task::Poll::Ready(_), task::Poll::Ready(_)) => task::Poll::Ready(Ok(())),
                _ => task::Poll::Pending,
            }
        } else {
            this.conn.poll(cx)
        }
    }
}
