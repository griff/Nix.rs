use std::pin::{Pin, pin};
use std::task;

use capnp::capability::{Client, FromClientHook};
use capnp::message::ReaderOptions;
use capnp_rpc::{Disconnector, RpcSystem, rpc_twoparty_capnp, twoparty};
use futures::AsyncReadExt as _;
use futures::io as fio;
use pin_project_lite::pin_project;
use tokio::io::{AsyncRead, AsyncWrite};
use tracing::{error, info};

pub use crate::graceful::{GracefulConnection, GracefulShutdown, Watcher};

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
    pub fn serve_connection<R, W>(self, reader: R, writer: W) -> ServerConnection
    where
        R: AsyncRead + Unpin + 'static,
        W: AsyncWrite + Unpin + 'static,
    {
        let reader = tokio_util::compat::TokioAsyncReadCompatExt::compat(reader);
        let writer = tokio_util::compat::TokioAsyncWriteCompatExt::compat_write(writer);
        let network = twoparty::VatNetwork::new(
            fio::BufReader::new(reader),
            fio::BufWriter::new(writer),
            rpc_twoparty_capnp::Side::Server,
            self.receiver_options,
        );

        let rpc_system = RpcSystem::new(Box::new(network), self.bootstrap);
        let disconnector = rpc_system.get_disconnector();
        ServerConnection {
            rpc_system: Some(rpc_system),
            disconnector,
        }
    }

    /// Connect to a given io stream as a client
    pub fn connect<R, W>(self, reader: R, writer: W) -> ClientConnection
    where
        R: AsyncRead + Unpin + 'static,
        W: AsyncWrite + Unpin + 'static,
    {
        let reader = tokio_util::compat::TokioAsyncReadCompatExt::compat(reader);
        let writer = tokio_util::compat::TokioAsyncWriteCompatExt::compat_write(writer);
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

impl Clone for RpcSystemBuilder {
    fn clone(&self) -> Self {
        Self {
            bootstrap: self
                .bootstrap
                .as_ref()
                .map(|c| Client::new(c.hook.add_ref())),
            receiver_options: self.receiver_options,
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

    pub async fn with_graceful_shutdown<S>(self, signal: S)
    where
        S: Future<Output = ()>,
    {
        let shutdown = GracefulShutdown::new();
        let watcher = shutdown.watcher();
        let join = tokio::task::spawn_local(async move {
            if let Err(err) = watcher.watch(self).await {
                error!("Failed to run RPC system: {err:#}");
            }
        });
        let mut signal = pin!(signal);
        tokio::select! {
            _ = join => {},
            _ = &mut signal => {
                info!("signal received, shutting down");
            }
        }
        shutdown.shutdown().await
    }
}
impl crate::private::Sealed for ServerConnection {}

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
impl crate::private::Sealed for ClientConnection {}

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
