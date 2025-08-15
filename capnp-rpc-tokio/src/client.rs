use std::{path::Path, rc::Rc};

use capnp::{
    capability::{FromClientHook, Promise},
    message::ReaderOptions,
    private::capability::ClientHook,
};
use futures::TryFutureExt;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::{TcpStream, ToSocketAddrs, UnixStream},
};
use tracing::error;

use crate::builder::{GracefulShutdown, RpcSystemBuilder};

#[derive(Default)]
pub struct ClientBuilder {
    inner: RpcSystemBuilder,
}

impl ClientBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn bootstrap<C>(mut self, client: C) -> Self
    where
        C: FromClientHook,
    {
        self.inner = self.inner.bootstrap(client);
        self
    }

    pub fn clear_bootstrap(mut self) -> Self {
        self.inner = self.inner.clear_bootstrap();
        self
    }

    pub fn receiver_options(mut self, options: ReaderOptions) -> Self {
        self.inner = self.inner.receiver_options(options);
        self
    }

    pub async fn connect_io<C, IO>(self, io: IO) -> capnp::Result<C>
    where
        C: FromClientHook,
        IO: AsyncRead + AsyncWrite + Unpin + 'static,
    {
        let shutdown = GracefulShutdown::new();
        let mut conn = self.inner.connect(io);
        let client: capnp::capability::Client = conn.server_bootstrap();
        let watcher = shutdown.watcher();

        tokio::task::spawn_local(async move {
            if let Err(err) = watcher.watch(conn).await {
                error!("Failed to run RPC system: {err:#}");
            }
        });
        let hook = Box::new(ShutdownHook {
            inner: client.hook,
            shutdown: Rc::new(ShutdownInner {
                actual: Some(shutdown),
            }),
        });
        Ok(C::new(hook))
    }

    pub async fn connect_unix<C, P>(self, path: P) -> capnp::Result<C>
    where
        C: FromClientHook,
        P: AsRef<Path>,
    {
        let stream = UnixStream::connect(path).await?;
        self.connect_io(stream).await
    }

    pub async fn connect_tcp<C, A>(self, addr: A) -> capnp::Result<C>
    where
        C: FromClientHook,
        A: ToSocketAddrs,
    {
        let stream = TcpStream::connect(addr).await?;
        self.connect_io(stream).await
    }
}

struct ShutdownInner {
    actual: Option<GracefulShutdown>,
}

impl Drop for ShutdownInner {
    fn drop(&mut self) {
        if let Some(actual) = self.actual.take() {
            actual.shutdown_background();
        }
    }
}

struct ShutdownHook {
    inner: Box<dyn ClientHook>,
    shutdown: Rc<ShutdownInner>,
}

impl ClientHook for ShutdownHook {
    fn add_ref(&self) -> Box<dyn ClientHook> {
        Box::new(Self {
            inner: self.inner.add_ref(),
            shutdown: self.shutdown.clone(),
        })
    }

    fn new_call(
        &self,
        interface_id: u64,
        method_id: u16,
        size_hint: Option<capnp::MessageSize>,
    ) -> capnp::capability::Request<capnp::any_pointer::Owned, capnp::any_pointer::Owned> {
        self.inner.new_call(interface_id, method_id, size_hint)
    }

    fn call(
        &self,
        interface_id: u64,
        method_id: u16,
        params: Box<dyn capnp::private::capability::ParamsHook>,
        results: Box<dyn capnp::private::capability::ResultsHook>,
    ) -> capnp::capability::Promise<(), capnp::Error> {
        self.inner.call(interface_id, method_id, params, results)
    }

    fn get_brand(&self) -> usize {
        self.inner.get_brand()
    }

    fn get_ptr(&self) -> usize {
        self.inner.get_ptr()
    }

    fn get_resolved(&self) -> Option<Box<dyn ClientHook>> {
        let inner = self.inner.get_resolved();
        inner.map(|inner| {
            Box::new(Self {
                inner,
                shutdown: self.shutdown.clone(),
            }) as Box<dyn ClientHook>
        })
    }

    fn when_more_resolved(
        &self,
    ) -> Option<capnp::capability::Promise<Box<dyn ClientHook>, capnp::Error>> {
        let inner = self.inner.when_more_resolved();
        inner.map(|inner| {
            let shutdown = self.shutdown.clone();
            Promise::from_future(
                inner.map_ok(|inner| Box::new(Self { inner, shutdown }) as Box<dyn ClientHook>),
            )
        })
    }

    fn when_resolved(&self) -> capnp::capability::Promise<(), capnp::Error> {
        self.inner.when_resolved()
    }
}
