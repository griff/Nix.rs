use std::{ops::Deref, path::Path, rc::Rc};

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

    pub async fn connect_io<C, R, W>(
        self,
        reader: R,
        writer: W,
    ) -> capnp::Result<GuardedClient<C, ShutdownDropGuard>>
    where
        C: FromClientHook,
        R: AsyncRead + Unpin + 'static,
        W: AsyncWrite + Unpin + 'static,
    {
        let shutdown = GracefulShutdown::new();
        let mut conn = self.inner.connect(reader, writer);
        let client: C = conn.server_bootstrap();
        let watcher = shutdown.watcher();

        tokio::task::spawn_local(async move {
            if let Err(err) = watcher.watch(conn).await {
                error!("Failed to run RPC system: {err:#}");
            }
        });
        Ok(GuardedClient::new(
            client,
            Rc::new(ShutdownDropGuard {
                actual: Some(shutdown),
            }),
        ))
    }

    pub async fn connect_unix<C, P>(
        self,
        path: P,
    ) -> capnp::Result<GuardedClient<C, ShutdownDropGuard>>
    where
        C: FromClientHook,
        P: AsRef<Path>,
    {
        let stream = UnixStream::connect(path).await?;
        let (reader, writer) = stream.into_split();
        self.connect_io(reader, writer).await
    }

    pub async fn connect_tcp<C, A>(
        self,
        addr: A,
    ) -> capnp::Result<GuardedClient<C, ShutdownDropGuard>>
    where
        C: FromClientHook,
        A: ToSocketAddrs,
    {
        let stream = TcpStream::connect(addr).await?;
        let (reader, writer) = stream.into_split();
        self.connect_io(reader, writer).await
    }
}

pub struct ShutdownDropGuard {
    actual: Option<GracefulShutdown>,
}

impl Drop for ShutdownDropGuard {
    fn drop(&mut self) {
        if let Some(actual) = self.actual.take() {
            eprintln!("Starting rpc shutdown");
            actual.shutdown_background();
        }
    }
}

pub struct GuardedClient<C, I> {
    client: C,
    guard: Rc<I>,
}
impl<C, I> Clone for GuardedClient<C, I>
where
    C: Clone,
{
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            guard: self.guard.clone(),
        }
    }
}

impl<C, I> GuardedClient<C, I>
where
    C: FromClientHook,
    I: 'static,
{
    pub fn new(client: C, guard: Rc<I>) -> Self {
        Self { client, guard }
    }

    pub fn wrap<NC>(self, client: NC) -> GuardedClient<NC, I> {
        GuardedClient {
            client,
            guard: self.guard,
        }
    }

    pub fn inner(&self) -> &C {
        &self.client
    }

    pub fn into_inner(self) -> (C, Rc<I>) {
        (self.client, self.guard)
    }

    pub fn into_client(self) -> C {
        let hook = Box::new(DropGuardHook {
            inner: self.client.into_client_hook(),
            guard: self.guard,
        });
        C::new(hook)
    }
}

impl<C, I> Deref for GuardedClient<C, I> {
    type Target = C;

    fn deref(&self) -> &Self::Target {
        &self.client
    }
}

impl<C, I> AsRef<C> for GuardedClient<C, I> {
    fn as_ref(&self) -> &C {
        &self.client
    }
}

struct DropGuardHook<I> {
    inner: Box<dyn ClientHook>,
    guard: Rc<I>,
}

impl<I: 'static> ClientHook for DropGuardHook<I> {
    fn add_ref(&self) -> Box<dyn ClientHook> {
        Box::new(Self {
            inner: self.inner.add_ref(),
            guard: self.guard.clone(),
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
                guard: self.guard.clone(),
            }) as Box<dyn ClientHook>
        })
    }

    fn when_more_resolved(
        &self,
    ) -> Option<capnp::capability::Promise<Box<dyn ClientHook>, capnp::Error>> {
        let inner = self.inner.when_more_resolved();
        inner.map(|inner| {
            let shutdown = self.guard.clone();
            Promise::from_future(inner.map_ok(|inner| {
                Box::new(Self {
                    inner,
                    guard: shutdown,
                }) as Box<dyn ClientHook>
            }))
        })
    }

    fn when_resolved(&self) -> capnp::capability::Promise<(), capnp::Error> {
        self.inner.when_resolved()
    }
}
