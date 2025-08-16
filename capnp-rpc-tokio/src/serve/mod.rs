use core::fmt;
use std::{
    convert::Infallible,
    future::{Ready, poll_fn, ready},
    io,
    pin::{Pin, pin},
};

use capnp::capability::{Client, FromClientHook};
use futures::future::LocalBoxFuture;
use tracing::{error, trace};

use crate::builder::{GracefulShutdown, RpcSystemBuilder};

mod listener;
pub use listener::{Listener, ListenerExt};

pub fn serve<L, M>(listener: L, make_service: M) -> Serve<L, M>
where
    L: Listener,
    L::Addr: fmt::Debug,
    M: for<'a> tower_service::Service<IncomingStream<'a, L>, Error = Infallible, Response = Client>
        + Unpin
        + 'static,
{
    Serve {
        listener,
        make_service,
    }
}

#[must_use = "futures must be awaited or polled"]
pub struct Serve<L, M> {
    listener: L,
    make_service: M,
}

impl<L, M> Serve<L, M>
where
    L: Listener,
{
    pub fn local_addr(&self) -> io::Result<L::Addr> {
        self.listener.local_addr()
    }

    pub fn with_graceful_shutdown<S>(self, signal: S) -> WithGracefulShutdown<L, M, S> {
        WithGracefulShutdown {
            listener: self.listener,
            make_service: self.make_service,
            signal,
        }
    }
}

impl<L, M> Serve<L, M>
where
    L: Listener,
    L::Addr: fmt::Debug,
    M: for<'a> tower_service::Service<IncomingStream<'a, L>, Error = Infallible, Response = Client>
        + Unpin
        + 'static,
{
    async fn run(self) -> ! {
        let Self {
            mut listener,
            mut make_service,
        } = self;
        loop {
            let (io, remote_addr) = listener.accept().await;
            let mut mr = Pin::new(&mut make_service);
            poll_fn(move |cx| mr.as_mut().poll_ready(cx))
                .await
                .unwrap_or_else(|err| match err {});
            let client = make_service
                .call(IncomingStream {
                    io: &io,
                    remote_addr,
                })
                .await
                .unwrap_or_else(|err| match err {});
            let server = RpcSystemBuilder::new()
                .bootstrap(client)
                .serve_connection(io);
            tokio::task::spawn_local(server);
        }
    }
}

impl<L, M> IntoFuture for Serve<L, M>
where
    L: Listener,
    L::Addr: fmt::Debug,
    M: for<'a> tower_service::Service<IncomingStream<'a, L>, Error = Infallible, Response = Client>
        + Unpin
        + 'static,
{
    type Output = io::Result<()>;
    type IntoFuture = LocalBoxFuture<'static, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move { self.run().await })
    }
}

#[must_use = "futures must be awaited or polled"]
pub struct WithGracefulShutdown<L, M, S> {
    listener: L,
    make_service: M,
    signal: S,
}

impl<L, M, S> WithGracefulShutdown<L, M, S>
where
    L: Listener,
{
    pub fn local_addr(&self) -> io::Result<L::Addr> {
        self.listener.local_addr()
    }
}

impl<L, M, S> WithGracefulShutdown<L, M, S>
where
    L: Listener,
    L::Addr: fmt::Debug,
    M: for<'a> tower_service::Service<IncomingStream<'a, L>, Error = Infallible, Response = Client>
        + Unpin
        + 'static,
    S: Future<Output = ()> + Send + 'static,
{
    async fn run(self) {
        let Self {
            mut listener,
            mut make_service,
            signal,
        } = self;
        let shutdown = GracefulShutdown::new();
        let mut signal = pin!(signal);

        loop {
            let (io, remote_addr) = tokio::select! {
                conn = listener.accept() => conn,
                _ = &mut signal => {
                    drop(listener);
                    trace!("graceful shutdown signal received, not accepting new connections");
                    break;
                }
            };

            let mut mr = Pin::new(&mut make_service);
            poll_fn(move |cx| mr.as_mut().poll_ready(cx))
                .await
                .unwrap_or_else(|err| match err {});
            let client = make_service
                .call(IncomingStream {
                    io: &io,
                    remote_addr,
                })
                .await
                .unwrap_or_else(|err| match err {});
            let conn = RpcSystemBuilder::new()
                .bootstrap(client)
                .serve_connection(io);
            let watcher = shutdown.watcher();
            tokio::task::spawn_local(async move {
                if let Err(err) = watcher.watch(conn).await {
                    error!("Failed to run RPC system: {err:#}");
                }
            });
        }
        trace!("waiting for {} tasks to finish", shutdown.count());
        shutdown.shutdown().await
    }
}

impl<L, M, S> IntoFuture for WithGracefulShutdown<L, M, S>
where
    L: Listener,
    L::Addr: fmt::Debug,
    M: for<'a> tower_service::Service<IncomingStream<'a, L>, Error = Infallible, Response = Client>
        + Unpin
        + 'static,
    S: Future<Output = ()> + Send + 'static,
{
    type Output = io::Result<()>;
    type IntoFuture = LocalBoxFuture<'static, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            self.run().await;
            Ok(())
        })
    }
}

pub struct IncomingStream<'a, L>
where
    L: Listener,
{
    io: &'a L::Io,
    remote_addr: L::Addr,
}

impl<'a, L> IncomingStream<'a, L>
where
    L: Listener,
{
    pub fn io(&self) -> &L::Io {
        self.io
    }

    pub fn remote_addr(&self) -> &L::Addr {
        &self.remote_addr
    }
}

pub fn make_client<C>(client: C) -> MakeClient<C>
where
    C: FromClientHook + Clone,
{
    MakeClient(client)
}

pub struct MakeClient<C>(C);
impl<'a, C, L> tower_service::Service<IncomingStream<'a, L>> for MakeClient<C>
where
    L: Listener,
    C: FromClientHook + Clone,
{
    type Response = Client;
    type Error = Infallible;
    type Future = Ready<Result<Client, Infallible>>;

    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn call(&mut self, _req: IncomingStream<'a, L>) -> Self::Future {
        ready(Ok(self.0.clone().cast_to()))
    }
}
