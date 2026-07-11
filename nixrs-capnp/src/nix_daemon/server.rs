use std::cell::{Cell, RefCell};
use std::future::{Future, poll_fn, ready};
use std::pin::{Pin, pin};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, ready};

use capnp::Error;
use capnp::capability::Rc;
use capnp_convert::{BuildFrom as _, ReadInto as _};
use capnp_rpc::new_client;
use capnp_rpc_tokio::stream::{ByteStreamWrap, ByteStreamWriter};
use futures::channel::{mpsc, oneshot};
use futures::stream::StreamExt;
use futures::{SinkExt as _, Stream, TryFutureExt};
use nixrs::daemon::{AddToStoreItem, DaemonResult, DaemonStore, HandshakeDaemonStore, ResultLog};
use nixrs::derived_path::DerivedPath;
use nixrs::store_path::{HasStoreDir, StoreDir, StorePathHash};
use pin_project_lite::pin_project;
use tokio::io::{AsyncWriteExt as _, BufReader, ReadHalf, SimplexStream, copy_buf, simplex};
use tracing::{debug, trace};

use crate::capnp::nix_daemon_capnp::{
    add_multiple_stream, has_store_dir, logged_nix_daemon, logger, nix_daemon,
};
use crate::{DEFAULT_BUF_SIZE, from_error};

pub struct NoopLogger;

#[forbid(clippy::missing_trait_methods)]
impl logger::Server for NoopLogger {
    async fn write(self: Rc<Self>, _: logger::WriteParams) -> capnp::Result<()> {
        Ok(())
    }

    async fn end(self: Rc<Self>, _: logger::EndParams, _: logger::EndResults) -> capnp::Result<()> {
        Ok(())
    }
}

#[derive(Clone)]
pub struct Logger {
    client: logger::Client,
}

impl Logger {
    pub async fn process_logs<R>(
        &self,
        logs: impl ResultLog<Output = DaemonResult<R>>,
    ) -> Result<R, Error> {
        let mut logs = pin!(logs);
        while let Some(msg) = logs.next().await {
            debug!("Sending log msg {msg:#?}");
            let mut req = self.client.write_request();
            let params = req.get();
            params.init_event().build_from(&msg)?;
            req.send().await?;
        }
        logs.await.map_err(from_error)
    }

    pub fn end(&self) -> impl Future<Output = Result<(), Error>> {
        self.client.end_request().send().promise.map_ok(|_| ())
    }
}

#[derive(Clone)]
pub struct CapnpServer<S> {
    logger: Logger,
    store: S,
    shutdown: bool,
}

impl<S> CapnpServer<S> {
    pub fn without_logger(store: S) -> Self {
        Self {
            logger: Logger {
                client: new_client(NoopLogger),
            },
            shutdown: false,
            store,
        }
    }
}

#[forbid(clippy::missing_trait_methods)]
impl<S> has_store_dir::Server for CapnpServer<S>
where
    S: HasStoreDir + Clone + 'static,
{
    fn store_dir(
        self: Rc<Self>,
        _: has_store_dir::StoreDirParams,
        mut result: has_store_dir::StoreDirResults,
    ) -> impl Future<Output = capnp::Result<()>> {
        let dir = self.store.store_dir();
        result.get().set_store_dir(dir.to_str());
        ready(Ok(()))
    }
}

#[forbid(clippy::missing_trait_methods)]
impl<S> nix_daemon::Server for CapnpServer<S>
where
    S: DaemonStore + Clone + 'static,
{
    async fn end(
        self: Rc<Self>,
        _: nix_daemon::EndParams,
        _: nix_daemon::EndResults,
    ) -> capnp::Result<()> {
        if self.shutdown {
            let mut store = self.store.clone();
            self.logger.process_logs(store.shutdown()).await?;
        }
        self.logger.end().await?;
        Ok(())
    }

    async fn set_options(
        self: Rc<Self>,
        params: nix_daemon::SetOptionsParams,
        _: nix_daemon::SetOptionsResults,
    ) -> capnp::Result<()> {
        let mut store = self.store.clone();
        let options = params.get()?.get_options()?.read_into()?;
        self.logger.process_logs(store.set_options(&options)).await
    }

    async fn is_valid_path(
        self: Rc<Self>,
        params: nix_daemon::IsValidPathParams,
        mut result: nix_daemon::IsValidPathResults,
    ) -> capnp::Result<()> {
        let mut store = self.store.clone();
        debug!("is_valid_path");
        let path = params.get()?.get_path()?.read_into()?;
        debug!("is_valid_path {path}");
        let valid = self.logger.process_logs(store.is_valid_path(&path)).await?;
        debug!("is_valid_path {path} result {valid}");
        result.get().set_valid(valid);
        Ok(())
    }

    async fn query_valid_paths(
        self: Rc<Self>,
        params: nix_daemon::QueryValidPathsParams,
        mut result: nix_daemon::QueryValidPathsResults,
    ) -> capnp::Result<()> {
        let mut store = self.store.clone();
        let p = params.get()?;
        let paths = p.get_paths()?.read_into()?;
        let substitute = p.get_substitute();
        let valid = self
            .logger
            .process_logs(store.query_valid_paths(&paths, substitute))
            .await?;
        result
            .get()
            .init_valid_set(valid.len() as u32)
            .build_from(&valid)?;
        Ok(())
    }

    async fn query_path_info(
        self: Rc<Self>,
        params: nix_daemon::QueryPathInfoParams,
        mut result: nix_daemon::QueryPathInfoResults,
    ) -> capnp::Result<()> {
        let mut store = self.store.clone();
        let path = params.get()?.get_path()?.read_into()?;
        if let Some(info) = self
            .logger
            .process_logs(store.query_path_info(&path))
            .await?
        {
            result.get().init_info().build_from(&info)?;
        }
        Ok(())
    }

    async fn nar_from_path(
        self: Rc<Self>,
        params: nix_daemon::NarFromPathParams,
        _: nix_daemon::NarFromPathResults,
    ) -> capnp::Result<()> {
        let mut store = self.store.clone();
        let p = params.get()?;
        let path = p.get_path()?.read_into()?;
        let stream = p.get_stream()?;
        let writer = ByteStreamWriter::new(stream);
        let reader = self.logger.process_logs(store.nar_from_path(&path)).await?;
        let mut writer = pin!(writer);
        let mut reader = pin!(reader);
        copy_buf(&mut reader, &mut writer).await?;
        writer.shutdown().await?;
        Ok(())
    }

    async fn build_paths(
        self: Rc<Self>,
        params: nix_daemon::BuildPathsParams,
        _: nix_daemon::BuildPathsResults,
    ) -> capnp::Result<()> {
        let mut store = self.store.clone();
        let p = params.get()?;
        let drvs: Vec<DerivedPath> = p.get_drvs()?.read_into()?;
        let mode = p.get_mode()?.into();
        self.logger
            .process_logs(store.build_paths(&drvs, mode))
            .await?;
        Ok(())
    }

    async fn build_paths_with_results(
        self: Rc<Self>,
        params: nix_daemon::BuildPathsWithResultsParams,
        mut result: nix_daemon::BuildPathsWithResultsResults,
    ) -> capnp::Result<()> {
        let mut store = self.store.clone();
        let p = params.get()?;
        let drvs: Vec<DerivedPath> = p.get_drvs()?.read_into()?;
        let mode = p.get_mode()?.into();
        let results = self
            .logger
            .process_logs(store.build_paths_with_results(&drvs, mode))
            .await?;
        result
            .get()
            .init_results(results.len() as u32)
            .build_from(&results)?;
        Ok(())
    }

    async fn build_derivation(
        self: Rc<Self>,
        params: nix_daemon::BuildDerivationParams,
        mut result: nix_daemon::BuildDerivationResults,
    ) -> capnp::Result<()> {
        let mut store = self.store.clone();
        let p = params.get()?;
        let drv = p.get_drv()?.read_into()?;
        let mode = p.get_mode()?.into();
        let build_result = self
            .logger
            .process_logs(store.build_derivation(&drv, mode))
            .await?;
        result.get().init_result().build_from(&build_result)?;
        Ok(())
    }

    async fn query_missing(
        self: Rc<Self>,
        params: nix_daemon::QueryMissingParams,
        mut result: nix_daemon::QueryMissingResults,
    ) -> capnp::Result<()> {
        let mut store = self.store.clone();
        let p = params.get()?;
        let paths: Vec<DerivedPath> = p.get_paths()?.read_into()?;
        let missing = self
            .logger
            .process_logs(store.query_missing(&paths))
            .await?;
        result.get().init_missing().build_from(&missing)?;
        Ok(())
    }

    async fn add_to_store_nar(
        self: Rc<Self>,
        params: nix_daemon::AddToStoreNarParams,
        mut result: nix_daemon::AddToStoreNarResults,
    ) -> capnp::Result<()> {
        let mut store = self.store.clone();
        let p = params.get()?;
        let info = p.get_info()?.read_into()?;
        let repair = p.get_repair();
        let dont_check_sigs = p.get_dont_check_sigs();
        let (reader, writer) = simplex(DEFAULT_BUF_SIZE);
        let source = BufReader::new(reader);
        let wrap = ByteStreamWrap::new(writer);
        let bs_client = new_client(wrap);
        result.get().set_stream(bs_client);
        result.set_pipeline()?;
        eprintln!("add_to_store_nar set_pipeline");
        self.logger
            .process_logs(store.add_to_store_nar(&info, source, repair, dont_check_sigs))
            .await?;
        eprintln!("add_to_store_nar Done");
        Ok(())
    }

    async fn add_multiple_to_store(
        self: Rc<Self>,
        params: nix_daemon::AddMultipleToStoreParams,
        mut result: nix_daemon::AddMultipleToStoreResults,
    ) -> capnp::Result<()> {
        let mut store = self.store.clone();
        let p = params.get()?;
        let repair = p.get_repair();
        let dont_check_sigs = p.get_dont_check_sigs();
        let remaining = p.get_count();
        trace!(count = remaining, "add_multiple_to_store Processing stream");
        let (mut sender, stream) = mpsc::channel(2);
        if remaining == 0 {
            sender.close_channel();
        }
        let sender = AddMultipleStreamServer {
            remaining: Cell::new(remaining),
            sender,
        };
        let add_stream = new_client(sender);
        result.get().set_stream(add_stream);
        result.set_pipeline()?;
        trace!(count = remaining, "add_multiple_to_store set_pipeline");
        let stream = CountedStream { remaining, stream };
        self.logger
            .process_logs(store.add_multiple_to_store(repair, dont_check_sigs, stream))
            .await?;
        trace!(count = remaining, "add_multiple_to_store Done");
        Ok(())
    }

    async fn query_all_valid_paths(
        self: Rc<Self>,
        _params: nix_daemon::QueryAllValidPathsParams,
        mut result: nix_daemon::QueryAllValidPathsResults,
    ) -> capnp::Result<()> {
        let mut store = self.store.clone();
        let paths = self
            .logger
            .process_logs(store.query_all_valid_paths())
            .await?;
        result
            .get()
            .init_paths(paths.len() as u32)
            .build_from(&paths)?;
        Ok(())
    }

    async fn query_path_from_hash_part(
        self: Rc<Self>,
        params: nix_daemon::QueryPathFromHashPartParams,
        mut result: nix_daemon::QueryPathFromHashPartResults,
    ) -> capnp::Result<()> {
        let mut store = self.store.clone();
        let p = params.get()?;
        let hash =
            StorePathHash::try_from(p.get_hash()?).map_err(|err| Error::failed(err.to_string()))?;
        let res = self
            .logger
            .process_logs(store.query_path_from_hash_part(&hash))
            .await?;
        if let Some(path) = res {
            result.get().init_path().build_from(&path)?;
        }
        Ok(())
    }

    async fn add_temp_root(
        self: Rc<Self>,
        params: nix_daemon::AddTempRootParams,
        _result: nix_daemon::AddTempRootResults,
    ) -> capnp::Result<()> {
        let mut store = self.store.clone();
        let p = params.get()?;
        let path = p.get_path()?.read_into()?;
        self.logger.process_logs(store.add_temp_root(&path)).await
    }

    async fn add_indirect_root(
        self: Rc<Self>,
        params: nix_daemon::AddIndirectRootParams,
        _result: nix_daemon::AddIndirectRootResults,
    ) -> capnp::Result<()> {
        let mut store = self.store.clone();
        let p = params.get()?;
        let path = p.get_path()?.read_into()?;
        self.logger
            .process_logs(store.add_indirect_root(&path))
            .await
    }

    async fn add_perm_root(
        self: Rc<Self>,
        params: nix_daemon::AddPermRootParams,
        mut result: nix_daemon::AddPermRootResults,
    ) -> capnp::Result<()> {
        let mut store = self.store.clone();
        let p = params.get()?;
        let path = p.get_path()?.read_into()?;
        let gc_root = p.get_gc_root()?.read_into()?;
        let res = self
            .logger
            .process_logs(store.add_perm_root(&path, &gc_root))
            .await?;
        result.get().set_path(&res);
        Ok(())
    }
}

struct AddMultipleStreamServer {
    remaining: Cell<u16>,
    sender: mpsc::Sender<DaemonResult<AddToStoreItem<BufReader<ReadHalf<SimplexStream>>>>>,
}

#[forbid(clippy::missing_trait_methods)]
impl add_multiple_stream::Server for AddMultipleStreamServer {
    async fn add(
        self: Rc<Self>,
        params: add_multiple_stream::AddParams,
        mut result: add_multiple_stream::AddResults,
    ) -> capnp::Result<()> {
        if self.remaining.get() == 0 {
            return Err(capnp::Error::failed(
                "Sending more items than specified in addMultipleToStore call".into(),
            ));
        }
        self.remaining.update(|old| old - 1);
        let mut sender = self.sender.clone();
        let p = params.get()?;
        let info = p.get_info()?.read_into()?;
        let (reader, writer) = simplex(DEFAULT_BUF_SIZE);
        let reader = BufReader::new(reader);
        let wrap = ByteStreamWrap::new(writer);
        let bs_client = new_client(wrap);
        result.get().set_stream(bs_client);
        result.set_pipeline()?;
        sender
            .send(Ok(AddToStoreItem { info, reader }))
            .await
            .map_err(from_error)?;
        trace!(remaining = self.remaining.get(), "returning from add");
        if self.remaining.get() == 0 {
            sender.close_channel();
        }
        Ok(())
    }
}

pin_project! {
    struct CountedStream {
        remaining: u16,
        #[pin]
        stream: mpsc::Receiver<DaemonResult<AddToStoreItem<BufReader<ReadHalf<SimplexStream>>>>>,
    }
}

impl Stream for CountedStream {
    type Item = DaemonResult<AddToStoreItem<BufReader<ReadHalf<SimplexStream>>>>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let this = self.project();
        trace!(remaining = *this.remaining, "CountedStream: poll_next");
        if let Some(result) = ready!(this.stream.poll_next(cx)) {
            trace!(
                remaining = *this.remaining,
                "CountedStream: Returning result"
            );
            *this.remaining -= 1;
            Poll::Ready(Some(result))
        } else {
            trace!(remaining = *this.remaining, "CountedStream: Done");
            Poll::Ready(None)
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining as usize, Some(self.remaining as usize))
    }
}

enum Inner<HS, S> {
    Handshake(HS),
    Progress(Pin<Box<dyn Future<Output = ::capnp::Result<S>>>>),
    Store(S),
    Invalid,
}

impl<HS, S> Inner<HS, S>
where
    HS: HandshakeDaemonStore<Store = S> + 'static,
    S: DaemonStore + Clone + 'static,
{
    fn poll_handshake(
        &mut self,
        cx: &mut Context<'_>,
        logger: logger::Client,
    ) -> Poll<::capnp::Result<S>> {
        loop {
            match std::mem::replace(self, Self::Invalid) {
                Inner::Handshake(hs) => {
                    let logger = logger.clone();
                    let fut = Box::pin(async move {
                        let logger = Logger { client: logger };
                        let logs = hs.handshake();
                        debug!("Handshake");
                        logger.process_logs(logs).await
                    });
                    *self = Inner::Progress(fut);
                }
                Inner::Progress(mut fut) => match fut.as_mut().poll(cx) {
                    Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
                    Poll::Ready(Ok(store)) => {
                        *self = Inner::Store(store);
                    }
                    Poll::Pending => {
                        *self = Inner::Progress(fut);
                        return Poll::Pending;
                    }
                },
                Inner::Store(store) => {
                    let ret = store.clone();
                    *self = Inner::Store(store);
                    return Poll::Ready(Ok(ret));
                }
                Inner::Invalid => panic!("Invalid inner for HandshakeDaemonStore"),
            }
        }
    }
}

#[derive(Clone)]
pub struct HandshakeLoggedCapnpServer<HS, S> {
    store_dir: StoreDir,
    inner: Arc<Mutex<Inner<HS, S>>>,
}

impl<HS, S> HandshakeLoggedCapnpServer<HS, S>
where
    HS: HandshakeDaemonStore<Store = S>,
    S: DaemonStore + Clone + 'static,
{
    pub fn new(store: HS) -> Self {
        Self {
            store_dir: store.store_dir().clone(),
            inner: Arc::new(Mutex::new(Inner::Handshake(store))),
        }
    }
}

#[forbid(clippy::missing_trait_methods)]
impl<HS, S> has_store_dir::Server for HandshakeLoggedCapnpServer<HS, S>
where
    HS: HasStoreDir + 'static,
    S: 'static,
{
    async fn store_dir(
        self: Rc<Self>,
        _: has_store_dir::StoreDirParams,
        mut result: has_store_dir::StoreDirResults,
    ) -> capnp::Result<()> {
        result.get().set_store_dir(self.store_dir.to_str());
        Ok(())
    }
}

#[forbid(clippy::missing_trait_methods)]
impl<HS, S> logged_nix_daemon::Server for HandshakeLoggedCapnpServer<HS, S>
where
    HS: HandshakeDaemonStore<Store = S> + 'static,
    S: DaemonStore + Clone + 'static,
{
    async fn capture_logs(
        self: Rc<Self>,
        params: logged_nix_daemon::CaptureLogsParams,
        mut result: logged_nix_daemon::CaptureLogsResults,
    ) -> capnp::Result<()> {
        let inner = self.inner.clone();
        let logger = if params.get()?.has_logger() {
            params.get()?.get_logger()?
        } else {
            new_client(NoopLogger)
        };
        let captures = Captures {
            client: logger,
            sender: RefCell::new(None),
        };
        let client: logger::Client = new_client(captures);
        let store = poll_fn(|cx| {
            let logger = client.clone();
            let mut guard = inner.lock().unwrap();
            guard.poll_handshake(cx, logger)
        })
        .await?;

        let server = CapnpServer {
            logger: Logger { client },
            store,
            shutdown: false,
        };
        result.get().set_daemon(new_client(server));
        Ok(())
    }
}

#[derive(Clone)]
pub struct LoggedCapnpServer<S> {
    store: S,
}

impl<S> LoggedCapnpServer<S> {
    pub fn new(store: S) -> Self {
        Self { store }
    }
}

struct Captures {
    client: logger::Client,
    sender: RefCell<Option<oneshot::Sender<()>>>,
}

#[forbid(clippy::missing_trait_methods)]
impl logger::Server for Captures {
    async fn write(self: Rc<Self>, params: logger::WriteParams) -> capnp::Result<()> {
        let client = self.client.clone();
        let mut req = client.write_request();
        req.get().set_event(params.get()?.get_event()?)?;
        req.send().await
    }

    async fn end(self: Rc<Self>, _: logger::EndParams, _: logger::EndResults) -> capnp::Result<()> {
        let client = self.client.clone();
        let sender = self.sender.take();
        client.end_request().send().promise.await?;
        if let Some(sender) = sender {
            let _ = sender.send(());
        }
        Ok(())
    }
}

#[forbid(clippy::missing_trait_methods)]
impl<S> has_store_dir::Server for LoggedCapnpServer<S>
where
    S: HasStoreDir + 'static,
{
    async fn store_dir(
        self: Rc<Self>,
        _: has_store_dir::StoreDirParams,
        mut result: has_store_dir::StoreDirResults,
    ) -> capnp::Result<()> {
        result.get().set_store_dir(self.store.store_dir().to_str());
        Ok(())
    }
}

#[forbid(clippy::missing_trait_methods)]
impl<S> logged_nix_daemon::Server for LoggedCapnpServer<S>
where
    S: DaemonStore + Clone + 'static,
{
    async fn capture_logs(
        self: Rc<Self>,
        params: logged_nix_daemon::CaptureLogsParams,
        mut result: logged_nix_daemon::CaptureLogsResults,
    ) -> capnp::Result<()> {
        let store = self.store.clone();
        let client = if params.get()?.has_logger() {
            params.get()?.get_logger()?
        } else {
            new_client(NoopLogger)
        };
        let captures = Captures {
            client,
            sender: RefCell::new(None),
        };
        let client = new_client(captures);
        let server = CapnpServer {
            logger: Logger { client },
            store,
            shutdown: false,
        };
        result.get().set_daemon(new_client(server));
        Ok(())
    }
}
