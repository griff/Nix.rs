use std::future::{Future, poll_fn};
use std::pin::{Pin, pin};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, ready};

use ::capnp::Error;
use ::capnp::capability::Promise;
use capnp_rpc::new_client;
use capnp_rpc_tokio::stream::{ByteStreamWrap, ByteStreamWriter};
use futures::channel::{mpsc, oneshot};
use futures::stream::StreamExt;
use futures::{SinkExt as _, Stream, TryFutureExt};
use nixrs::daemon::{AddToStoreItem, DaemonResult, DaemonStore, HandshakeDaemonStore, ResultLog};
use nixrs::derived_path::DerivedPath;
use nixrs::store_path::StorePathHash;
use pin_project_lite::pin_project;
use tokio::io::{AsyncWriteExt as _, BufReader, ReadHalf, SimplexStream, copy_buf, simplex};
use tracing::trace;

use crate::capnp::nix_daemon_capnp;
use crate::capnp::nix_daemon_capnp::add_multiple_stream::{AddParams, AddResults};
use crate::capnp::nix_daemon_capnp::logger;
use crate::capnp::nix_daemon_capnp::nix_daemon::{
    AddMultipleToStoreParams, AddMultipleToStoreResults, AddToStoreNarParams, AddToStoreNarResults,
    BuildDerivationParams, BuildDerivationResults, BuildPathsParams, BuildPathsResults,
    BuildPathsWithResultsParams, BuildPathsWithResultsResults, EndParams, EndResults,
    IsValidPathParams, IsValidPathResults, NarFromPathParams, NarFromPathResults,
    QueryAllValidPathsParams, QueryAllValidPathsResults, QueryMissingParams, QueryMissingResults,
    QueryPathFromHashPartParams, QueryPathFromHashPartResults, QueryPathInfoParams,
    QueryPathInfoResults, QueryValidPathsParams, QueryValidPathsResults, SetOptionsParams,
    SetOptionsResults,
};
use crate::convert::{BuildFrom, ReadInto};
use crate::{DEFAULT_BUF_SIZE, from_error};

pub struct NoopLogger;
impl logger::Server for NoopLogger {
    fn write(&mut self, _: logger::WriteParams) -> Promise<(), Error> {
        Promise::ok(())
    }

    fn end(&mut self, _: logger::EndParams, _: logger::EndResults) -> Promise<(), Error> {
        Promise::ok(())
    }
}

#[derive(Clone)]
pub struct Logger {
    client: logger::Client,
}

impl Logger {
    pub async fn process_logs<R>(
        self,
        logs: impl ResultLog<Output = DaemonResult<R>>,
    ) -> Result<R, Error> {
        let mut logs = pin!(logs);
        while let Some(msg) = logs.next().await {
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

impl<S> nix_daemon_capnp::nix_daemon::Server for CapnpServer<S>
where
    S: DaemonStore + Clone + 'static,
{
    fn end(&mut self, _: EndParams, _: EndResults) -> Promise<(), Error> {
        let mut this = self.clone();
        Promise::from_future(async move {
            this.logger.end().await?;
            if this.shutdown {
                this.store.shutdown().await.map_err(from_error)?;
            }
            Ok(())
        })
    }

    fn set_options(
        &mut self,
        params: SetOptionsParams,
        _: SetOptionsResults,
    ) -> Promise<(), Error> {
        let mut this = self.clone();
        Promise::from_future(async move {
            let options = params.get()?.get_options()?.read_into()?;
            this.logger
                .process_logs(this.store.set_options(&options))
                .await
        })
    }

    fn is_valid_path(
        &mut self,
        params: IsValidPathParams,
        mut result: IsValidPathResults,
    ) -> Promise<(), Error> {
        let mut this = self.clone();
        Promise::from_future(async move {
            let path = params.get()?.get_path()?.read_into()?;
            let valid = this
                .logger
                .process_logs(this.store.is_valid_path(&path))
                .await?;
            result.get().set_valid(valid);
            Ok(())
        })
    }

    fn query_valid_paths(
        &mut self,
        params: QueryValidPathsParams,
        mut result: QueryValidPathsResults,
    ) -> Promise<(), Error> {
        let mut this = self.clone();
        Promise::from_future(async move {
            let p = params.get()?;
            let paths = p.get_paths()?.read_into()?;
            let substitute = p.get_substitute();
            let valid = this
                .logger
                .process_logs(this.store.query_valid_paths(&paths, substitute))
                .await?;
            result
                .get()
                .init_valid_set(valid.len() as u32)
                .build_from(&valid)?;
            Ok(())
        })
    }

    fn query_path_info(
        &mut self,
        params: QueryPathInfoParams,
        mut result: QueryPathInfoResults,
    ) -> Promise<(), Error> {
        let mut this = self.clone();
        Promise::from_future(async move {
            let path = params.get()?.get_path()?.read_into()?;
            if let Some(info) = this
                .logger
                .process_logs(this.store.query_path_info(&path))
                .await?
            {
                result.get().init_info().build_from(&info)?;
            }
            Ok(())
        })
    }

    fn nar_from_path(
        &mut self,
        params: NarFromPathParams,
        _: NarFromPathResults,
    ) -> Promise<(), Error> {
        let mut this = self.clone();
        Promise::from_future(async move {
            let p = params.get()?;
            let path = p.get_path()?.read_into()?;
            let stream = p.get_stream()?;
            let writer = ByteStreamWriter::new(stream);
            let reader = this
                .logger
                .process_logs(this.store.nar_from_path(&path))
                .await?;
            let mut writer = pin!(writer);
            let mut reader = pin!(reader);
            copy_buf(&mut reader, &mut writer).await?;
            writer.shutdown().await?;
            Ok(())
        })
    }

    fn build_paths(
        &mut self,
        params: BuildPathsParams,
        _: BuildPathsResults,
    ) -> Promise<(), Error> {
        let mut this = self.clone();
        Promise::from_future(async move {
            let p = params.get()?;
            let drvs: Vec<DerivedPath> = p.get_drvs()?.read_into()?;
            let mode = p.get_mode()?.into();
            this.logger
                .process_logs(this.store.build_paths(&drvs, mode))
                .await?;
            Ok(())
        })
    }

    fn build_paths_with_results(
        &mut self,
        params: BuildPathsWithResultsParams,
        mut result: BuildPathsWithResultsResults,
    ) -> Promise<(), Error> {
        let mut this = self.clone();
        Promise::from_future(async move {
            let p = params.get()?;
            let drvs: Vec<DerivedPath> = p.get_drvs()?.read_into()?;
            let mode = p.get_mode()?.into();
            let results = this
                .logger
                .process_logs(this.store.build_paths_with_results(&drvs, mode))
                .await?;
            result
                .get()
                .init_results(results.len() as u32)
                .build_from(&results)?;
            Ok(())
        })
    }

    fn build_derivation(
        &mut self,
        params: BuildDerivationParams,
        mut result: BuildDerivationResults,
    ) -> Promise<(), Error> {
        let mut this = self.clone();
        Promise::from_future(async move {
            let p = params.get()?;
            let drv = p.get_drv()?.read_into()?;
            let mode = p.get_mode()?.into();
            let build_result = this
                .logger
                .process_logs(this.store.build_derivation(&drv, mode))
                .await?;
            result.get().init_result().build_from(&build_result)?;
            Ok(())
        })
    }

    fn query_missing(
        &mut self,
        params: QueryMissingParams,
        mut result: QueryMissingResults,
    ) -> Promise<(), Error> {
        let mut this = self.clone();
        Promise::from_future(async move {
            let p = params.get()?;
            let paths: Vec<DerivedPath> = p.get_paths()?.read_into()?;
            let missing = this
                .logger
                .process_logs(this.store.query_missing(&paths))
                .await?;
            result.get().init_missing().build_from(&missing)?;
            Ok(())
        })
    }

    fn add_to_store_nar(
        &mut self,
        params: AddToStoreNarParams,
        mut result: AddToStoreNarResults,
    ) -> Promise<(), Error> {
        let mut this = self.clone();
        Promise::from_future(async move {
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
            this.logger
                .process_logs(
                    this.store
                        .add_to_store_nar(&info, source, repair, dont_check_sigs),
                )
                .await?;
            eprintln!("add_to_store_nar Done");
            Ok(())
        })
    }

    fn add_multiple_to_store(
        &mut self,
        params: AddMultipleToStoreParams,
        mut result: AddMultipleToStoreResults,
    ) -> Promise<(), Error> {
        let mut this = self.clone();
        Promise::from_future(async move {
            let p = params.get()?;
            let repair = p.get_repair();
            let dont_check_sigs = p.get_dont_check_sigs();
            let remaining = p.get_count();
            trace!(count = remaining, "add_multiple_to_store Processing stream");
            let (mut sender, stream) = mpsc::channel(2);
            if remaining == 0 {
                sender.close_channel();
            }
            let sender = AddMultipleStreamServer { remaining, sender };
            let add_stream = new_client(sender);
            result.get().set_stream(add_stream);
            result.set_pipeline()?;
            trace!(count = remaining, "add_multiple_to_store set_pipeline");
            let stream = CountedStream { remaining, stream };
            this.logger
                .process_logs(
                    this.store
                        .add_multiple_to_store(repair, dont_check_sigs, stream),
                )
                .await?;
            trace!(count = remaining, "add_multiple_to_store Done");
            Ok(())
        })
    }

    fn query_all_valid_paths(
        &mut self,
        _params: QueryAllValidPathsParams,
        mut result: QueryAllValidPathsResults,
    ) -> Promise<(), Error> {
        let mut this = self.clone();
        Promise::from_future(async move {
            let paths = this
                .logger
                .process_logs(this.store.query_all_valid_paths())
                .await?;
            result
                .get()
                .init_paths(paths.len() as u32)
                .build_from(&paths)?;
            Ok(())
        })
    }

    fn query_path_from_hash_part(
        &mut self,
        params: QueryPathFromHashPartParams,
        mut result: QueryPathFromHashPartResults,
    ) -> Promise<(), Error> {
        let mut this = self.clone();
        Promise::from_future(async move {
            let p = params.get()?;
            let hash: StorePathHash = p.get_hash()?.read_into()?;
            let res = this
                .logger
                .process_logs(this.store.query_path_from_hash_part(&hash))
                .await?;
            if let Some(path) = res {
                result.get().set_path(&path)?;
            }
            Ok(())
        })
    }
}

struct AddMultipleStreamServer {
    remaining: u16,
    sender: mpsc::Sender<DaemonResult<AddToStoreItem<BufReader<ReadHalf<SimplexStream>>>>>,
}
impl nix_daemon_capnp::add_multiple_stream::Server for AddMultipleStreamServer {
    fn add(&mut self, params: AddParams, mut result: AddResults) -> Promise<(), Error> {
        if self.remaining == 0 {
            return Promise::err(Error::failed(
                "Sending more items than specified in addMultipleToStore call".into(),
            ));
        }
        self.remaining -= 1;
        let mut sender = self.sender.clone();
        let remaining = self.remaining;
        Promise::from_future(async move {
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
            if remaining == 0 {
                sender.close_channel();
            }
            Ok(())
        })
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
        logger: nix_daemon_capnp::logger::Client,
    ) -> Poll<::capnp::Result<S>> {
        loop {
            match std::mem::replace(self, Self::Invalid) {
                Inner::Handshake(hs) => {
                    let logger = logger.clone();
                    let fut = Box::pin(async move {
                        let logger = Logger { client: logger };
                        let logs = hs.handshake();
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
    inner: Arc<Mutex<Inner<HS, S>>>,
}

impl<HS, S> HandshakeLoggedCapnpServer<HS, S>
where
    HS: HandshakeDaemonStore<Store = S>,
    S: DaemonStore + Clone + 'static,
{
    pub fn new(store: HS) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Inner::Handshake(store))),
        }
    }
}

impl<HS, S> nix_daemon_capnp::logged_nix_daemon::Server for HandshakeLoggedCapnpServer<HS, S>
where
    HS: HandshakeDaemonStore<Store = S> + 'static,
    S: DaemonStore + Clone + 'static,
{
    fn capture_logs(
        &mut self,
        params: nix_daemon_capnp::logged_nix_daemon::CaptureLogsParams,
        mut result: nix_daemon_capnp::logged_nix_daemon::CaptureLogsResults,
    ) -> Promise<(), ::capnp::Error> {
        let inner = self.inner.clone();
        Promise::from_future(async move {
            let (sender, receiver) = oneshot::channel();
            let logger = params.get()?.get_logger()?;
            let captures = Captures {
                client: logger,
                sender: Some(sender),
            };
            let client: nix_daemon_capnp::logger::Client = new_client(captures);
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
            result.set_pipeline()?;
            receiver.await.map_err(from_error)?;
            Ok(())
        })
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
    client: nix_daemon_capnp::logger::Client,
    sender: Option<oneshot::Sender<()>>,
}

impl nix_daemon_capnp::logger::Server for Captures {
    fn write(
        &mut self,
        params: nix_daemon_capnp::logger::WriteParams,
    ) -> Promise<(), ::capnp::Error> {
        let client = self.client.clone();
        Promise::from_future(async move {
            let mut req = client.write_request();
            req.get().set_event(params.get()?.get_event()?)?;
            req.send().await
        })
    }

    fn end(
        &mut self,
        _: nix_daemon_capnp::logger::EndParams,
        _: nix_daemon_capnp::logger::EndResults,
    ) -> Promise<(), ::capnp::Error> {
        let client = self.client.clone();
        let sender = self.sender.take();
        Promise::from_future(async move {
            let req = client.end_request();
            req.send().promise.await?;
            if let Some(sender) = sender {
                let _ = sender.send(());
            }
            Ok(())
        })
    }
}

impl<S> nix_daemon_capnp::logged_nix_daemon::Server for LoggedCapnpServer<S>
where
    S: DaemonStore + Clone + 'static,
{
    fn capture_logs(
        &mut self,
        params: nix_daemon_capnp::logged_nix_daemon::CaptureLogsParams,
        mut result: nix_daemon_capnp::logged_nix_daemon::CaptureLogsResults,
    ) -> Promise<(), ::capnp::Error> {
        let store = self.store.clone();
        Promise::from_future(async move {
            let (sender, receiver) = oneshot::channel();
            let client = params.get()?.get_logger()?;
            let captures = Captures {
                client,
                sender: Some(sender),
            };
            let client = new_client(captures);
            let server = CapnpServer {
                logger: Logger { client },
                store,
                shutdown: false,
            };
            result.get().set_daemon(new_client(server));
            result.set_pipeline()?;
            receiver.await.map_err(from_error)?;
            Ok(())
        })
    }
}
