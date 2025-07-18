use std::future::Future;
use std::pin::pin;
use std::task::{ready, Poll};

use ::capnp::capability::Promise;
use ::capnp::Error;
use capnp_rpc::new_client;
use futures::channel::mpsc;
use futures::stream::StreamExt;
use futures::{SinkExt as _, Stream, TryFutureExt};
use nixrs::daemon::{AddToStoreItem, DaemonResult, DaemonStore, ResultLog};
use nixrs::derived_path::DerivedPath;
use pin_project_lite::pin_project;
use tokio::io::{copy_buf, simplex, AsyncWriteExt as _, BufReader, ReadHalf, SimplexStream};

use crate::capnp::nix_daemon_capnp;
use crate::capnp::nix_daemon_capnp::add_multiple_stream::{AddParams, AddResults};
use crate::capnp::nix_daemon_capnp::logger;
use crate::capnp::nix_daemon_capnp::nix_daemon::{
    AddMultipleToStoreParams, AddMultipleToStoreResults, AddToStoreNarParams, AddToStoreNarResults,
    BuildDerivationParams, BuildDerivationResults, BuildPathsParams, BuildPathsResults,
    BuildPathsWithResultsParams, BuildPathsWithResultsResults, EndParams, EndResults,
    IsValidPathParams, IsValidPathResults, NarFromPathParams, NarFromPathResults,
    QueryMissingParams, QueryMissingResults, QueryPathInfoParams, QueryPathInfoResults,
    QueryValidPathsParams, QueryValidPathsResults, SetOptionsParams, SetOptionsResults,
};
use crate::convert::{BuildFrom, ReadInto};
use crate::{ByteStreamWrap, ByteStreamWriter, DEFAULT_BUF_SIZE};

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
        logs.await.map_err(|err| Error::failed(err.to_string()))
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
                this.store
                    .shutdown()
                    .await
                    .map_err(|err| Error::failed(err.to_string()))?;
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
            let count = p.get_count();
            let (sender, stream) = mpsc::channel(2);
            let sender = AddMultipleStreamServer {
                remaining: count,
                sender,
            };
            let add_stream = new_client(sender);
            result.get().set_stream(add_stream);
            result.set_pipeline()?;
            eprintln!("add_multiple_to_store set_pipeline");
            let stream = CountedStream { count, stream };
            this.logger
                .process_logs(
                    this.store
                        .add_multiple_to_store(repair, dont_check_sigs, stream),
                )
                .await?;
            eprintln!("add_multiple_to_store Done");
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
                .map_err(|err| Error::failed(err.to_string()))?;
            if remaining == 0 {
                sender.close_channel();
            }
            Ok(())
        })
    }
}

pin_project! {
    struct CountedStream {
        count: u16,
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
        eprintln!("CountedStream: poll_next {}", *this.count);
        if let Some(result) = ready!(this.stream.poll_next(cx)) {
            eprintln!("CountedStream: Returning result {}", *this.count);
            *this.count -= 1;
            Poll::Ready(Some(result))
        } else {
            eprintln!("CountedStream: Donne {}", *this.count);
            Poll::Ready(None)
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.count as usize, Some(self.count as usize))
    }
}
