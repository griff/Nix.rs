use std::fmt;
use std::future::ready;
use std::pin::pin;

use ::capnp::Error;
use ::capnp::capability::Promise;
use capnp_rpc::new_client;
use capnp_rpc_tokio::stream::{ByteStreamWrap, ByteStreamWriter, from_cap_error};
use futures::channel::mpsc;
use futures::stream::TryStreamExt;
use futures::{SinkExt, TryFutureExt as _};
use nixrs::daemon::wire::types2::BuildMode;
use nixrs::daemon::{
    DaemonError, DaemonResult, DriveResult, FutureResultExt as _, LocalDaemonStore,
    LocalHandshakeDaemonStore, ResultLog, UnkeyedValidPathInfo,
};
use nixrs::derived_path::DerivedPath;
use nixrs::log::LogMessage;
use nixrs::store_path::{HasStoreDir, StoreDir, StorePath, StorePathSet};
use tokio::io::{AsyncWriteExt, BufReader, copy_buf, simplex};

use crate::capnp::nix_daemon_capnp;
use crate::convert::{BuildFrom, ReadInto};
use crate::{DEFAULT_BUF_SIZE, from_error};

pub struct CapnpStore {
    store_dir: StoreDir,
    store: nix_daemon_capnp::nix_daemon::Client,
}

impl CapnpStore {
    pub fn new(store: nix_daemon_capnp::nix_daemon::Client) -> Self {
        Self::with_store_dir(store, StoreDir::default())
    }

    pub fn with_store_dir(
        store: nix_daemon_capnp::nix_daemon::Client,
        store_dir: StoreDir,
    ) -> Self {
        Self { store, store_dir }
    }
}

impl fmt::Debug for CapnpStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CapnpStore").finish()
    }
}

impl HasStoreDir for CapnpStore {
    fn store_dir(&self) -> &StoreDir {
        &self.store_dir
    }
}

impl LocalHandshakeDaemonStore for CapnpStore {
    type Store = Self;

    fn handshake(self) -> impl ResultLog<Output = DaemonResult<Self::Store>> {
        ready(Ok(self)).empty_logs()
    }
}

impl LocalDaemonStore for CapnpStore {
    fn trust_level(&self) -> nixrs::daemon::TrustLevel {
        nixrs::daemon::TrustLevel::Trusted
    }

    async fn shutdown(&mut self) -> DaemonResult<()> {
        let req = self.store.end_request();
        req.send()
            .promise
            .await
            .map(|_| ())
            .map_err(DaemonError::custom)
    }

    fn set_options<'a>(
        &'a mut self,
        options: &'a nixrs::daemon::ClientOptions,
    ) -> impl ResultLog<Output = DaemonResult<()>> + 'a {
        (async move {
            let mut req = self.store.set_options_request();
            req.get().set_options(options)?;
            req.send().promise.await.map(|_| ())
        })
        .map_err(DaemonError::custom)
        .empty_logs()
    }

    fn is_valid_path<'a>(
        &'a mut self,
        path: &'a nixrs::store_path::StorePath,
    ) -> impl ResultLog<Output = DaemonResult<bool>> + 'a {
        (async move {
            let mut req = self.store.is_valid_path_request();
            let mut params = req.get();
            params.set_path(path)?;
            let resp = req.send().promise.await?;
            resp.get().map(|r| r.get_valid())
        })
        .map_err(DaemonError::custom)
        .empty_logs()
    }

    fn query_valid_paths<'a>(
        &'a mut self,
        paths: &'a StorePathSet,
        substitute: bool,
    ) -> impl ResultLog<Output = DaemonResult<StorePathSet>> + 'a {
        (async move {
            let mut req = self.store.query_valid_paths_request();
            let mut params = req.get();
            let mut c_paths = params.reborrow().init_paths(paths.len() as u32);
            c_paths.build_from(paths)?;
            params.set_substitute(substitute);
            let resp = req.send().promise.await?;
            let r = resp.get()?;
            r.get_valid_set()?.read_into()
        })
        .map_err(DaemonError::custom)
        .empty_logs()
    }

    fn query_path_info<'a>(
        &'a mut self,
        path: &'a StorePath,
    ) -> impl ResultLog<Output = DaemonResult<Option<UnkeyedValidPathInfo>>> + 'a {
        (async move {
            let mut req = self.store.query_path_info_request();
            let mut params = req.get();
            params.set_path(path)?;

            let resp = req.send().promise.await?;
            let r = resp.get()?;
            if r.has_info() {
                Ok(Some(r.get_info()?.read_into()?))
            } else {
                Ok(None) as Result<_, Error>
            }
        })
        .map_err(DaemonError::custom)
        .empty_logs()
    }

    fn nar_from_path<'s>(
        &'s mut self,
        path: &'s StorePath,
    ) -> impl ResultLog<Output = DaemonResult<impl tokio::io::AsyncBufRead + use<>>> + 's {
        (async move {
            let (reader, writer) = simplex(DEFAULT_BUF_SIZE);
            let reader = BufReader::new(reader);
            let bs_write = ByteStreamWrap::new(writer);

            let mut req = self.store.nar_from_path_request();
            let mut params = req.get();
            params.reborrow().set_path(path)?;
            params.set_stream(new_client(bs_write));

            let driver = req.send().promise.map_ok(|_| ()).map_err(from_cap_error);
            let stream = DriveResult::new(reader, driver);
            Ok(stream) as Result<_, Error>
        })
        .map_err(DaemonError::custom)
        .empty_logs()
    }

    fn build_paths<'a>(
        &'a mut self,
        drvs: &'a [DerivedPath],
        mode: BuildMode,
    ) -> impl ResultLog<Output = DaemonResult<()>> + 'a {
        (async move {
            let mut req = self.store.build_paths_request();
            let mut params = req.get();
            let mut c_paths = params.reborrow().init_drvs(drvs.len() as u32);
            c_paths.build_from(&drvs)?;
            params.set_mode(mode.into());
            let resp = req.send().promise.await?;
            resp.get()?;
            Ok(()) as Result<_, Error>
        })
        .map_err(DaemonError::custom)
        .empty_logs()
    }

    fn build_paths_with_results<'a>(
        &'a mut self,
        drvs: &'a [DerivedPath],
        mode: BuildMode,
    ) -> impl ResultLog<Output = DaemonResult<Vec<nixrs::daemon::wire::types2::KeyedBuildResult>>> + 'a
    {
        (async move {
            let mut req = self.store.build_paths_with_results_request();
            let mut params = req.get();
            let mut c_paths = params.reborrow().init_drvs(drvs.len() as u32);
            c_paths.build_from(&drvs)?;
            params.set_mode(mode.into());
            let resp = req.send().promise.await?;
            resp.get()?.get_results()?.read_into()
        })
        .map_err(DaemonError::custom)
        .empty_logs()
    }

    fn build_derivation<'a>(
        &'a mut self,
        drv: &'a nixrs::derivation::BasicDerivation,
        mode: BuildMode,
    ) -> impl ResultLog<Output = DaemonResult<nixrs::daemon::wire::types2::BuildResult>> + 'a {
        (async move {
            let mut req = self.store.build_derivation_request();
            let mut params = req.get();
            params.set_drv(drv)?;
            /*
            let mut drv_b = params.reborrow().init_drv();
            drv_b.build_from(drv)?;
            */
            params.set_mode(mode.into());
            let resp = req.send().promise.await?;
            resp.get()?.get_result()?.read_into()
        })
        .map_err(DaemonError::custom)
        .empty_logs()
    }

    fn query_missing<'a>(
        &'a mut self,
        paths: &'a [DerivedPath],
    ) -> impl ResultLog<Output = DaemonResult<nixrs::daemon::wire::types2::QueryMissingResult>> + 'a
    {
        (async move {
            let mut req = self.store.query_missing_request();
            let mut params = req.get();
            let mut paths_b = params.reborrow().init_paths(paths.len() as u32);
            paths_b.build_from(&paths)?;
            let resp = req.send().promise.await?;
            resp.get()?.get_missing()?.read_into()
        })
        .map_err(DaemonError::custom)
        .empty_logs()
    }

    fn add_to_store_nar<'s, 'r, 'i, R>(
        &'s mut self,
        info: &'i nixrs::daemon::wire::types2::ValidPathInfo,
        mut source: R,
        repair: bool,
        dont_check_sigs: bool,
    ) -> impl ResultLog<Output = DaemonResult<()>> + 'r
    where
        R: tokio::io::AsyncBufRead + Unpin + 'r,
        's: 'r,
        'i: 'r,
    {
        (async move {
            let mut req = self.store.add_to_store_nar_request();
            let mut params = req.get();
            params.reborrow().init_info().build_from(info)?;
            params.set_repair(repair);
            params.set_dont_check_sigs(dont_check_sigs);
            let res = req.send();
            let stream = res.pipeline.get_stream();
            let mut writer = ByteStreamWriter::new(stream);
            let written = copy_buf(&mut source, &mut writer).await?;
            eprintln!("add_to_store_nar Done writing {written}");
            writer.shutdown().await?;
            eprintln!("add_to_store_nar Shutdown");
            res.promise.await?;
            Ok(()) as Result<(), Error>
        })
        .map_err(DaemonError::custom)
        .empty_logs()
    }

    fn add_multiple_to_store<'s, 'i, 'r, S, R>(
        &'s mut self,
        repair: bool,
        dont_check_sigs: bool,
        stream: S,
    ) -> impl ResultLog<Output = DaemonResult<()>> + 'r
    where
        S: futures::Stream<Item = Result<nixrs::daemon::AddToStoreItem<R>, DaemonError>> + 'i,
        R: tokio::io::AsyncBufRead + Unpin + 'i,
        's: 'r,
        'i: 'r,
    {
        let size = stream.size_hint().1.expect("Stream with size");
        (async move {
            let mut req = self.store.add_multiple_to_store_request();
            let mut params = req.get();
            params.set_repair(repair);
            params.set_dont_check_sigs(dont_check_sigs);
            params.set_count(size.try_into().map_err(DaemonError::custom)?);
            let res = req.send();
            let add_multiple = res.pipeline.get_stream();
            let mut stream = pin!(stream);
            while let Some(mut item) = stream.try_next().await? {
                let mut add_req = add_multiple.add_request();
                let params = add_req.get();
                params
                    .init_info()
                    .build_from(&item.info)
                    .map_err(DaemonError::custom)?;
                let res = add_req.send();
                let stream = res.pipeline.get_stream();
                let mut writer = ByteStreamWriter::new(stream);
                copy_buf(&mut item.reader, &mut writer).await?;
                writer.shutdown().await?;
                eprintln!("add_multiple_to_store waiting for add result");
                res.promise.await.map_err(DaemonError::custom)?;
            }
            eprintln!("add_multiple_to_store waiting for result");
            res.promise.await.map_err(DaemonError::custom)?;
            eprintln!("add_multiple_to_store request done");
            Ok(())
        })
        .empty_logs()
    }
}

pub struct LoggerStream {
    sender: mpsc::Sender<LogMessage>,
}
impl LoggerStream {
    pub fn new() -> (LoggerStream, mpsc::Receiver<LogMessage>) {
        let (sender, receiver) = mpsc::channel(2);
        (LoggerStream { sender }, receiver)
    }
}

impl nix_daemon_capnp::logger::Server for LoggerStream {
    fn write(
        &mut self,
        params: nix_daemon_capnp::logger::WriteParams,
    ) -> Promise<(), ::capnp::Error> {
        let mut sender = self.sender.clone();
        Promise::from_future(async move {
            let msg = params.get()?.get_event()?.read_into()?;
            sender.send(msg).await.map_err(from_error)
        })
    }

    fn end(
        &mut self,
        _: nix_daemon_capnp::logger::EndParams,
        _: nix_daemon_capnp::logger::EndResults,
    ) -> Promise<(), ::capnp::Error> {
        self.sender.disconnect();
        Promise::ok(())
    }
}

pub struct LoggedCapnpStore {
    store_dir: StoreDir,
    store: nix_daemon_capnp::logged_nix_daemon::Client,
}

impl LoggedCapnpStore {
    pub fn new(store: nix_daemon_capnp::logged_nix_daemon::Client) -> Self {
        Self::with_store_dir(store, Default::default())
    }

    pub fn with_store_dir(
        store: nix_daemon_capnp::logged_nix_daemon::Client,
        store_dir: StoreDir,
    ) -> Self {
        Self { store, store_dir }
    }
}

impl fmt::Debug for LoggedCapnpStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LoggedCapnpStore").finish()
    }
}

impl HasStoreDir for LoggedCapnpStore {
    fn store_dir(&self) -> &StoreDir {
        &self.store_dir
    }
}

impl LocalHandshakeDaemonStore for LoggedCapnpStore {
    type Store = Self;

    fn handshake(self) -> impl ResultLog<Output = DaemonResult<Self::Store>> {
        (async move {
            let (sender, receiver) = LoggerStream::new();
            let mut req = self.store.capture_logs_request();
            req.get().set_logger(new_client(sender));
            eprintln!("Doing client handsshake");
            let capture_res = req.send();
            let capnp_store = capture_res.pipeline.get_daemon();
            Ok(async move {
                let mut store = CapnpStore::new(capnp_store);
                eprintln!("Shutting down client handsshake");
                let end_res = store.shutdown().await;
                let mres = capture_res.promise.await.map(|_| ());
                eprintln!("Sending client handsshake result {end_res:?} {mres:?}");
                end_res?;
                Ok(self)
            }
            .with_logs(receiver))
        })
        .future_result()
    }
}

macro_rules! make_request {
    ($self:ident, |$store:ident| $($block:tt)*) => {
        (async move {
            let (sender, receiver) = LoggerStream::new();
            let mut req = $self.store.capture_logs_request();
            req.get().set_logger(new_client(sender));
            let capture_res = req.send();
            let capnp_store = capture_res.pipeline.get_daemon();
            Ok(async move {
                let mut $store = CapnpStore::new(capnp_store);
                let res = {
                    $($block)*
                };
                let end_res = $store.shutdown().await;
                let _ = capture_res.promise.await.map_err(DaemonError::custom);
                let value = res.map_err(DaemonError::custom)?;
                end_res?;
                Ok(value)
            }.with_logs(receiver))
        })
        .future_result()
    };
}

impl LocalDaemonStore for LoggedCapnpStore {
    fn trust_level(&self) -> nixrs::daemon::TrustLevel {
        nixrs::daemon::TrustLevel::Trusted
    }

    fn set_options<'a>(
        &'a mut self,
        options: &'a nixrs::daemon::ClientOptions,
    ) -> impl ResultLog<Output = DaemonResult<()>> + 'a {
        make_request!(self, |store| store.set_options(options).await)
    }

    fn is_valid_path<'a>(
        &'a mut self,
        path: &'a nixrs::store_path::StorePath,
    ) -> impl ResultLog<Output = DaemonResult<bool>> + 'a {
        make_request!(self, |store| store.is_valid_path(path).await)
    }

    fn query_valid_paths<'a>(
        &'a mut self,
        paths: &'a StorePathSet,
        substitute: bool,
    ) -> impl ResultLog<Output = DaemonResult<StorePathSet>> + 'a {
        make_request!(self, |store| {
            store.query_valid_paths(paths, substitute).await
        })
    }

    fn query_path_info<'a>(
        &'a mut self,
        path: &'a StorePath,
    ) -> impl ResultLog<Output = DaemonResult<Option<UnkeyedValidPathInfo>>> + 'a {
        make_request!(self, |store| store.query_path_info(path).await)
    }

    fn nar_from_path<'s>(
        &'s mut self,
        path: &'s StorePath,
    ) -> impl ResultLog<Output = DaemonResult<impl tokio::io::AsyncBufRead + use<>>> + 's {
        make_request!(self, |store| store.nar_from_path(path).await)
    }

    fn build_paths<'a>(
        &'a mut self,
        drvs: &'a [DerivedPath],
        mode: BuildMode,
    ) -> impl ResultLog<Output = DaemonResult<()>> + 'a {
        make_request!(self, |store| store.build_paths(drvs, mode).await)
    }

    fn build_paths_with_results<'a>(
        &'a mut self,
        drvs: &'a [DerivedPath],
        mode: BuildMode,
    ) -> impl ResultLog<Output = DaemonResult<Vec<nixrs::daemon::wire::types2::KeyedBuildResult>>> + 'a
    {
        make_request!(self, |store| {
            store.build_paths_with_results(drvs, mode).await
        })
    }

    fn build_derivation<'a>(
        &'a mut self,
        drv: &'a nixrs::derivation::BasicDerivation,
        mode: BuildMode,
    ) -> impl ResultLog<Output = DaemonResult<nixrs::daemon::wire::types2::BuildResult>> + 'a {
        make_request!(self, |store| store.build_derivation(drv, mode).await)
    }

    fn query_missing<'a>(
        &'a mut self,
        paths: &'a [DerivedPath],
    ) -> impl ResultLog<Output = DaemonResult<nixrs::daemon::wire::types2::QueryMissingResult>> + 'a
    {
        make_request!(self, |store| store.query_missing(paths).await)
    }

    fn add_to_store_nar<'s, 'r, 'i, R>(
        &'s mut self,
        info: &'i nixrs::daemon::wire::types2::ValidPathInfo,
        source: R,
        repair: bool,
        dont_check_sigs: bool,
    ) -> impl ResultLog<Output = DaemonResult<()>> + 'r
    where
        R: tokio::io::AsyncBufRead + Unpin + 'r,
        's: 'r,
        'i: 'r,
    {
        make_request!(self, |store| {
            store
                .add_to_store_nar(info, source, repair, dont_check_sigs)
                .await
        })
    }

    fn add_multiple_to_store<'s, 'i, 'r, S, R>(
        &'s mut self,
        repair: bool,
        dont_check_sigs: bool,
        stream: S,
    ) -> impl ResultLog<Output = DaemonResult<()>> + 'r
    where
        S: futures::Stream<Item = Result<nixrs::daemon::AddToStoreItem<R>, DaemonError>> + 'i,
        R: tokio::io::AsyncBufRead + Unpin + 'i,
        's: 'r,
        'i: 'r,
    {
        make_request!(self, |store| {
            store
                .add_multiple_to_store(repair, dont_check_sigs, stream)
                .await
        })
    }

    fn query_all_valid_paths(
        &mut self,
    ) -> impl ResultLog<Output = DaemonResult<StorePathSet>> + '_ {
        make_request!(self, |store| store.query_all_valid_paths().await)
    }

    fn query_referrers<'a>(
        &'a mut self,
        path: &'a StorePath,
    ) -> impl ResultLog<Output = DaemonResult<StorePathSet>> + 'a {
        make_request!(self, |store| store.query_referrers(path).await)
    }

    fn ensure_path<'a>(
        &'a mut self,
        path: &'a StorePath,
    ) -> impl ResultLog<Output = DaemonResult<()>> + 'a {
        make_request!(self, |store| store.ensure_path(path).await)
    }

    fn add_temp_root<'a>(
        &'a mut self,
        path: &'a StorePath,
    ) -> impl ResultLog<Output = DaemonResult<()>> + 'a {
        make_request!(self, |store| store.add_temp_root(path).await)
    }

    fn add_indirect_root<'a>(
        &'a mut self,
        path: &'a nixrs::daemon::DaemonPath,
    ) -> impl ResultLog<Output = DaemonResult<()>> + 'a {
        make_request!(self, |store| store.add_indirect_root(path).await)
    }

    fn find_roots(
        &mut self,
    ) -> impl ResultLog<
        Output = DaemonResult<std::collections::BTreeMap<nixrs::daemon::DaemonPath, StorePath>>,
    > + '_ {
        make_request!(self, |store| store.find_roots().await)
    }

    fn collect_garbage<'a>(
        &'a mut self,
        action: nixrs::daemon::wire::types2::GCAction,
        paths_to_delete: &'a StorePathSet,
        ignore_liveness: bool,
        max_freed: u64,
    ) -> impl ResultLog<Output = DaemonResult<nixrs::daemon::wire::types2::CollectGarbageResponse>> + 'a
    {
        make_request!(self, |store| {
            store
                .collect_garbage(action, paths_to_delete, ignore_liveness, max_freed)
                .await
        })
    }

    fn query_path_from_hash_part<'a>(
        &'a mut self,
        hash: &'a nixrs::store_path::StorePathHash,
    ) -> impl ResultLog<Output = DaemonResult<Option<StorePath>>> + 'a {
        make_request!(self, |store| {
            store.query_path_from_hash_part(hash).await
        })
    }

    fn query_substitutable_paths<'a>(
        &'a mut self,
        paths: &'a StorePathSet,
    ) -> impl ResultLog<Output = DaemonResult<StorePathSet>> + 'a {
        make_request!(self, |store| {
            store.query_substitutable_paths(paths).await
        })
    }

    fn query_valid_derivers<'a>(
        &'a mut self,
        path: &'a StorePath,
    ) -> impl ResultLog<Output = DaemonResult<StorePathSet>> + 'a {
        make_request!(self, |store| store.query_valid_derivers(path).await)
    }

    fn optimise_store(&mut self) -> impl ResultLog<Output = DaemonResult<()>> + '_ {
        make_request!(self, |store| store.optimise_store().await)
    }

    fn verify_store(
        &mut self,
        check_contents: bool,
        repair: bool,
    ) -> impl ResultLog<Output = DaemonResult<bool>> + '_ {
        make_request!(self, |store| {
            store.verify_store(check_contents, repair).await
        })
    }

    fn add_signatures<'a>(
        &'a mut self,
        path: &'a StorePath,
        signatures: &'a [nixrs::signature::Signature],
    ) -> impl ResultLog<Output = DaemonResult<()>> + 'a {
        make_request!(self, |store| {
            store.add_signatures(path, signatures).await
        })
    }

    fn query_derivation_output_map<'a>(
        &'a mut self,
        path: &'a StorePath,
    ) -> impl ResultLog<
        Output = DaemonResult<
            std::collections::BTreeMap<nixrs::derived_path::OutputName, Option<StorePath>>,
        >,
    > + 'a {
        make_request!(self, |store| {
            store.query_derivation_output_map(path).await
        })
    }

    fn register_drv_output<'a>(
        &'a mut self,
        realisation: &'a nixrs::realisation::Realisation,
    ) -> impl ResultLog<Output = DaemonResult<()>> + 'a {
        make_request!(self, |store| {
            store.register_drv_output(realisation).await
        })
    }

    fn query_realisation<'a>(
        &'a mut self,
        output_id: &'a nixrs::realisation::DrvOutput,
    ) -> impl ResultLog<
        Output = DaemonResult<std::collections::BTreeSet<nixrs::realisation::Realisation>>,
    > + 'a {
        make_request!(self, |store| store.query_realisation(output_id).await)
    }

    fn add_build_log<'s, 'r, 'p, R>(
        &'s mut self,
        path: &'p StorePath,
        source: R,
    ) -> impl ResultLog<Output = DaemonResult<()>> + 'r
    where
        R: tokio::io::AsyncBufRead + Unpin + 'r,
        's: 'r,
        'p: 'r,
    {
        make_request!(self, |store| store.add_build_log(path, source).await)
    }

    fn add_perm_root<'a>(
        &'a mut self,
        path: &'a StorePath,
        gc_root: &'a nixrs::daemon::DaemonPath,
    ) -> impl ResultLog<Output = DaemonResult<nixrs::daemon::DaemonPath>> + 'a {
        make_request!(self, |store| store.add_perm_root(path, gc_root).await)
    }

    fn sync_with_gc(&mut self) -> impl ResultLog<Output = DaemonResult<()>> + '_ {
        make_request!(self, |store| store.sync_with_gc().await)
    }

    fn query_derivation_outputs<'a>(
        &'a mut self,
        path: &'a StorePath,
    ) -> impl ResultLog<Output = DaemonResult<StorePathSet>> + 'a {
        make_request!(self, |store| store.query_derivation_outputs(path).await)
    }

    fn query_derivation_output_names<'a>(
        &'a mut self,
        path: &'a StorePath,
    ) -> impl ResultLog<
        Output = DaemonResult<std::collections::BTreeSet<nixrs::derived_path::OutputName>>,
    > + 'a {
        make_request!(self, |store| {
            store.query_derivation_output_names(path).await
        })
    }

    fn add_ca_to_store<'a, 'r, R>(
        &'a mut self,
        name: &'a str,
        cam: nixrs::store_path::ContentAddressMethodAlgorithm,
        refs: &'a StorePathSet,
        repair: bool,
        source: R,
    ) -> impl ResultLog<Output = DaemonResult<nixrs::daemon::wire::types2::ValidPathInfo>> + 'r
    where
        R: tokio::io::AsyncBufRead + Unpin + 'r,
        'a: 'r,
    {
        make_request!(self, |store| {
            store.add_ca_to_store(name, cam, refs, repair, source).await
        })
    }

    async fn shutdown(&mut self) -> DaemonResult<()> {
        Ok(())
    }
}
