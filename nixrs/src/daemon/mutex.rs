use std::future::Future;
use std::pin::Pin;
use std::{pin::pin, sync::Arc};

use crate::daemon::{
    DaemonStore, FutureResultExt as _, HandshakeDaemonStore, ResultLog, ResultLogExt,
};
use crate::store_path::{HasStoreDir, StoreDir};
use async_stream::stream;
use futures::channel::oneshot;
use futures::{FutureExt as _, StreamExt as _};
use tokio::io::AsyncBufRead;
use tokio::sync::Mutex;

#[derive(Debug)]
pub struct MutexHandshakeStore<HS> {
    inner: HS,
}

impl<HS> MutexHandshakeStore<HS> {
    pub fn new(store: HS) -> Self {
        Self { inner: store }
    }
}

impl<HS: HasStoreDir> HasStoreDir for MutexHandshakeStore<HS> {
    fn store_dir(&self) -> &StoreDir {
        self.inner.store_dir()
    }
}
impl<HS, S> HandshakeDaemonStore for MutexHandshakeStore<HS>
where
    HS: HandshakeDaemonStore<Store = S>,
    S: DaemonStore + 'static,
{
    type Store = MutexStore<S>;

    fn handshake(self) -> impl ResultLog<Output = super::DaemonResult<Self::Store>> + Send {
        self.inner.handshake().map_ok(MutexStore::new)
    }
}

#[derive(Debug)]
pub struct MutexStore<S> {
    trust: super::TrustLevel,
    store_dir: StoreDir,
    m: Arc<Mutex<S>>,
}

impl<S: DaemonStore + Send> MutexStore<S> {
    pub fn new(store: S) -> Self {
        let trust = store.trust_level();
        let store_dir = store.store_dir().clone();
        Self {
            trust,
            store_dir,
            m: Arc::new(Mutex::new(store)),
        }
    }
}

impl<S> Clone for MutexStore<S> {
    fn clone(&self) -> Self {
        Self {
            trust: self.trust,
            store_dir: self.store_dir.clone(),
            m: self.m.clone(),
        }
    }
}

macro_rules! mutex_result {
    ($self:ident, |$store:ident| { $($stm:tt)* }) => {{
        let store = $self.clone();
        let (sender, receiver) = oneshot::channel();
        receiver.map(Result::unwrap).with_logs(stream! {
            let mut $store = store.m.lock().await;
            let real = {
                $($stm)*
            };
            let mut r = pin!(real);
            while let Some(msg) = r.next().await {
                yield msg;
            }
            let ret = r.await;
            let _ = sender.send(ret);
        })
    }};
}

impl<S: HasStoreDir> HasStoreDir for MutexStore<S> {
    fn store_dir(&self) -> &crate::store_path::StoreDir {
        &self.store_dir
    }
}

#[forbid(clippy::missing_trait_methods)]
impl<S> DaemonStore for MutexStore<S>
where
    S: DaemonStore + Send,
{
    fn trust_level(&self) -> super::TrustLevel {
        self.trust
    }

    fn shutdown(&mut self) -> impl Future<Output = super::DaemonResult<()>> + Send + '_ {
        let m = self.clone();
        async move {
            let mut g = m.m.lock().await;
            g.shutdown().await
        }
    }

    fn set_options<'r>(
        &'r mut self,
        options: &'r super::ClientOptions,
    ) -> impl ResultLog<Output = super::DaemonResult<()>> + Send + 'r {
        mutex_result!(self, |store| { store.set_options(options) })
    }

    fn is_valid_path<'a>(
        &'a mut self,
        path: &'a crate::store_path::StorePath,
    ) -> impl ResultLog<Output = super::DaemonResult<bool>> + Send + 'a {
        mutex_result!(self, |store| { store.is_valid_path(path) })
    }

    fn query_valid_paths<'a>(
        &'a mut self,
        paths: &'a crate::store_path::StorePathSet,
        substitute: bool,
    ) -> impl ResultLog<Output = super::DaemonResult<crate::store_path::StorePathSet>> + Send + 'a
    {
        mutex_result!(self, |store| { store.query_valid_paths(paths, substitute) })
    }

    fn query_path_info<'a>(
        &'a mut self,
        path: &'a crate::store_path::StorePath,
    ) -> impl ResultLog<Output = super::DaemonResult<Option<super::UnkeyedValidPathInfo>>> + Send + 'a
    {
        mutex_result!(self, |store| { store.query_path_info(path) })
    }

    fn nar_from_path<'s>(
        &'s mut self,
        path: &'s crate::store_path::StorePath,
    ) -> impl ResultLog<Output = super::DaemonResult<impl tokio::io::AsyncBufRead + Send + use<S>>>
    + Send
    + 's {
        let store = self.m.clone();
        let (sender, receiver) = oneshot::channel();
        let logs = stream! {
            let mut store = store.lock().await;
            let real = store.nar_from_path(path);
            let mut r = pin!(real);
            while let Some(msg) = r.next().await {
                yield msg;
            }
            let ret = r.await;
            let _ = sender.send(ret);
        };
        receiver.map(Result::unwrap).with_logs(logs)
    }

    fn build_paths<'a>(
        &'a mut self,
        drvs: &'a [crate::derived_path::DerivedPath],
        mode: super::wire::types2::BuildMode,
    ) -> impl ResultLog<Output = super::DaemonResult<()>> + Send + 'a {
        mutex_result!(self, |store| { store.build_paths(drvs, mode) })
    }

    fn build_paths_with_results<'a>(
        &'a mut self,
        drvs: &'a [crate::derived_path::DerivedPath],
        mode: super::wire::types2::BuildMode,
    ) -> impl ResultLog<Output = super::DaemonResult<Vec<super::wire::types2::KeyedBuildResult>>>
    + Send
    + 'a {
        mutex_result!(self, |store| { store.build_paths_with_results(drvs, mode) })
    }

    fn build_derivation<'a>(
        &'a mut self,
        drv: &'a crate::derivation::BasicDerivation,
        mode: super::wire::types2::BuildMode,
    ) -> impl ResultLog<Output = super::DaemonResult<super::wire::types2::BuildResult>> + Send + 'a
    {
        mutex_result!(self, |store| { store.build_derivation(drv, mode) })
    }

    fn query_missing<'a>(
        &'a mut self,
        paths: &'a [crate::derived_path::DerivedPath],
    ) -> impl ResultLog<Output = super::DaemonResult<super::wire::types2::QueryMissingResult>> + Send + 'a
    {
        mutex_result!(self, |store| { store.query_missing(paths) })
    }

    fn add_to_store_nar<'s, 'r, 'i, R>(
        &'s mut self,
        info: &'i super::wire::types2::ValidPathInfo,
        source: R,
        repair: bool,
        dont_check_sigs: bool,
    ) -> Pin<Box<dyn ResultLog<Output = super::DaemonResult<()>> + Send + 'r>>
    where
        R: AsyncBufRead + Send + Unpin + 'r,
        's: 'r,
        'i: 'r,
    {
        mutex_result!(self, |store| {
            store.add_to_store_nar(info, source, repair, dont_check_sigs)
        })
        .boxed_result()
    }

    fn add_multiple_to_store<'s, 'i, 'r, SS, R>(
        &'s mut self,
        repair: bool,
        dont_check_sigs: bool,
        stream: SS,
    ) -> Pin<Box<dyn ResultLog<Output = super::DaemonResult<()>> + Send + 'r>>
    where
        SS: futures::Stream<Item = Result<super::AddToStoreItem<R>, super::DaemonError>>
            + Send
            + 'i,
        R: tokio::io::AsyncBufRead + Send + Unpin + 'i,
        's: 'r,
        'i: 'r,
    {
        mutex_result!(self, |store| {
            store.add_multiple_to_store(repair, dont_check_sigs, stream)
        })
        .boxed_result()
    }

    fn query_all_valid_paths(
        &mut self,
    ) -> impl ResultLog<Output = super::DaemonResult<crate::store_path::StorePathSet>> + Send + '_
    {
        mutex_result!(self, |store| { store.query_all_valid_paths() })
    }

    fn query_referrers<'a>(
        &'a mut self,
        path: &'a crate::store_path::StorePath,
    ) -> impl ResultLog<Output = super::DaemonResult<crate::store_path::StorePathSet>> + Send + 'a
    {
        mutex_result!(self, |store| { store.query_referrers(path) })
    }

    fn ensure_path<'a>(
        &'a mut self,
        path: &'a crate::store_path::StorePath,
    ) -> impl ResultLog<Output = super::DaemonResult<()>> + Send + 'a {
        mutex_result!(self, |store| { store.ensure_path(path) })
    }

    fn add_temp_root<'a>(
        &'a mut self,
        path: &'a crate::store_path::StorePath,
    ) -> impl ResultLog<Output = super::DaemonResult<()>> + Send + 'a {
        mutex_result!(self, |store| { store.add_temp_root(path) })
    }

    fn add_indirect_root<'a>(
        &'a mut self,
        path: &'a super::DaemonPath,
    ) -> impl ResultLog<Output = super::DaemonResult<()>> + Send + 'a {
        mutex_result!(self, |store| { store.add_indirect_root(path) })
    }

    fn find_roots(
        &mut self,
    ) -> impl ResultLog<
        Output = super::DaemonResult<
            std::collections::BTreeMap<super::DaemonPath, crate::store_path::StorePath>,
        >,
    > + Send
    + '_ {
        mutex_result!(self, |store| { store.find_roots() })
    }

    fn collect_garbage<'a>(
        &'a mut self,
        action: super::wire::types2::GCAction,
        paths_to_delete: &'a crate::store_path::StorePathSet,
        ignore_liveness: bool,
        max_freed: u64,
    ) -> impl ResultLog<Output = super::DaemonResult<super::wire::types2::CollectGarbageResponse>>
    + Send
    + 'a {
        mutex_result!(self, |store| {
            store.collect_garbage(action, paths_to_delete, ignore_liveness, max_freed)
        })
    }

    fn query_path_from_hash_part<'a>(
        &'a mut self,
        hash: &'a crate::store_path::StorePathHash,
    ) -> impl ResultLog<Output = super::DaemonResult<Option<crate::store_path::StorePath>>> + Send + 'a
    {
        mutex_result!(self, |store| { store.query_path_from_hash_part(hash) })
    }

    fn query_substitutable_paths<'a>(
        &'a mut self,
        paths: &'a crate::store_path::StorePathSet,
    ) -> impl ResultLog<Output = super::DaemonResult<crate::store_path::StorePathSet>> + Send + 'a
    {
        mutex_result!(self, |store| { store.query_substitutable_paths(paths) })
    }

    fn query_valid_derivers<'a>(
        &'a mut self,
        path: &'a crate::store_path::StorePath,
    ) -> impl ResultLog<Output = super::DaemonResult<crate::store_path::StorePathSet>> + Send + 'a
    {
        mutex_result!(self, |store| { store.query_valid_derivers(path) })
    }

    fn optimise_store(&mut self) -> impl ResultLog<Output = super::DaemonResult<()>> + Send + '_ {
        mutex_result!(self, |store| { store.optimise_store() })
    }

    fn verify_store(
        &mut self,
        check_contents: bool,
        repair: bool,
    ) -> impl ResultLog<Output = super::DaemonResult<bool>> + Send + '_ {
        mutex_result!(self, |store| { store.verify_store(check_contents, repair) })
    }

    fn add_signatures<'a>(
        &'a mut self,
        path: &'a crate::store_path::StorePath,
        signatures: &'a [crate::signature::Signature],
    ) -> impl ResultLog<Output = super::DaemonResult<()>> + Send + 'a {
        mutex_result!(self, |store| { store.add_signatures(path, signatures) })
    }

    fn query_derivation_output_map<'a>(
        &'a mut self,
        path: &'a crate::store_path::StorePath,
    ) -> impl ResultLog<
        Output = super::DaemonResult<
            std::collections::BTreeMap<
                crate::derived_path::OutputName,
                Option<crate::store_path::StorePath>,
            >,
        >,
    > + Send
    + 'a {
        mutex_result!(self, |store| { store.query_derivation_output_map(path) })
    }

    fn register_drv_output<'a>(
        &'a mut self,
        realisation: &'a crate::realisation::Realisation,
    ) -> impl ResultLog<Output = super::DaemonResult<()>> + Send + 'a {
        mutex_result!(self, |store| { store.register_drv_output(realisation) })
    }

    fn query_realisation<'a>(
        &'a mut self,
        output_id: &'a crate::realisation::DrvOutput,
    ) -> impl ResultLog<
        Output = super::DaemonResult<std::collections::BTreeSet<crate::realisation::Realisation>>,
    > + Send
    + 'a {
        mutex_result!(self, |store| { store.query_realisation(output_id) })
    }

    fn add_build_log<'s, 'r, 'p, R>(
        &'s mut self,
        path: &'p crate::store_path::StorePath,
        source: R,
    ) -> Pin<Box<dyn ResultLog<Output = super::DaemonResult<()>> + Send + 'r>>
    where
        R: tokio::io::AsyncBufRead + Send + Unpin + 'r,
        's: 'r,
        'p: 'r,
    {
        mutex_result!(self, |store| { store.add_build_log(path, source) }).boxed_result()
    }

    fn add_perm_root<'a>(
        &'a mut self,
        path: &'a crate::store_path::StorePath,
        gc_root: &'a super::DaemonPath,
    ) -> impl ResultLog<Output = super::DaemonResult<super::DaemonPath>> + Send + 'a {
        mutex_result!(self, |store| { store.add_perm_root(path, gc_root) })
    }

    fn sync_with_gc(&mut self) -> impl ResultLog<Output = super::DaemonResult<()>> + Send + '_ {
        mutex_result!(self, |store| { store.sync_with_gc() })
    }

    fn query_derivation_outputs<'a>(
        &'a mut self,
        path: &'a crate::store_path::StorePath,
    ) -> impl ResultLog<Output = super::DaemonResult<crate::store_path::StorePathSet>> + Send + 'a
    {
        mutex_result!(self, |store| { store.query_derivation_outputs(path) })
    }

    fn query_derivation_output_names<'a>(
        &'a mut self,
        path: &'a crate::store_path::StorePath,
    ) -> impl ResultLog<
        Output = super::DaemonResult<std::collections::BTreeSet<crate::derived_path::OutputName>>,
    > + Send
    + 'a {
        mutex_result!(self, |store| { store.query_derivation_output_names(path) })
    }

    fn add_ca_to_store<'a, 'r, R>(
        &'a mut self,
        name: &'a str,
        cam: crate::store_path::ContentAddressMethodAlgorithm,
        refs: &'a crate::store_path::StorePathSet,
        repair: bool,
        source: R,
    ) -> Pin<
        Box<
            dyn ResultLog<Output = super::DaemonResult<super::wire::types2::ValidPathInfo>>
                + Send
                + 'r,
        >,
    >
    where
        R: tokio::io::AsyncBufRead + Send + Unpin + 'r,
        'a: 'r,
    {
        mutex_result!(self, |store| {
            store.add_ca_to_store(name, cam, refs, repair, source)
        })
        .boxed_result()
    }
}
