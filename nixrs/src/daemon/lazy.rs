use std::future::ready;
use std::pin::Pin;

use tokio::io::AsyncBufRead;

use crate::daemon::{
    DaemonError, DaemonResult, DaemonStore, FutureResultExt, HandshakeDaemonStore, HasTrustLevel,
    LocalDaemonStore, LocalHandshakeDaemonStore, ResultLog, ResultLogExt, TrustLevel, make_result,
};
use crate::store_path::{HasStoreDir, StoreDir};

pub fn ready_connection<C>(conn: C) -> ReadyConnection<C>
where
    C: DaemonStore + Send,
{
    ReadyConnection { conn }
}

pub fn ready_local_connection<C>(conn: C) -> ReadyConnection<C>
where
    C: LocalDaemonStore,
{
    ReadyConnection { conn }
}

pub struct ReadyConnection<C> {
    conn: C,
}

impl<C: HasStoreDir> HasStoreDir for ReadyConnection<C> {
    fn store_dir(&self) -> &StoreDir {
        self.conn.store_dir()
    }
}

impl<C> HandshakeDaemonStore for ReadyConnection<C>
where
    C: DaemonStore + Send,
{
    type Store = C;

    fn handshake(self) -> impl ResultLog<Output = DaemonResult<Self::Store>> + Send {
        ready(Ok(self.conn)).empty_logs()
    }
}

impl<C> LocalHandshakeDaemonStore for ReadyConnection<C>
where
    C: LocalDaemonStore,
{
    type Store = C;

    fn handshake(self) -> impl ResultLog<Output = DaemonResult<Self::Store>> {
        ready(Ok(self.conn)).empty_logs()
    }
}

enum Inner<HC, C, R> {
    PreHandshake(HC),
    Handshake(R),
    Connection(DaemonResult<C>),
    Invalid,
}

type LazyResult<C> = Pin<Box<dyn ResultLog<Output = DaemonResult<C>> + Send>>;

pub struct LazyDaemonConnection<HC, C> {
    store_dir: StoreDir,
    inner: Inner<HC, C, LazyResult<C>>,
}

impl<C> LazyDaemonConnection<ReadyConnection<C>, C>
where
    C: DaemonStore,
{
    pub fn with_connection(conn: C) -> Self {
        Self {
            store_dir: conn.store_dir().clone(),
            inner: Inner::Connection(Ok(conn)),
        }
    }
}

impl<C> LazyDaemonConnection<ReadyConnection<C>, C> {
    pub fn with_result<R>(store_dir: StoreDir, result: R) -> Self
    where
        R: ResultLog<Output = DaemonResult<C>> + Send + 'static,
    {
        Self {
            store_dir,
            inner: Inner::Handshake(Box::pin(result)),
        }
    }
}

impl<HC, C> LazyDaemonConnection<HC, C>
where
    HC: HandshakeDaemonStore<Store = C>,
    C: Send,
{
    pub fn with_pre_handshake(pre_handshake: HC) -> Self {
        Self {
            store_dir: pre_handshake.store_dir().clone(),
            inner: Inner::PreHandshake(pre_handshake),
        }
    }

    pub fn connection(&mut self) -> impl ResultLog<Output = DaemonResult<&mut C>> {
        make_result(|logger| async move {
            let next = match std::mem::replace(&mut self.inner, Inner::Invalid) {
                Inner::PreHandshake(pre_handshake) => {
                    Inner::Connection(pre_handshake.handshake().forward_logs(logger).await)
                }
                Inner::Handshake(fut) => Inner::Connection(fut.forward_logs(logger).await),
                e => e,
            };
            self.inner = next;
            match &mut self.inner {
                Inner::PreHandshake(_) => unreachable!(),
                Inner::Handshake(_) => unreachable!(),
                Inner::Invalid => unreachable!(),
                Inner::Connection(Ok(conn)) => Ok(conn),
                Inner::Connection(Err(err)) => Err(err.clone()),
            }
        })
    }
}

impl<HC, C> HasStoreDir for LazyDaemonConnection<HC, C> {
    fn store_dir(&self) -> &StoreDir {
        &self.store_dir
    }
}

impl<HC, C> HandshakeDaemonStore for LazyDaemonConnection<HC, C>
where
    HC: HandshakeDaemonStore<Store = C> + Send,
    C: DaemonStore + Send,
{
    type Store = Self;

    fn handshake(mut self) -> impl ResultLog<Output = DaemonResult<Self::Store>> + Send {
        make_result(|logs| async move {
            self.connection().forward_logs(logs).await?;
            Ok(self)
        })
    }
}

#[forbid(clippy::missing_trait_methods)]
impl<HC, C> HasTrustLevel for LazyDaemonConnection<HC, C>
where
    HC: HandshakeDaemonStore<Store = C> + Send,
    C: HasTrustLevel + Send,
{
    fn trust_level(&self) -> super::TrustLevel {
        if let Inner::Connection(Ok(conn)) = &self.inner {
            conn.trust_level()
        } else {
            TrustLevel::Unknown
        }
    }
}

macro_rules! mutex_result {
    ($self:ident, |$store:ident| { $($stm:tt)* }) => {{
        make_result(move |mut logs| async move {
            let $store = $self.connection().forward_logs(&mut logs).await?;
            { $($stm)* }.forward_logs(&mut logs).await
        })
    }};
}

#[forbid(clippy::missing_trait_methods)]
impl<HC, C> DaemonStore for LazyDaemonConnection<HC, C>
where
    HC: HandshakeDaemonStore<Store = C> + Send,
    C: DaemonStore + Send,
{
    fn shutdown(&mut self) -> impl ResultLog<Output = DaemonResult<()>> + Send + '_ {
        make_result(|mut logs| async move {
            self.connection()
                .forward_logs(&mut logs)
                .await?
                .shutdown()
                .forward_logs(&mut logs)
                .await?;
            eprintln!("Shutting down lazy connection");
            self.inner =
                Inner::Connection(Err(DaemonError::custom("connection has been shut dowh")));
            Ok(())
        })
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
    ) -> impl ResultLog<
        Output = super::DaemonResult<impl tokio::io::AsyncBufRead + Send + use<HC, C>>,
    > + Send
    + 's {
        mutex_result!(self, |store| { store.nar_from_path(path) })
    }

    fn build_paths<'a>(
        &'a mut self,
        drvs: &'a [crate::derived_path::DerivedPath],
        mode: super::BuildMode,
    ) -> impl ResultLog<Output = super::DaemonResult<()>> + Send + 'a {
        mutex_result!(self, |store| { store.build_paths(drvs, mode) })
    }

    fn build_paths_with_results<'a>(
        &'a mut self,
        drvs: &'a [crate::derived_path::DerivedPath],
        mode: super::BuildMode,
    ) -> impl ResultLog<Output = super::DaemonResult<Vec<super::KeyedBuildResult>>> + Send + 'a
    {
        mutex_result!(self, |store| { store.build_paths_with_results(drvs, mode) })
    }

    fn build_derivation<'a>(
        &'a mut self,
        drv: &'a crate::derivation::BasicDerivation,
        mode: super::BuildMode,
    ) -> impl ResultLog<Output = super::DaemonResult<super::BuildResult>> + Send + 'a {
        mutex_result!(self, |store| { store.build_derivation(drv, mode) })
    }

    fn query_missing<'a>(
        &'a mut self,
        paths: &'a [crate::derived_path::DerivedPath],
    ) -> impl ResultLog<Output = super::DaemonResult<super::QueryMissingResult>> + Send + 'a {
        mutex_result!(self, |store| { store.query_missing(paths) })
    }

    fn add_to_store_nar<'s, 'r, 'i, R>(
        &'s mut self,
        info: &'i super::ValidPathInfo,
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
        action: super::GCAction,
        paths_to_delete: &'a crate::store_path::StorePathSet,
        ignore_liveness: bool,
        max_freed: u64,
    ) -> impl ResultLog<Output = super::DaemonResult<super::CollectGarbageResponse>> + Send + 'a
    {
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
                crate::derivation::OutputName,
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
    ) -> impl ResultLog<Output = super::DaemonResult<Option<crate::realisation::Realisation>>> + Send + 'a
    {
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
        Output = super::DaemonResult<std::collections::BTreeSet<crate::derivation::OutputName>>,
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
    ) -> Pin<Box<dyn ResultLog<Output = super::DaemonResult<super::ValidPathInfo>> + Send + 'r>>
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

type LocalLazyResult<C> = Pin<Box<dyn ResultLog<Output = DaemonResult<C>>>>;

pub struct LocalLazyDaemonConnection<HC, C> {
    store_dir: StoreDir,
    inner: Inner<HC, C, LocalLazyResult<C>>,
}

impl<C> LocalLazyDaemonConnection<ReadyConnection<C>, C>
where
    C: LocalDaemonStore,
{
    pub fn with_connection(conn: C) -> Self {
        Self {
            store_dir: conn.store_dir().clone(),
            inner: Inner::Connection(Ok(conn)),
        }
    }

    pub fn with_result<R>(store_dir: StoreDir, result: R) -> Self
    where
        R: ResultLog<Output = DaemonResult<C>> + 'static,
    {
        Self {
            store_dir,
            inner: Inner::Handshake(Box::pin(result)),
        }
    }
}

impl<HC, C> LocalLazyDaemonConnection<HC, C>
where
    HC: LocalHandshakeDaemonStore<Store = C>,
{
    pub fn with_pre_handshake(pre_handshake: HC) -> Self {
        Self {
            store_dir: pre_handshake.store_dir().clone(),
            inner: Inner::PreHandshake(pre_handshake),
        }
    }

    pub fn connection(&mut self) -> impl ResultLog<Output = DaemonResult<&mut C>> {
        make_result(|logger| async move {
            let next = match std::mem::replace(&mut self.inner, Inner::Invalid) {
                Inner::PreHandshake(pre_handshake) => {
                    Inner::Connection(pre_handshake.handshake().forward_logs(logger).await)
                }
                Inner::Handshake(fut) => Inner::Connection(fut.forward_logs(logger).await),
                e => e,
            };
            self.inner = next;
            match &mut self.inner {
                Inner::PreHandshake(_) => unreachable!(),
                Inner::Handshake(_) => unreachable!(),
                Inner::Invalid => unreachable!(),
                Inner::Connection(Ok(conn)) => Ok(conn),
                Inner::Connection(Err(err)) => Err(err.clone()),
            }
        })
    }
}

impl<HC, C> HasStoreDir for LocalLazyDaemonConnection<HC, C> {
    fn store_dir(&self) -> &StoreDir {
        &self.store_dir
    }
}

impl<HC, C> LocalHandshakeDaemonStore for LocalLazyDaemonConnection<HC, C>
where
    HC: LocalHandshakeDaemonStore<Store = C>,
    C: LocalDaemonStore,
{
    type Store = Self;

    fn handshake(mut self) -> impl ResultLog<Output = DaemonResult<Self::Store>> {
        make_result(|logs| async move {
            self.connection().forward_logs(logs).await?;
            Ok(self)
        })
    }
}

#[forbid(clippy::missing_trait_methods)]
impl<HC, C> HasTrustLevel for LocalLazyDaemonConnection<HC, C>
where
    HC: LocalHandshakeDaemonStore<Store = C>,
    C: HasTrustLevel,
{
    fn trust_level(&self) -> super::TrustLevel {
        if let Inner::Connection(Ok(conn)) = &self.inner {
            conn.trust_level()
        } else {
            TrustLevel::Unknown
        }
    }
}

#[forbid(clippy::missing_trait_methods)]
impl<HC, C> LocalDaemonStore for LocalLazyDaemonConnection<HC, C>
where
    HC: LocalHandshakeDaemonStore<Store = C>,
    C: LocalDaemonStore,
{
    fn shutdown(&mut self) -> impl ResultLog<Output = DaemonResult<()>> + '_ {
        make_result(|mut logs| async move {
            self.connection()
                .forward_logs(&mut logs)
                .await?
                .shutdown()
                .forward_logs(&mut logs)
                .await?;
            eprintln!("Shutting down lazy connection");
            self.inner =
                Inner::Connection(Err(DaemonError::custom("connection has been shut dowh")));
            Ok(())
        })
    }

    fn set_options<'r>(
        &'r mut self,
        options: &'r super::ClientOptions,
    ) -> impl ResultLog<Output = super::DaemonResult<()>> + 'r {
        mutex_result!(self, |store| { store.set_options(options) })
    }

    fn is_valid_path<'a>(
        &'a mut self,
        path: &'a crate::store_path::StorePath,
    ) -> impl ResultLog<Output = super::DaemonResult<bool>> + 'a {
        mutex_result!(self, |store| { store.is_valid_path(path) })
    }

    fn query_valid_paths<'a>(
        &'a mut self,
        paths: &'a crate::store_path::StorePathSet,
        substitute: bool,
    ) -> impl ResultLog<Output = super::DaemonResult<crate::store_path::StorePathSet>> + 'a {
        mutex_result!(self, |store| { store.query_valid_paths(paths, substitute) })
    }

    fn query_path_info<'a>(
        &'a mut self,
        path: &'a crate::store_path::StorePath,
    ) -> impl ResultLog<Output = super::DaemonResult<Option<super::UnkeyedValidPathInfo>>> + 'a
    {
        mutex_result!(self, |store| { store.query_path_info(path) })
    }

    fn nar_from_path<'s>(
        &'s mut self,
        path: &'s crate::store_path::StorePath,
    ) -> impl ResultLog<Output = super::DaemonResult<impl tokio::io::AsyncBufRead + use<HC, C>>> + 's
    {
        mutex_result!(self, |store| { store.nar_from_path(path) })
    }

    fn build_paths<'a>(
        &'a mut self,
        drvs: &'a [crate::derived_path::DerivedPath],
        mode: super::BuildMode,
    ) -> impl ResultLog<Output = super::DaemonResult<()>> + 'a {
        mutex_result!(self, |store| { store.build_paths(drvs, mode) })
    }

    fn build_paths_with_results<'a>(
        &'a mut self,
        drvs: &'a [crate::derived_path::DerivedPath],
        mode: super::BuildMode,
    ) -> impl ResultLog<Output = super::DaemonResult<Vec<super::KeyedBuildResult>>> + 'a {
        mutex_result!(self, |store| { store.build_paths_with_results(drvs, mode) })
    }

    fn build_derivation<'a>(
        &'a mut self,
        drv: &'a crate::derivation::BasicDerivation,
        mode: super::BuildMode,
    ) -> impl ResultLog<Output = super::DaemonResult<super::BuildResult>> + 'a {
        mutex_result!(self, |store| { store.build_derivation(drv, mode) })
    }

    fn query_missing<'a>(
        &'a mut self,
        paths: &'a [crate::derived_path::DerivedPath],
    ) -> impl ResultLog<Output = super::DaemonResult<super::QueryMissingResult>> + 'a {
        mutex_result!(self, |store| { store.query_missing(paths) })
    }

    fn add_to_store_nar<'s, 'r, 'i, R>(
        &'s mut self,
        info: &'i super::ValidPathInfo,
        source: R,
        repair: bool,
        dont_check_sigs: bool,
    ) -> impl ResultLog<Output = super::DaemonResult<()>> + 'r
    where
        R: AsyncBufRead + Unpin + 'r,
        's: 'r,
        'i: 'r,
    {
        mutex_result!(self, |store| {
            store.add_to_store_nar(info, source, repair, dont_check_sigs)
        })
    }

    fn add_multiple_to_store<'s, 'i, 'r, SS, R>(
        &'s mut self,
        repair: bool,
        dont_check_sigs: bool,
        stream: SS,
    ) -> impl ResultLog<Output = super::DaemonResult<()>> + 'r
    where
        SS: futures::Stream<Item = Result<super::AddToStoreItem<R>, super::DaemonError>> + 'i,
        R: tokio::io::AsyncBufRead + Unpin + 'i,
        's: 'r,
        'i: 'r,
    {
        mutex_result!(self, |store| {
            store.add_multiple_to_store(repair, dont_check_sigs, stream)
        })
    }

    fn query_all_valid_paths(
        &mut self,
    ) -> impl ResultLog<Output = super::DaemonResult<crate::store_path::StorePathSet>> + '_ {
        mutex_result!(self, |store| { store.query_all_valid_paths() })
    }

    fn query_referrers<'a>(
        &'a mut self,
        path: &'a crate::store_path::StorePath,
    ) -> impl ResultLog<Output = super::DaemonResult<crate::store_path::StorePathSet>> + 'a {
        mutex_result!(self, |store| { store.query_referrers(path) })
    }

    fn ensure_path<'a>(
        &'a mut self,
        path: &'a crate::store_path::StorePath,
    ) -> impl ResultLog<Output = super::DaemonResult<()>> + 'a {
        mutex_result!(self, |store| { store.ensure_path(path) })
    }

    fn add_temp_root<'a>(
        &'a mut self,
        path: &'a crate::store_path::StorePath,
    ) -> impl ResultLog<Output = super::DaemonResult<()>> + 'a {
        mutex_result!(self, |store| { store.add_temp_root(path) })
    }

    fn add_indirect_root<'a>(
        &'a mut self,
        path: &'a super::DaemonPath,
    ) -> impl ResultLog<Output = super::DaemonResult<()>> + 'a {
        mutex_result!(self, |store| { store.add_indirect_root(path) })
    }

    fn find_roots(
        &mut self,
    ) -> impl ResultLog<
        Output = super::DaemonResult<
            std::collections::BTreeMap<super::DaemonPath, crate::store_path::StorePath>,
        >,
    > + '_ {
        mutex_result!(self, |store| { store.find_roots() })
    }

    fn collect_garbage<'a>(
        &'a mut self,
        action: super::GCAction,
        paths_to_delete: &'a crate::store_path::StorePathSet,
        ignore_liveness: bool,
        max_freed: u64,
    ) -> impl ResultLog<Output = super::DaemonResult<super::CollectGarbageResponse>> + 'a {
        mutex_result!(self, |store| {
            store.collect_garbage(action, paths_to_delete, ignore_liveness, max_freed)
        })
    }

    fn query_path_from_hash_part<'a>(
        &'a mut self,
        hash: &'a crate::store_path::StorePathHash,
    ) -> impl ResultLog<Output = super::DaemonResult<Option<crate::store_path::StorePath>>> + 'a
    {
        mutex_result!(self, |store| { store.query_path_from_hash_part(hash) })
    }

    fn query_substitutable_paths<'a>(
        &'a mut self,
        paths: &'a crate::store_path::StorePathSet,
    ) -> impl ResultLog<Output = super::DaemonResult<crate::store_path::StorePathSet>> + 'a {
        mutex_result!(self, |store| { store.query_substitutable_paths(paths) })
    }

    fn query_valid_derivers<'a>(
        &'a mut self,
        path: &'a crate::store_path::StorePath,
    ) -> impl ResultLog<Output = super::DaemonResult<crate::store_path::StorePathSet>> + 'a {
        mutex_result!(self, |store| { store.query_valid_derivers(path) })
    }

    fn optimise_store(&mut self) -> impl ResultLog<Output = super::DaemonResult<()>> + '_ {
        mutex_result!(self, |store| { store.optimise_store() })
    }

    fn verify_store(
        &mut self,
        check_contents: bool,
        repair: bool,
    ) -> impl ResultLog<Output = super::DaemonResult<bool>> + '_ {
        mutex_result!(self, |store| { store.verify_store(check_contents, repair) })
    }

    fn add_signatures<'a>(
        &'a mut self,
        path: &'a crate::store_path::StorePath,
        signatures: &'a [crate::signature::Signature],
    ) -> impl ResultLog<Output = super::DaemonResult<()>> + 'a {
        mutex_result!(self, |store| { store.add_signatures(path, signatures) })
    }

    fn query_derivation_output_map<'a>(
        &'a mut self,
        path: &'a crate::store_path::StorePath,
    ) -> impl ResultLog<
        Output = super::DaemonResult<
            std::collections::BTreeMap<
                crate::derivation::OutputName,
                Option<crate::store_path::StorePath>,
            >,
        >,
    > + 'a {
        mutex_result!(self, |store| { store.query_derivation_output_map(path) })
    }

    fn register_drv_output<'a>(
        &'a mut self,
        realisation: &'a crate::realisation::Realisation,
    ) -> impl ResultLog<Output = super::DaemonResult<()>> + 'a {
        mutex_result!(self, |store| { store.register_drv_output(realisation) })
    }

    fn query_realisation<'a>(
        &'a mut self,
        output_id: &'a crate::realisation::DrvOutput,
    ) -> impl ResultLog<Output = super::DaemonResult<Option<crate::realisation::Realisation>>> + 'a
    {
        mutex_result!(self, |store| { store.query_realisation(output_id) })
    }

    fn add_build_log<'s, 'r, 'p, R>(
        &'s mut self,
        path: &'p crate::store_path::StorePath,
        source: R,
    ) -> impl ResultLog<Output = super::DaemonResult<()>> + 'r
    where
        R: tokio::io::AsyncBufRead + Unpin + 'r,
        's: 'r,
        'p: 'r,
    {
        mutex_result!(self, |store| { store.add_build_log(path, source) })
    }

    fn add_perm_root<'a>(
        &'a mut self,
        path: &'a crate::store_path::StorePath,
        gc_root: &'a super::DaemonPath,
    ) -> impl ResultLog<Output = super::DaemonResult<super::DaemonPath>> + 'a {
        mutex_result!(self, |store| { store.add_perm_root(path, gc_root) })
    }

    fn sync_with_gc(&mut self) -> impl ResultLog<Output = super::DaemonResult<()>> + '_ {
        mutex_result!(self, |store| { store.sync_with_gc() })
    }

    fn query_derivation_outputs<'a>(
        &'a mut self,
        path: &'a crate::store_path::StorePath,
    ) -> impl ResultLog<Output = super::DaemonResult<crate::store_path::StorePathSet>> + 'a {
        mutex_result!(self, |store| { store.query_derivation_outputs(path) })
    }

    fn query_derivation_output_names<'a>(
        &'a mut self,
        path: &'a crate::store_path::StorePath,
    ) -> impl ResultLog<
        Output = super::DaemonResult<std::collections::BTreeSet<crate::derivation::OutputName>>,
    > + 'a {
        mutex_result!(self, |store| { store.query_derivation_output_names(path) })
    }

    fn add_ca_to_store<'a, 'r, R>(
        &'a mut self,
        name: &'a str,
        cam: crate::store_path::ContentAddressMethodAlgorithm,
        refs: &'a crate::store_path::StorePathSet,
        repair: bool,
        source: R,
    ) -> impl ResultLog<Output = super::DaemonResult<super::ValidPathInfo>> + 'r
    where
        R: tokio::io::AsyncBufRead + Unpin + 'r,
        'a: 'r,
    {
        mutex_result!(self, |store| {
            store.add_ca_to_store(name, cam, refs, repair, source)
        })
    }
}
