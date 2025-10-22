use std::collections::{BTreeMap, BTreeSet};
use std::future::{Future, ready};

use futures::Stream;
use tokio::io::AsyncBufRead;

use crate::daemon::wire::types2::{
    BuildMode, BuildResult, CollectGarbageResponse, GCAction, KeyedBuildResult, QueryMissingResult,
    ValidPathInfo,
};
use crate::daemon::{
    AddToStoreItem, ClientOptions, DaemonError, DaemonPath, DaemonResult, FutureResultExt,
    ResultLog, TrustLevel, UnkeyedValidPathInfo,
};
use crate::derivation::BasicDerivation;
use crate::derived_path::{DerivedPath, OutputName};
use crate::realisation::{DrvOutput, Realisation};
use crate::signature::Signature;
use crate::store_path::{
    ContentAddressMethodAlgorithm, HasStoreDir, StorePath, StorePathHash, StorePathSet,
};

use super::wire::types::Operation;

pub trait LocalHandshakeDaemonStore: HasStoreDir {
    type Store: LocalDaemonStore;
    fn handshake(self) -> impl ResultLog<Output = DaemonResult<Self::Store>>;
}

#[allow(unused_variables)]
pub trait LocalDaemonStore: HasStoreDir {
    fn trust_level(&self) -> TrustLevel;

    /// Sets options on server.
    /// This is usually called by the client just after the handshake to set
    /// options for the rest of the session.
    fn set_options<'a>(
        &'a mut self,
        options: &'a ClientOptions,
    ) -> impl ResultLog<Output = DaemonResult<()>> + 'a {
        ready(Err(DaemonError::unimplemented(Operation::SetOptions))).empty_logs()
    }

    fn is_valid_path<'a>(
        &'a mut self,
        path: &'a StorePath,
    ) -> impl ResultLog<Output = DaemonResult<bool>> + 'a {
        ready(Err(DaemonError::unimplemented(Operation::IsValidPath))).empty_logs()
    }

    fn query_valid_paths<'a>(
        &'a mut self,
        paths: &'a StorePathSet,
        substitute: bool,
    ) -> impl ResultLog<Output = DaemonResult<StorePathSet>> + 'a {
        ready(Err(DaemonError::unimplemented(Operation::QueryValidPaths))).empty_logs()
    }

    fn query_path_info<'a>(
        &'a mut self,
        path: &'a StorePath,
    ) -> impl ResultLog<Output = DaemonResult<Option<UnkeyedValidPathInfo>>> + 'a {
        ready(Err(DaemonError::unimplemented(Operation::QueryPathInfo))).empty_logs()
    }

    fn nar_from_path<'s>(
        &'s mut self,
        path: &'s StorePath,
    ) -> impl ResultLog<Output = DaemonResult<impl AsyncBufRead + use<Self>>> + 's {
        ready(Err(DaemonError::unimplemented(Operation::NarFromPath)) as Result<&[u8], DaemonError>)
            .empty_logs()
    }

    fn build_paths<'a>(
        &'a mut self,
        paths: &'a [DerivedPath],
        mode: BuildMode,
    ) -> impl ResultLog<Output = DaemonResult<()>> + 'a {
        ready(Err(DaemonError::unimplemented(Operation::BuildPaths))).empty_logs()
    }

    fn build_paths_with_results<'a>(
        &'a mut self,
        drvs: &'a [DerivedPath],
        mode: BuildMode,
    ) -> impl ResultLog<Output = DaemonResult<Vec<KeyedBuildResult>>> + 'a {
        ready(Err(DaemonError::unimplemented(
            Operation::BuildPathsWithResults,
        )))
        .empty_logs()
    }

    fn build_derivation<'a>(
        &'a mut self,
        drv: &'a BasicDerivation,
        mode: BuildMode,
    ) -> impl ResultLog<Output = DaemonResult<BuildResult>> + 'a {
        ready(Err(DaemonError::unimplemented(Operation::BuildDerivation))).empty_logs()
    }

    fn query_missing<'a>(
        &'a mut self,
        paths: &'a [DerivedPath],
    ) -> impl ResultLog<Output = DaemonResult<QueryMissingResult>> + 'a {
        ready(Err(DaemonError::unimplemented(Operation::QueryMissing))).empty_logs()
    }

    fn add_to_store_nar<'s, 'r, 'i, R>(
        &'s mut self,
        info: &'i ValidPathInfo,
        source: R,
        repair: bool,
        dont_check_sigs: bool,
    ) -> impl ResultLog<Output = DaemonResult<()>> + 'r
    where
        R: AsyncBufRead + Unpin + 'r,
        's: 'r,
        'i: 'r,
    {
        ready(Err(DaemonError::unimplemented(Operation::AddToStoreNar))).empty_logs()
    }

    fn add_multiple_to_store<'s, 'i, 'r, S, R>(
        &'s mut self,
        repair: bool,
        dont_check_sigs: bool,
        stream: S,
    ) -> impl ResultLog<Output = DaemonResult<()>> + 'r
    where
        S: Stream<Item = Result<AddToStoreItem<R>, DaemonError>> + 'i,
        R: AsyncBufRead + Unpin + 'i,
        's: 'r,
        'i: 'r,
    {
        ready(Err(DaemonError::unimplemented(
            Operation::AddMultipleToStore,
        )))
        .empty_logs()
    }

    fn query_all_valid_paths(
        &mut self,
    ) -> impl ResultLog<Output = DaemonResult<StorePathSet>> + '_ {
        ready(Err(DaemonError::unimplemented(
            Operation::QueryAllValidPaths,
        )))
        .empty_logs()
    }

    fn query_referrers<'a>(
        &'a mut self,
        path: &'a StorePath,
    ) -> impl ResultLog<Output = DaemonResult<StorePathSet>> + 'a {
        ready(Err(DaemonError::unimplemented(Operation::QueryReferrers))).empty_logs()
    }

    fn ensure_path<'a>(
        &'a mut self,
        path: &'a StorePath,
    ) -> impl ResultLog<Output = DaemonResult<()>> + 'a {
        ready(Err(DaemonError::unimplemented(Operation::EnsurePath))).empty_logs()
    }

    fn add_temp_root<'a>(
        &'a mut self,
        path: &'a StorePath,
    ) -> impl ResultLog<Output = DaemonResult<()>> + 'a {
        ready(Err(DaemonError::unimplemented(Operation::AddTempRoot))).empty_logs()
    }

    fn add_indirect_root<'a>(
        &'a mut self,
        path: &'a DaemonPath,
    ) -> impl ResultLog<Output = DaemonResult<()>> + 'a {
        ready(Err(DaemonError::unimplemented(Operation::AddIndirectRoot))).empty_logs()
    }

    fn find_roots(
        &mut self,
    ) -> impl ResultLog<Output = DaemonResult<BTreeMap<DaemonPath, StorePath>>> + '_ {
        ready(Err(DaemonError::unimplemented(Operation::FindRoots))).empty_logs()
    }

    fn collect_garbage<'a>(
        &'a mut self,
        action: GCAction,
        paths_to_delete: &'a StorePathSet,
        ignore_liveness: bool,
        max_freed: u64,
    ) -> impl ResultLog<Output = DaemonResult<CollectGarbageResponse>> + 'a {
        ready(Err(DaemonError::unimplemented(Operation::CollectGarbage))).empty_logs()
    }

    fn query_path_from_hash_part<'a>(
        &'a mut self,
        hash: &'a StorePathHash,
    ) -> impl ResultLog<Output = DaemonResult<Option<StorePath>>> + 'a {
        ready(Err(DaemonError::unimplemented(
            Operation::QueryPathFromHashPart,
        )))
        .empty_logs()
    }

    fn query_substitutable_paths<'a>(
        &'a mut self,
        paths: &'a StorePathSet,
    ) -> impl ResultLog<Output = DaemonResult<StorePathSet>> + 'a {
        ready(Err(DaemonError::unimplemented(
            Operation::QuerySubstitutablePaths,
        )))
        .empty_logs()
    }

    fn query_valid_derivers<'a>(
        &'a mut self,
        path: &'a StorePath,
    ) -> impl ResultLog<Output = DaemonResult<StorePathSet>> + 'a {
        ready(Err(DaemonError::unimplemented(
            Operation::QueryValidDerivers,
        )))
        .empty_logs()
    }

    fn optimise_store(&mut self) -> impl ResultLog<Output = DaemonResult<()>> + '_ {
        ready(Err(DaemonError::unimplemented(Operation::OptimiseStore))).empty_logs()
    }

    fn verify_store(
        &mut self,
        check_contents: bool,
        repair: bool,
    ) -> impl ResultLog<Output = DaemonResult<bool>> + '_ {
        ready(Err(DaemonError::unimplemented(Operation::VerifyStore))).empty_logs()
    }

    fn add_signatures<'a>(
        &'a mut self,
        path: &'a StorePath,
        signatures: &'a [Signature],
    ) -> impl ResultLog<Output = DaemonResult<()>> + 'a {
        ready(Err(DaemonError::unimplemented(Operation::AddSignatures))).empty_logs()
    }

    fn query_derivation_output_map<'a>(
        &'a mut self,
        path: &'a StorePath,
    ) -> impl ResultLog<Output = DaemonResult<BTreeMap<OutputName, Option<StorePath>>>> + 'a {
        ready(Err(DaemonError::unimplemented(
            Operation::QueryDerivationOutputMap,
        )))
        .empty_logs()
    }

    fn register_drv_output<'a>(
        &'a mut self,
        realisation: &'a Realisation,
    ) -> impl ResultLog<Output = DaemonResult<()>> + 'a {
        ready(Err(DaemonError::unimplemented(
            Operation::RegisterDrvOutput,
        )))
        .empty_logs()
    }

    fn query_realisation<'a>(
        &'a mut self,
        output_id: &'a DrvOutput,
    ) -> impl ResultLog<Output = DaemonResult<BTreeSet<Realisation>>> + 'a {
        ready(Err(DaemonError::unimplemented(Operation::QueryRealisation))).empty_logs()
    }

    fn add_build_log<'s, 'r, 'p, R>(
        &'s mut self,
        path: &'p StorePath,
        source: R,
    ) -> impl ResultLog<Output = DaemonResult<()>> + 'r
    where
        R: AsyncBufRead + Unpin + 'r,
        's: 'r,
        'p: 'r,
    {
        ready(Err(DaemonError::unimplemented(Operation::AddBuildLog))).empty_logs()
    }

    fn add_perm_root<'a>(
        &'a mut self,
        path: &'a StorePath,
        gc_root: &'a DaemonPath,
    ) -> impl ResultLog<Output = DaemonResult<DaemonPath>> + 'a {
        ready(Err(DaemonError::unimplemented(Operation::AddPermRoot))).empty_logs()
    }

    fn sync_with_gc(&mut self) -> impl ResultLog<Output = DaemonResult<()>> + '_ {
        ready(Err(DaemonError::unimplemented(Operation::SyncWithGC))).empty_logs()
    }

    fn query_derivation_outputs<'a>(
        &'a mut self,
        path: &'a StorePath,
    ) -> impl ResultLog<Output = DaemonResult<StorePathSet>> + 'a {
        ready(Err(DaemonError::unimplemented(
            Operation::QueryDerivationOutputs,
        )))
        .empty_logs()
    }

    fn query_derivation_output_names<'a>(
        &'a mut self,
        path: &'a StorePath,
    ) -> impl ResultLog<Output = DaemonResult<BTreeSet<OutputName>>> + 'a {
        ready(Err(DaemonError::unimplemented(
            Operation::QueryDerivationOutputNames,
        )))
        .empty_logs()
    }

    fn add_ca_to_store<'a, 'r, R>(
        &'a mut self,
        name: &'a str,
        cam: ContentAddressMethodAlgorithm,
        refs: &'a StorePathSet,
        repair: bool,
        source: R,
    ) -> impl ResultLog<Output = DaemonResult<ValidPathInfo>> + 'r
    where
        R: AsyncBufRead + Unpin + 'r,
        'a: 'r,
    {
        ready(Err(DaemonError::unimplemented(Operation::AddToStore))).empty_logs()
    }

    fn shutdown(&mut self) -> impl Future<Output = DaemonResult<()>> + '_;
}

#[forbid(clippy::missing_trait_methods)]
impl<'bs, S> LocalDaemonStore for &'bs mut S
where
    S: LocalDaemonStore,
{
    fn trust_level(&self) -> TrustLevel {
        (**self).trust_level()
    }

    fn set_options<'a>(
        &'a mut self,
        options: &'a ClientOptions,
    ) -> impl ResultLog<Output = DaemonResult<()>> + 'a {
        (**self).set_options(options)
    }

    fn is_valid_path<'a>(
        &'a mut self,
        path: &'a StorePath,
    ) -> impl ResultLog<Output = DaemonResult<bool>> + 'a {
        (**self).is_valid_path(path)
    }

    fn query_valid_paths<'a>(
        &'a mut self,
        paths: &'a StorePathSet,
        substitute: bool,
    ) -> impl ResultLog<Output = DaemonResult<StorePathSet>> + 'a {
        (**self).query_valid_paths(paths, substitute)
    }

    fn query_path_info<'a>(
        &'a mut self,
        path: &'a StorePath,
    ) -> impl ResultLog<Output = DaemonResult<Option<UnkeyedValidPathInfo>>> + 'a {
        (**self).query_path_info(path)
    }

    fn nar_from_path<'s>(
        &'s mut self,
        path: &'s StorePath,
    ) -> impl ResultLog<Output = DaemonResult<impl AsyncBufRead + use<'bs, S>>> + 's {
        (**self).nar_from_path(path)
    }

    fn build_paths<'a>(
        &'a mut self,
        paths: &'a [DerivedPath],
        mode: BuildMode,
    ) -> impl ResultLog<Output = DaemonResult<()>> + 'a {
        (**self).build_paths(paths, mode)
    }

    fn build_derivation<'a>(
        &'a mut self,
        drv: &'a BasicDerivation,
        mode: BuildMode,
    ) -> impl ResultLog<Output = DaemonResult<BuildResult>> + 'a {
        (**self).build_derivation(drv, mode)
    }

    fn query_missing<'a>(
        &'a mut self,
        paths: &'a [DerivedPath],
    ) -> impl ResultLog<Output = DaemonResult<QueryMissingResult>> + 'a {
        (**self).query_missing(paths)
    }

    fn add_to_store_nar<'s, 'r, 'i, R>(
        &'s mut self,
        info: &'i ValidPathInfo,
        source: R,
        repair: bool,
        dont_check_sigs: bool,
    ) -> impl ResultLog<Output = DaemonResult<()>> + 'r
    where
        R: AsyncBufRead + Unpin + 'r,
        's: 'r,
        'i: 'r,
    {
        (**self).add_to_store_nar(info, source, repair, dont_check_sigs)
    }

    fn add_multiple_to_store<'s, 'i, 'r, I, R>(
        &'s mut self,
        repair: bool,
        dont_check_sigs: bool,
        stream: I,
    ) -> impl ResultLog<Output = DaemonResult<()>> + 'r
    where
        I: Stream<Item = Result<AddToStoreItem<R>, DaemonError>> + 'i,
        R: AsyncBufRead + Unpin + 'i,
        's: 'r,
        'i: 'r,
    {
        (**self).add_multiple_to_store(repair, dont_check_sigs, stream)
    }

    fn build_paths_with_results<'a>(
        &'a mut self,
        drvs: &'a [DerivedPath],
        mode: BuildMode,
    ) -> impl ResultLog<Output = DaemonResult<Vec<KeyedBuildResult>>> + 'a {
        (**self).build_paths_with_results(drvs, mode)
    }

    fn query_all_valid_paths(
        &mut self,
    ) -> impl ResultLog<Output = DaemonResult<StorePathSet>> + '_ {
        (**self).query_all_valid_paths()
    }

    fn query_referrers<'a>(
        &'a mut self,
        path: &'a StorePath,
    ) -> impl ResultLog<Output = DaemonResult<StorePathSet>> + 'a {
        (**self).query_referrers(path)
    }

    fn ensure_path<'a>(
        &'a mut self,
        path: &'a StorePath,
    ) -> impl ResultLog<Output = DaemonResult<()>> + 'a {
        (**self).ensure_path(path)
    }

    fn add_temp_root<'a>(
        &'a mut self,
        path: &'a StorePath,
    ) -> impl ResultLog<Output = DaemonResult<()>> + 'a {
        (**self).add_temp_root(path)
    }

    fn add_indirect_root<'a>(
        &'a mut self,
        path: &'a DaemonPath,
    ) -> impl ResultLog<Output = DaemonResult<()>> + 'a {
        (**self).add_indirect_root(path)
    }

    fn find_roots(
        &mut self,
    ) -> impl ResultLog<Output = DaemonResult<BTreeMap<DaemonPath, StorePath>>> + '_ {
        (**self).find_roots()
    }

    fn collect_garbage<'a>(
        &'a mut self,
        action: GCAction,
        paths_to_delete: &'a StorePathSet,
        ignore_liveness: bool,
        max_freed: u64,
    ) -> impl ResultLog<Output = DaemonResult<CollectGarbageResponse>> + 'a {
        (**self).collect_garbage(action, paths_to_delete, ignore_liveness, max_freed)
    }

    fn query_path_from_hash_part<'a>(
        &'a mut self,
        hash: &'a StorePathHash,
    ) -> impl ResultLog<Output = DaemonResult<Option<StorePath>>> + 'a {
        (**self).query_path_from_hash_part(hash)
    }

    fn query_substitutable_paths<'a>(
        &'a mut self,
        paths: &'a StorePathSet,
    ) -> impl ResultLog<Output = DaemonResult<StorePathSet>> + 'a {
        (**self).query_substitutable_paths(paths)
    }

    fn query_valid_derivers<'a>(
        &'a mut self,
        path: &'a StorePath,
    ) -> impl ResultLog<Output = DaemonResult<StorePathSet>> + 'a {
        (**self).query_valid_derivers(path)
    }

    fn optimise_store(&mut self) -> impl ResultLog<Output = DaemonResult<()>> + '_ {
        (**self).optimise_store()
    }

    fn verify_store(
        &mut self,
        check_contents: bool,
        repair: bool,
    ) -> impl ResultLog<Output = DaemonResult<bool>> + '_ {
        (**self).verify_store(check_contents, repair)
    }

    fn add_signatures<'a>(
        &'a mut self,
        path: &'a StorePath,
        signatures: &'a [Signature],
    ) -> impl ResultLog<Output = DaemonResult<()>> + 'a {
        (**self).add_signatures(path, signatures)
    }

    fn query_derivation_output_map<'a>(
        &'a mut self,
        path: &'a StorePath,
    ) -> impl ResultLog<Output = DaemonResult<BTreeMap<OutputName, Option<StorePath>>>> + 'a {
        (**self).query_derivation_output_map(path)
    }

    fn register_drv_output<'a>(
        &'a mut self,
        realisation: &'a Realisation,
    ) -> impl ResultLog<Output = DaemonResult<()>> + 'a {
        (**self).register_drv_output(realisation)
    }

    fn query_realisation<'a>(
        &'a mut self,
        output_id: &'a DrvOutput,
    ) -> impl ResultLog<Output = DaemonResult<BTreeSet<Realisation>>> + 'a {
        (**self).query_realisation(output_id)
    }

    fn add_build_log<'s, 'r, 'p, R>(
        &'s mut self,
        path: &'p StorePath,
        source: R,
    ) -> impl ResultLog<Output = DaemonResult<()>> + 'r
    where
        R: AsyncBufRead + Unpin + 'r,
        's: 'r,
        'p: 'r,
    {
        (**self).add_build_log(path, source)
    }

    fn add_perm_root<'a>(
        &'a mut self,
        path: &'a StorePath,
        gc_root: &'a DaemonPath,
    ) -> impl ResultLog<Output = DaemonResult<DaemonPath>> + 'a {
        (**self).add_perm_root(path, gc_root)
    }

    fn sync_with_gc(&mut self) -> impl ResultLog<Output = DaemonResult<()>> + '_ {
        (**self).sync_with_gc()
    }

    fn query_derivation_outputs<'a>(
        &'a mut self,
        path: &'a StorePath,
    ) -> impl ResultLog<Output = DaemonResult<StorePathSet>> + 'a {
        (**self).query_derivation_outputs(path)
    }

    fn query_derivation_output_names<'a>(
        &'a mut self,
        path: &'a StorePath,
    ) -> impl ResultLog<Output = DaemonResult<BTreeSet<OutputName>>> + 'a {
        (**self).query_derivation_output_names(path)
    }

    fn add_ca_to_store<'a, 'r, R>(
        &'a mut self,
        name: &'a str,
        cam: ContentAddressMethodAlgorithm,
        refs: &'a StorePathSet,
        repair: bool,
        source: R,
    ) -> impl ResultLog<Output = DaemonResult<ValidPathInfo>> + 'r
    where
        R: AsyncBufRead + Unpin + 'r,
        'a: 'r,
    {
        (**self).add_ca_to_store(name, cam, refs, repair, source)
    }

    fn shutdown(&mut self) -> impl Future<Output = DaemonResult<()>> + '_ {
        (**self).shutdown()
    }
}
