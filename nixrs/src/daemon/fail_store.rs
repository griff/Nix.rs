use std::collections::{BTreeMap, BTreeSet};
use std::future::ready;

use futures::stream::empty;
use tokio::io::AsyncBufRead;

use crate::derived_path::{DerivedPath, OutputName};
use crate::realisation::{DrvOutput, Realisation};
use crate::signature::Signature;
use crate::store_path::{ContentAddressMethodAlgorithm, StorePath, StorePathHash, StorePathSet};

use super::logger::ResultProcess;
use super::wire::types2::{CollectGarbageResponse, GCAction, ValidPathInfo};
use super::{
    DaemonPath, DaemonResult, DaemonResultExt as _, DaemonStore, HandshakeDaemonStore, ResultLog,
};

#[derive(Debug)]
pub struct FailStore;

impl HandshakeDaemonStore for FailStore {
    type Store = Self;

    fn handshake(self) -> impl ResultLog<Output = DaemonResult<Self::Store>> {
        ResultProcess {
            stream: empty(),
            result: ready(Ok(self)),
        }
    }
}

impl DaemonStore for FailStore {
    fn trust_level(&self) -> super::TrustLevel {
        super::TrustLevel::Unknown
    }

    fn build_paths_with_results<'a>(
        &'a mut self,
        _drvs: &'a [DerivedPath],
        _mode: super::wire::types2::BuildMode,
    ) -> impl ResultLog<Output = DaemonResult<Vec<super::wire::types2::KeyedBuildResult>>> + Send + 'a
    {
        ResultProcess {
            stream: empty(),
            result: ready(
                Err(super::DaemonErrorKind::UnimplementedOperation(
                    super::wire::types::Operation::BuildPathsWithResults,
                ))
                .with_operation(super::wire::types::Operation::BuildPathsWithResults),
            ),
        }
    }

    fn query_all_valid_paths(
        &mut self,
    ) -> impl ResultLog<Output = DaemonResult<crate::store_path::StorePathSet>> + Send + '_ {
        ResultProcess {
            stream: empty(),
            result: ready(
                Err(super::DaemonErrorKind::UnimplementedOperation(
                    super::wire::types::Operation::QueryAllValidPaths,
                ))
                .with_operation(super::wire::types::Operation::QueryAllValidPaths),
            ),
        }
    }

    fn query_referrers<'a>(
        &'a mut self,
        _path: &'a StorePath,
    ) -> impl ResultLog<Output = DaemonResult<StorePathSet>> + Send + 'a {
        ResultProcess {
            stream: empty(),
            result: ready(
                Err(super::DaemonErrorKind::UnimplementedOperation(
                    super::wire::types::Operation::QueryReferrers,
                ))
                .with_operation(super::wire::types::Operation::QueryReferrers),
            ),
        }
    }

    fn ensure_path<'a>(
        &'a mut self,
        _path: &'a StorePath,
    ) -> impl ResultLog<Output = DaemonResult<()>> + Send + 'a {
        ResultProcess {
            stream: empty(),
            result: ready(
                Err(super::DaemonErrorKind::UnimplementedOperation(
                    super::wire::types::Operation::EnsurePath,
                ))
                .with_operation(super::wire::types::Operation::EnsurePath),
            ),
        }
    }

    fn add_temp_root<'a>(
        &'a mut self,
        _path: &'a StorePath,
    ) -> impl ResultLog<Output = DaemonResult<()>> + Send + 'a {
        ResultProcess {
            stream: empty(),
            result: ready(
                Err(super::DaemonErrorKind::UnimplementedOperation(
                    super::wire::types::Operation::AddTempRoot,
                ))
                .with_operation(super::wire::types::Operation::AddTempRoot),
            ),
        }
    }

    fn add_indirect_root<'a>(
        &'a mut self,
        _path: &'a DaemonPath,
    ) -> impl ResultLog<Output = DaemonResult<()>> + Send + 'a {
        ResultProcess {
            stream: empty(),
            result: ready(
                Err(super::DaemonErrorKind::UnimplementedOperation(
                    super::wire::types::Operation::AddIndirectRoot,
                ))
                .with_operation(super::wire::types::Operation::AddIndirectRoot),
            ),
        }
    }

    fn find_roots(
        &mut self,
    ) -> impl ResultLog<Output = DaemonResult<BTreeMap<DaemonPath, StorePath>>> + Send + '_ {
        ResultProcess {
            stream: empty(),
            result: ready(
                Err(super::DaemonErrorKind::UnimplementedOperation(
                    super::wire::types::Operation::FindRoots,
                ))
                .with_operation(super::wire::types::Operation::FindRoots),
            ),
        }
    }

    fn collect_garbage<'a>(
        &'a mut self,
        _action: GCAction,
        _paths_to_delete: &'a StorePathSet,
        _ignore_liveness: bool,
        _max_freed: u64,
    ) -> impl ResultLog<Output = DaemonResult<CollectGarbageResponse>> + Send + 'a {
        ResultProcess {
            stream: empty(),
            result: ready(
                Err(super::DaemonErrorKind::UnimplementedOperation(
                    super::wire::types::Operation::CollectGarbage,
                ))
                .with_operation(super::wire::types::Operation::CollectGarbage),
            ),
        }
    }

    fn query_path_from_hash_part<'a>(
        &'a mut self,
        _hash: &'a StorePathHash,
    ) -> impl ResultLog<Output = DaemonResult<Option<StorePath>>> + Send + 'a {
        ResultProcess {
            stream: empty(),
            result: ready(
                Err(super::DaemonErrorKind::UnimplementedOperation(
                    super::wire::types::Operation::QueryPathFromHashPart,
                ))
                .with_operation(super::wire::types::Operation::QueryPathFromHashPart),
            ),
        }
    }

    fn query_substitutable_paths<'a>(
        &'a mut self,
        _paths: &'a StorePathSet,
    ) -> impl ResultLog<Output = DaemonResult<StorePathSet>> + Send + 'a {
        ResultProcess {
            stream: empty(),
            result: ready(
                Err(super::DaemonErrorKind::UnimplementedOperation(
                    super::wire::types::Operation::QuerySubstitutablePaths,
                ))
                .with_operation(super::wire::types::Operation::QuerySubstitutablePaths),
            ),
        }
    }

    fn query_valid_derivers<'a>(
        &'a mut self,
        _path: &'a StorePath,
    ) -> impl ResultLog<Output = DaemonResult<StorePathSet>> + Send + 'a {
        ResultProcess {
            stream: empty(),
            result: ready(
                Err(super::DaemonErrorKind::UnimplementedOperation(
                    super::wire::types::Operation::QueryValidDerivers,
                ))
                .with_operation(super::wire::types::Operation::QueryValidDerivers),
            ),
        }
    }

    fn optimise_store(&mut self) -> impl ResultLog<Output = DaemonResult<()>> + Send + '_ {
        ResultProcess {
            stream: empty(),
            result: ready(
                Err(super::DaemonErrorKind::UnimplementedOperation(
                    super::wire::types::Operation::OptimiseStore,
                ))
                .with_operation(super::wire::types::Operation::OptimiseStore),
            ),
        }
    }

    fn verify_store(
        &mut self,
        _check_contents: bool,
        _repair: bool,
    ) -> impl ResultLog<Output = DaemonResult<bool>> + Send + '_ {
        ResultProcess {
            stream: empty(),
            result: ready(
                Err(super::DaemonErrorKind::UnimplementedOperation(
                    super::wire::types::Operation::VerifyStore,
                ))
                .with_operation(super::wire::types::Operation::VerifyStore),
            ),
        }
    }

    fn add_signatures<'a>(
        &'a mut self,
        _path: &'a StorePath,
        _signatures: &'a [Signature],
    ) -> impl ResultLog<Output = DaemonResult<()>> + Send + 'a {
        ResultProcess {
            stream: empty(),
            result: ready(
                Err(super::DaemonErrorKind::UnimplementedOperation(
                    super::wire::types::Operation::AddSignatures,
                ))
                .with_operation(super::wire::types::Operation::AddSignatures),
            ),
        }
    }

    fn query_derivation_output_map<'a>(
        &'a mut self,
        _path: &'a StorePath,
    ) -> impl ResultLog<Output = DaemonResult<BTreeMap<OutputName, Option<StorePath>>>> + Send + 'a
    {
        ResultProcess {
            stream: empty(),
            result: ready(
                Err(super::DaemonErrorKind::UnimplementedOperation(
                    super::wire::types::Operation::QueryDerivationOutputMap,
                ))
                .with_operation(super::wire::types::Operation::QueryDerivationOutputMap),
            ),
        }
    }

    fn register_drv_output<'a>(
        &'a mut self,
        _realisation: &'a Realisation,
    ) -> impl ResultLog<Output = DaemonResult<()>> + Send + 'a {
        ResultProcess {
            stream: empty(),
            result: ready(
                Err(super::DaemonErrorKind::UnimplementedOperation(
                    super::wire::types::Operation::RegisterDrvOutput,
                ))
                .with_operation(super::wire::types::Operation::RegisterDrvOutput),
            ),
        }
    }

    fn query_realisation<'a>(
        &'a mut self,
        _output_id: &'a DrvOutput,
    ) -> impl ResultLog<Output = DaemonResult<BTreeSet<Realisation>>> + Send + 'a {
        ResultProcess {
            stream: empty(),
            result: ready(
                Err(super::DaemonErrorKind::UnimplementedOperation(
                    super::wire::types::Operation::QueryRealisation,
                ))
                .with_operation(super::wire::types::Operation::QueryRealisation),
            ),
        }
    }

    fn add_build_log<'s, 'r, 'p, R>(
        &'s mut self,
        _path: &'p StorePath,
        _source: R,
    ) -> impl ResultLog<Output = DaemonResult<()>> + 'r
    where
        R: AsyncBufRead + Send + Unpin + 'r,
        's: 'r,
        'p: 'r,
    {
        ResultProcess {
            stream: empty(),
            result: ready(
                Err(super::DaemonErrorKind::UnimplementedOperation(
                    super::wire::types::Operation::AddBuildLog,
                ))
                .with_operation(super::wire::types::Operation::AddBuildLog),
            ),
        }
    }

    fn add_perm_root<'a>(
        &'a mut self,
        _path: &'a StorePath,
        _gc_root: &'a DaemonPath,
    ) -> impl ResultLog<Output = DaemonResult<DaemonPath>> + Send + 'a {
        ResultProcess {
            stream: empty(),
            result: ready(
                Err(super::DaemonErrorKind::UnimplementedOperation(
                    super::wire::types::Operation::AddPermRoot,
                ))
                .with_operation(super::wire::types::Operation::AddPermRoot),
            ),
        }
    }

    fn sync_with_gc(&mut self) -> impl ResultLog<Output = DaemonResult<()>> + Send + '_ {
        ResultProcess {
            stream: empty(),
            result: ready(
                Err(super::DaemonErrorKind::UnimplementedOperation(
                    super::wire::types::Operation::SyncWithGC,
                ))
                .with_operation(super::wire::types::Operation::SyncWithGC),
            ),
        }
    }

    fn query_derivation_outputs<'a>(
        &'a mut self,
        _path: &'a StorePath,
    ) -> impl ResultLog<Output = DaemonResult<StorePathSet>> + Send + 'a {
        ResultProcess {
            stream: empty(),
            result: ready(
                Err(super::DaemonErrorKind::UnimplementedOperation(
                    super::wire::types::Operation::QueryDerivationOutputs,
                ))
                .with_operation(super::wire::types::Operation::QueryDerivationOutputs),
            ),
        }
    }

    fn query_derivation_output_names<'a>(
        &'a mut self,
        _path: &'a StorePath,
    ) -> impl ResultLog<Output = DaemonResult<BTreeSet<OutputName>>> + Send + 'a {
        ResultProcess {
            stream: empty(),
            result: ready(
                Err(super::DaemonErrorKind::UnimplementedOperation(
                    super::wire::types::Operation::QueryDerivationOutputNames,
                ))
                .with_operation(super::wire::types::Operation::QueryDerivationOutputNames),
            ),
        }
    }

    fn add_ca_to_store<'a, 'r, R>(
        &'a mut self,
        _name: &'a str,
        _cam: ContentAddressMethodAlgorithm,
        _refs: &'a StorePathSet,
        _repair: bool,
        _source: R,
    ) -> impl ResultLog<Output = DaemonResult<ValidPathInfo>> + Send + 'r
    where
        R: AsyncBufRead + Send + Unpin + 'r,
        'a: 'r,
    {
        ResultProcess {
            stream: empty(),
            result: ready(
                Err(super::DaemonErrorKind::UnimplementedOperation(
                    super::wire::types::Operation::AddToStore,
                ))
                .with_operation(super::wire::types::Operation::AddToStore),
            ),
        }
    }

    async fn shutdown(&mut self) -> DaemonResult<()> {
        Ok(())
    }
}
