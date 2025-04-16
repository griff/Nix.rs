use std::future::ready;

use futures::stream::empty;

use super::{
    logger::ResultProcess, DaemonResult, DaemonResultExt as _, DaemonStore, HandshakeDaemonStore,
};

#[derive(Debug)]
pub struct FailStore;

impl HandshakeDaemonStore for FailStore {
    type Store = Self;

    fn handshake(self) -> impl super::ResultLog<Output = DaemonResult<Self::Store>> {
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
        _drvs: &'a [super::wire::types2::DerivedPath],
        _mode: super::wire::types2::BuildMode,
    ) -> impl super::ResultLog<Output = DaemonResult<Vec<super::wire::types2::KeyedBuildResult>>>
           + Send
           + 'a {
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
    ) -> impl super::ResultLog<Output = DaemonResult<crate::store_path::StorePathSet>> + Send + '_
    {
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
}
