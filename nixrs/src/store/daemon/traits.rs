use std::fmt;

use async_trait::async_trait;
use tokio::io::AsyncRead;
use tracing::warn;

use crate::store::{BuildMode, CheckSignaturesFlag, DerivedPath, Error, RepairFlag, Store};
use crate::store_path::{StorePath, StorePathSet};

use super::TrustedFlag;

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone)]
pub struct QueryMissingResult {
    pub will_build: StorePathSet,
    pub will_substitute: StorePathSet,
    pub unknown: StorePathSet,
    pub download_size: u64,
    pub nar_size: u64,
}

#[async_trait]
pub trait DaemonStore: Store {
    fn is_trusted_client(&self) -> Option<TrustedFlag>;
    async fn set_options(&mut self) -> Result<(), Error>;
    async fn is_valid_path(&mut self, path: &StorePath) -> Result<bool, Error>;

    async fn add_multiple_to_store<R: AsyncRead + fmt::Debug + Send + Unpin>(
        &mut self,
        source: R,
        repair: RepairFlag,
        check_sigs: CheckSignaturesFlag,
    ) -> Result<(), Error>;

    /// Given a set of paths that are to be built, return the set of
    /// derivations that will be built, and the set of output paths that
    /// will be substituted.
    async fn query_missing(&mut self, targets: &[DerivedPath])
        -> Result<QueryMissingResult, Error>;
    async fn substitute_paths(&mut self, paths: &StorePathSet) -> Result<(), Error> {
        let mut paths2 = Vec::new();
        for path in paths {
            if path.is_derivation() {
                paths2.push(DerivedPath::Opaque(path.clone()));
            }
        }
        let res = self.query_missing(&paths2).await?;
        if res.will_substitute.is_empty() {
            let ret = async {
                let mut subs = Vec::new();
                for p in res.will_substitute {
                    subs.push(DerivedPath::Opaque(p));
                }
                self.build_paths(&subs, BuildMode::Normal).await
            }
            .await;
            if let Err(err) = ret {
                warn!("{}", err);
            }
        }
        Ok(())
    }
}
