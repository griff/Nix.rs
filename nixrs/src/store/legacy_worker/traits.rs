use std::fmt;

use async_trait::async_trait;
use tokio::io::{AsyncRead, AsyncWrite};

use crate::store::{Error, Store, SubstituteFlag};
use crate::store_path::StorePathSet;

#[async_trait]
pub trait LegacyStore: Store {
    /*
    async fn query_path_infos(
        &mut self,
        paths: &StorePathSet
    ) -> Result<BTreeSet<ValidPathInfo>, Error>;
     */
    async fn query_valid_paths_locked(
        &mut self,
        paths: &StorePathSet,
        lock: bool,
        maybe_substitute: SubstituteFlag,
    ) -> Result<StorePathSet, Error>;
    async fn export_paths<SW: AsyncWrite + fmt::Debug + Send + Unpin>(
        &mut self,
        paths: &StorePathSet,
        mut sink: SW,
    ) -> Result<(), Error>;
    async fn import_paths<SR: AsyncRead + fmt::Debug + Send + Unpin>(
        &mut self,
        mut source: SR,
    ) -> Result<(), Error>;
    async fn query_closure(
        &mut self,
        paths: &StorePathSet,
        include_outputs: bool,
    ) -> Result<StorePathSet, Error>;
}
