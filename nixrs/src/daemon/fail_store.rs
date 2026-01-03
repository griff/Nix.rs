use std::future::ready;

use crate::daemon::FutureResultExt;
use crate::store_path::{HasStoreDir, StoreDir};

use super::{DaemonResult, DaemonStore, HandshakeDaemonStore, ResultLog};

#[derive(Debug)]
pub struct FailStore(StoreDir);

impl HasStoreDir for FailStore {
    fn store_dir(&self) -> &StoreDir {
        &self.0
    }
}
impl HandshakeDaemonStore for FailStore {
    type Store = Self;

    fn handshake(self) -> impl ResultLog<Output = DaemonResult<Self::Store>> {
        ready(Ok(self)).empty_logs()
    }
}

impl DaemonStore for FailStore {
    fn trust_level(&self) -> super::TrustLevel {
        super::TrustLevel::Unknown
    }

    fn shutdown(&mut self) -> impl ResultLog<Output = DaemonResult<()>> {
        ready(Ok(())).empty_logs()
    }
}
