use std::future::ready;

use futures::stream::empty;
use crate::store_path::{HasStoreDir, StoreDir};

use super::logger::ResultProcess;
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

    async fn shutdown(&mut self) -> DaemonResult<()> {
        Ok(())
    }
}
