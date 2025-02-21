use std::future::ready;

use futures::stream::empty;

use super::{logger::ResultProcess, DaemonStore, HandshakeDaemonStore};

#[derive(Debug)]
pub struct FailStore;

impl HandshakeDaemonStore for FailStore {
    type Store = Self;

    fn handshake(self) -> impl super::ResultLog<Self::Store, super::DaemonError> {
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
}
