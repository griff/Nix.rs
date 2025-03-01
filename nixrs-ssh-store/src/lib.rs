use std::error::Error as StdError;
use std::fmt;
use std::future::Future;

use self::io::ExtendedDataWrite;
use nixrs::daemon::HandshakeDaemonStore;
use nixrs_legacy::store::legacy_worker::LegacyStore;

mod error;

pub mod io;
pub mod server;

pub trait StoreProvider {
    type Error: StdError + Send + Sync;

    type LegacyStore: LegacyStore + fmt::Debug + Send;
    type LegacyFuture: Future<Output = Result<Option<Self::LegacyStore>, Self::Error>> + Send;

    type DaemonStore: HandshakeDaemonStore + fmt::Debug + Send;
    type DaemonFuture: Future<Output = Result<Option<Self::DaemonStore>, Self::Error>> + Send;

    fn get_legacy_store(&self, stderr: ExtendedDataWrite) -> Self::LegacyFuture;
    fn get_daemon_store(&self) -> Self::DaemonFuture;
}
