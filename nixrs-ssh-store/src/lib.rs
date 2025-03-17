use std::error::Error as StdError;
use std::fmt;
use std::future::Future;

#[cfg(feature = "legacy")]
use self::io::ExtendedDataWrite;
use nixrs::daemon::HandshakeDaemonStore;
#[cfg(feature = "legacy")]
use nixrs_legacy::store::legacy_worker::LegacyStore;

mod error;

pub mod io;
pub mod server;

pub trait StoreProvider {
    type Error: StdError + Send + Sync;

    #[cfg(feature = "legacy")]
    type LegacyStore: LegacyStore + fmt::Debug + Send;
    #[cfg(feature = "legacy")]
    type LegacyFuture: Future<Output = Result<Option<Self::LegacyStore>, Self::Error>> + Send;

    type DaemonStore: HandshakeDaemonStore + fmt::Debug + Send;
    type DaemonFuture: Future<Output = Result<Option<Self::DaemonStore>, Self::Error>> + Send;

    #[cfg(feature = "legacy")]
    fn get_legacy_store(&self, stderr: ExtendedDataWrite) -> Self::LegacyFuture;
    fn get_daemon_store(&self) -> Self::DaemonFuture;
}
