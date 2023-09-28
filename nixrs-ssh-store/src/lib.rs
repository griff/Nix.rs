use std::error::Error as StdError;
use std::future::Future;

use self::io::ExtendedDataWrite;
use nixrs_store::legacy_worker::LegacyStore;

mod error;

pub mod io;
pub mod server;

pub trait StoreProvider {
    type Store: LegacyStore + Send;
    type Error: StdError + Send + Sync;
    type Future: Future<Output = Result<Self::Store, Self::Error>>;

    fn get_store(&self, stderr: ExtendedDataWrite) -> Self::Future;
}
