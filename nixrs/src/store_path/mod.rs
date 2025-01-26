use std::collections::BTreeSet;

mod path;
mod store_dir;

pub use path::{StorePath, StorePathError, StorePathHash, StorePathName, ParseStorePathError};
pub use store_dir::{FromStoreDirStr, StoreDir, StoreDirDisplay};

pub type StorePathSet = BTreeSet<StorePath>;

#[cfg(any(test, feature = "test"))]
pub mod proptest {
    pub use super::path::proptest::*;
    pub use super::store_dir::proptest::*;
}