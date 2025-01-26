use std::collections::BTreeSet;

mod path;
mod store_dir;

pub use path::{ParseStorePathError, StorePath, StorePathError, StorePathHash, StorePathName};
pub use store_dir::{FromStoreDirStr, StoreDir, StoreDirDisplay};

pub type StorePathSet = BTreeSet<StorePath>;

#[cfg(any(test, feature = "test"))]
pub mod proptest {
    pub use super::path::proptest::*;
    pub use super::store_dir::proptest::*;
}
