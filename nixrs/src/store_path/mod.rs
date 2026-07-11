use std::collections::BTreeSet;

mod content_address;
mod create;
mod full_store_path;
mod path;
mod store_dir;

pub use content_address::{ContentAddress, ContentAddressMethod, ContentAddressMethodAlgorithm};
pub use full_store_path::FullStorePath;
pub(crate) use path::into_name;
pub use path::{
    MAX_NAME_LEN, ParseStorePathError, StorePath, StorePathError, StorePathHash, StorePathName,
    StorePathNameError,
};
pub use store_dir::{FromStoreDirStr, HasStoreDir, StoreDir, StoreDirDisplay};

pub type StorePathSet = BTreeSet<StorePath>;
