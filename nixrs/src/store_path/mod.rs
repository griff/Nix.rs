use std::collections::BTreeSet;

mod content_address;
mod create;
mod full_store_path;
mod macros;
mod path;
mod store_dir;
mod store_path_hash;
mod store_path_name;

pub use content_address::{ContentAddress, ContentAddressMethod, ContentAddressMethodAlgorithm};
pub use full_store_path::FullStorePath;
pub use path::{ParseStorePathError, StorePath, StorePathError};
pub use store_dir::{FromStoreDirStr, HasStoreDir, StoreDir, StoreDirDisplay};
pub use store_path_hash::{StorePathHash, StorePathHashError};
pub use store_path_name::{StorePathName, StorePathNameError, StorePathNameRef};

pub type StorePathSet = BTreeSet<StorePath>;

/// Convert a possible ASCII character to a string for display.
fn display_symbol(symbol: u8) -> String {
    if symbol < 127 {
        let ch = char::from_u32(symbol as u32).expect("ASCII is always a valid char");
        format!("'{ch}'")
    } else {
        format!("\\x{symbol:X}")
    }
}

#[cfg(test)]
mod unittests {
    use super::*;

    #[test]
    fn test_display_symbol() {
        assert_eq!("'d'", display_symbol(b'd'));
        assert_eq!("\\xFF", display_symbol(255));
    }
}
