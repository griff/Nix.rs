mod nar_info;
mod valid_path_info;

pub use nar_info::{Compression, NarInfo, ParseNarInfoError};
pub use valid_path_info::{InvalidPathInfo, ValidPathInfo};

#[cfg(any(test, feature = "test"))]
pub use valid_path_info::proptest;
