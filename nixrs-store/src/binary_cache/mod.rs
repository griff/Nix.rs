pub mod file;
pub mod http;
mod traits;
mod wrap;

pub use self::traits::BinaryCache;
pub use self::wrap::BinaryStoreWrap;