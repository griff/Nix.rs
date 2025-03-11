mod async_bytes_read;
mod bytes_reader;
#[cfg(feature = "nixrs-derive")]
mod compat;
mod read_u64;
#[cfg(feature = "nixrs-derive")]
mod taken;
mod try_read_bytes_limited;

pub use async_bytes_read::AsyncBytesRead;
pub use bytes_reader::{BytesReader, DEFAULT_MAX_BUF_SIZE, DEFAULT_RESERVED_BUF_SIZE};
#[cfg(feature = "nixrs-derive")]
pub use compat::AsyncBufReadCompat;
pub use read_u64::TryReadU64;
#[cfg(feature = "nixrs-derive")]
pub use taken::TakenReader;
pub use try_read_bytes_limited::TryReadBytesLimited;
