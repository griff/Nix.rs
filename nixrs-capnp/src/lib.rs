pub mod capnp;
mod client;
mod convert;
mod server;
mod stream;

pub use client::CapnpStore;
pub use server::CapnpServer;
pub use stream::{ByteStreamWrap, ByteStreamWriter};

pub const DEFAULT_BUF_SIZE: usize = 32 * 1024;
