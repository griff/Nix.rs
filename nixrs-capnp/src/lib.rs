pub mod capnp;
mod client;
mod convert;
mod server;
mod stream;

pub use client::{CapnpStore, LoggedCapnpStore};
pub use server::{CapnpServer, HandshakeLoggedCapnpServer, LoggedCapnpServer};
pub use stream::{ByteStreamWrap, ByteStreamWriter};

pub const DEFAULT_BUF_SIZE: usize = 32 * 1024;

pub fn from_error<E: ToString>(err: E) -> ::capnp::Error {
    ::capnp::Error::failed(err.to_string())
}
