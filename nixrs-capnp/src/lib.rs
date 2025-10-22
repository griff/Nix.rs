pub mod capnp;
pub mod convert;
pub mod lookup;
pub mod nix_daemon;
pub mod nixrs;

pub const DEFAULT_BUF_SIZE: usize = 32 * 1024;

pub fn from_error<E: ToString>(err: E) -> ::capnp::Error {
    ::capnp::Error::failed(err.to_string())
}
