pub mod builder;
pub mod client;
mod graceful;
pub mod serve;
pub mod stream;
pub mod byte_stream_capnp {
    include!(concat!(env!("OUT_DIR"), "/byte_stream_capnp.rs"));
}

mod private {
    pub trait Sealed {}
}
