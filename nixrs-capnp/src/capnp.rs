#![allow(clippy::needless_lifetimes, clippy::extra_unused_type_parameters)]

pub mod byte_stream_capnp {
    include!(concat!(env!("OUT_DIR"), "/byte_stream_capnp.rs"));
}

pub mod nix_daemon_capnp {
    include!(concat!(env!("OUT_DIR"), "/nix_daemon_capnp.rs"));
}

pub mod nixrs_capnp {
    include!(concat!(env!("OUT_DIR"), "/nixrs_capnp.rs"));
}
