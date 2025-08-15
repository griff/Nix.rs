#![allow(clippy::needless_lifetimes, clippy::extra_unused_type_parameters)]

pub mod lookup_capnp {
    include!(concat!(env!("OUT_DIR"), "/lookup_capnp.rs"));
}

pub mod nix_daemon_capnp {
    include!(concat!(env!("OUT_DIR"), "/nix_daemon_capnp.rs"));
}

pub mod nix_types_capnp {
    include!(concat!(env!("OUT_DIR"), "/nix_types_capnp.rs"));
}

pub mod nixrs_capnp {
    include!(concat!(env!("OUT_DIR"), "/nixrs_capnp.rs"));
}
