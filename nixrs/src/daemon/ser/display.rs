#[cfg(feature="nixrs-derive")]
use nixrs_derive::nix_serialize_remote;

#[cfg(feature="nixrs-derive")]
use crate::hash;

#[cfg(feature="nixrs-derive")]
nix_serialize_remote!(
    #[nix(display)]
    hash::Algorithm
);

#[cfg(feature="nixrs-derive")]
nix_serialize_remote!(
    #[nix(display)]
    hash::Hash
);
