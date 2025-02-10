use nixrs_derive::nix_deserialize_remote;

pub struct Test;

nix_deserialize_remote!(Test);

pub fn main() {}
