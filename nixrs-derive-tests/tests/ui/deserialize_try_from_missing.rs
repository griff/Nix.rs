use nixrs_derive::NixDeserialize;

#[derive(NixDeserialize)]
#[nix(try_from = "u64")]
pub struct Test;

fn main() {}
