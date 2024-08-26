use nixrs_derive::NixDeserialize;

#[derive(NixDeserialize)]
#[nix(from_str)]
pub struct Test;

fn main() {}
