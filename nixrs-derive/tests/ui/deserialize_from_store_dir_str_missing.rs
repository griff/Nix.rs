use nixrs_derive::NixDeserialize;

#[derive(NixDeserialize)]
#[nix(from_store_dir_str)]
pub struct Test;

fn main() {}
