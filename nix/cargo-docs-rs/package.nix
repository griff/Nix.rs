{ lib, fetchFromGitHub, rustPlatform }:

rustPlatform.buildRustPackage rec {
  pname = "cargo-docs-rs";
  version = "1.0.0";

  src = fetchFromGitHub {
    owner = "dtolnay";
    repo = pname;
    rev = version;
    hash = "sha256-UYutiv4wFiZbpxGWcn5OT3ysd28djjb0AUInlSysYZ8=";
  };
  cargoPatches = [
    ./cargo-lock.patch
  ];
  cargoHash = "sha256-k3XIvIB++4FnZlTjbshd++PaL6yjMfVLpjC83kes2gs=";

  meta = with lib; {
    description = "Imitate the documentation build that docs.rs would do";
    homepage = "https://github.com/dtolnay/cargo-docs-rs";
    license = with licenses; [ mit asl20 ];
    maintainers = [];
  };
}