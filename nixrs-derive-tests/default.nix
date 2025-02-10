{ project, pkgs, lib, ... }:

pkgs.stdenv.mkDerivation {
  name = "nixrs-derive-tests";
  inherit (project) cargoDeps src;
  ALL_NIX = project.nix.all-nix.all-nix;
  nativeBuildInputs = with pkgs; [
    pkg-config
    cargo
    rustPlatform.cargoSetupHook
  ];
  buildPhase = ''
    cargo test -p nixrs-derive-tests | tee $out
  '';
}
