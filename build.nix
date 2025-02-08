{pkgs, lib, project, ...}: let
  crates = pkgs.callPackage ./Cargo.nix {
    defaultCrateOverrides = pkgs.defaultCrateOverrides // {

    };
  };
  cargoDeps = pkgs.rustPlatform.importCargoLock {
    lockFile = ./Cargo.lock;
    outputHashes = lib.listToAttrs
      (lib.map (c: let 
        crate = crates.internal.crates.${c};
      in lib.nameValuePair "${crate.crateName}-${crate.version}" crate.src.outputHash )
      [
        #"nix-compat"
      ]);
  };
  src = ./.;
in {
  inherit crates src cargoDeps;

  clippy = pkgs.stdenv.mkDerivation {
    name = "nixrs-clippy";
    inherit cargoDeps src;
    ALL_NIX = project.nix.all-nix.all-nix;
    nativeBuildInputs = with pkgs; [
      pkg-config
      cargo
      clippy
      rustPlatform.cargoSetupHook
      libsodium
      capnproto
      #protobuf
    ];
    buildPhase = "cargo clippy --tests --examples --benches -- -Dwarnings | tee $out";
  };
  /*
  crate2nix-check = pkgs.stdenv.mkDerivation {
    name = "nixrs-crate2nix-check";
    inherit src;
    outputHash = "sha256:${builtins.hashFile "sha256" ./Cargo.nix}";
    buildPhase = ''
      ${pkgs.crate2nix}/bin/crate2nix generate
      cp Cargo.nix $out
    '';
  };
  */
}