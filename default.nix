{ system ? builtins.currentSystem
, sources ? import ./npins
, nixpkgs ? sources.nixpkgs
, config ? {}
}: let
  pkgs = import nixpkgs {
    inherit system;
    overlays = [
      (import sources.rust-overlay)
    ];
    config = {
      permittedInsecurePackages = [
        "nix-2.4" "nix-2.5.1" "nix-2.6.1" "nix-2.7.0" "nix-2.8.1"
        "nix-2.9.2" "nix-2.10.2"];
    } // config;
  };
  readTree = import ./nix/readTree {};
  readProject = args: readTree {
    path = ./.;
    inherit args;
  };
  tree = readTree.fix (self: let
    args = {
      inherit pkgs sources;
      project = self;
      lib = pkgs.lib;
    };
  in (readProject args) // {
    gather = eligible: readTree.gather (t: eligible t) self;
  } // (import ./build.nix args));
  
in tree