{ system ? builtins.currentSystem
, sources ? import ./npins
, nixpkgs ? sources.nixpkgs
, config ? {}
}: let
  project = import ./default.nix {
    inherit system sources nixpkgs config;
  };
in project.shell
