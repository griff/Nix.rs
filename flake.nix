{
  description = "A very basic flake";
  inputs = {
    flake-utils.url = "github:numtide/flake-utils";
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
  };
  outputs = { self, nixpkgs, flake-utils } @ flake-inputs: {

    packages.x86_64-linux.hello = nixpkgs.legacyPackages.x86_64-linux.hello;

    packages.x86_64-linux.default = self.packages.x86_64-linux.hello;

  } // (flake-utils.lib.eachDefaultSystem (system:
    let
      tree = import ./default.nix {
        inherit system nixpkgs flake-inputs;
      };
    in tree.flake));
}
