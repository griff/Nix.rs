{
  description = "A very basic flake";
  inputs = {
    flake-utils.url = "github:numtide/flake-utils";
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
  };
  outputs = { self, nixpkgs, flake-utils }: {

    packages.x86_64-linux.hello = nixpkgs.legacyPackages.x86_64-linux.hello;

    packages.x86_64-linux.default = self.packages.x86_64-linux.hello;

  } // (flake-utils.lib.eachDefaultSystem (system:
    let
      pkgs = import nixpkgs {
        inherit system;
        config = {
          permittedInsecurePackages = [
            "nix-2.4" "nix-2.5.1" "nix-2.6.1" "nix-2.7.0" "nix-2.8.1"
            "nix-2.9.2" "nix-2.10.2"];
        };
      };
      readTree = import ./nix/readTree {};
      readProject = args: readTree {
        path = ./.;
        inherit args;
      };
      eligible = node: (node ? outPath) && ((node.meta.flake.exported or null) != null) && !(node.meta.broken or false);
      tree = readTree.fix (self: (readProject {
        inherit pkgs;
        project = self;
        lib = pkgs.lib;
      }) // {
        flake = null;
        packages = readTree.gather (t: eligible t) self;
      });
      allNix = pkgs.callPackage ./nix/nix {
        inherit (pkgs.darwin.apple_sdk.frameworks) Security;
      };
      allNixPackage = with pkgs; runCommand "all-nix" {} ''
        mkdir $out
        ln -s ${lix} $out/lix
        ${lib.concatStringsSep "\n" (lib.mapAttrsToList (n: d: ''
          ln -s ${toString d} $out/${n}
        '') (lib.filterAttrs (n: v: lib.isDerivation v) allNix))}
      '';
      nixTest = pkgs.nixVersions.latest.overrideAttrs (oldAttrs: {
        patches = oldAttrs.patches ++ [ ./nix-proxy.patch ];
      });
    in
    {
      packages = pkgs.lib.listToAttrs
        (map (p: {name = p.meta.flake.exported; value = p;})
        tree.packages);
      devShells.default = pkgs.mkShell {
        name = "Nix.rs";
        buildInputs = [ pkgs.bashInteractive ];
        ALL_NIX = tree.nix.all-nix.all-nix;
        packages = with pkgs; [
          git
          nix-diff
          libsodium
          pkg-config
          fuse
          protobuf
          libarchive
          jq
          cloc
          treefmt
          crate2nix
          rustc.llvmPackages.llvm
          capnproto
          nix-output-monitor
        ] ++ lib.optionals stdenv.isDarwin [
          darwin.apple_sdk.frameworks.CoreServices
          darwin.apple_sdk.frameworks.Security
          darwin.apple_sdk.frameworks.SystemConfiguration
          iconv
        ];
      };
    }));
}
