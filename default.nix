{system, nixpkgs, ...}: let
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
  eligibleCheck = node: (node ? outPath) && !(node.meta.broken or false);
  tree = readTree.fix (self: let
    args = {
      inherit pkgs;
      project = self;
      lib = pkgs.lib;
    };
  in (readProject args) // {
    packages = readTree.gather (t: eligible t) self;
    checks = readTree.gather (t: eligibleCheck t) self;
    flake = {
      tree = self;
      packages = (pkgs.lib.listToAttrs
        (map (p: {name = p.meta.flake.exported; value = p;})
        self.packages));
      checks = pkgs.lib.listToAttrs
        (map (p: {name = p.name; value = p;})
        self.checks);
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
          cloc
        ] ++ lib.optionals stdenv.isDarwin [
          darwin.apple_sdk.frameworks.CoreServices
          darwin.apple_sdk.frameworks.Security
          darwin.apple_sdk.frameworks.SystemConfiguration
          iconv
        ];
      };
    };
  } // (import ./build.nix args));
  
in tree