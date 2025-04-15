{pkgs, lib, project, ...}: let
  crates = pkgs.callPackage ./Cargo.nix {
    defaultCrateOverrides = pkgs.defaultCrateOverrides // {
      nixrs = prev: {
        ALL_NIX = project.nix.all-nix.all-nix;
      };
      nixrs-capnp = prev: {
        buildInputs = [pkgs.capnproto];
      };
      libsodium-sys = prev: {
        nativeBuildInputs = [pkgs.pkg-config];
        buildInputs = [pkgs.libsodium];
      };
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
  src = pkgs.lib.cleanSourceWith {
    name = "project";
    src = pkgs.nix-gitignore.gitignoreSource [] ./.;
    filter = pkgs.lib.cleanSourceFilter;
  };
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
    ];
    buildPhase = "cargo clippy --tests --examples --benches -- -Dwarnings | tee $out";
  };
  rustdoc = pkgs.stdenv.mkDerivation {
    name = "nixrs-rustdoc";
    inherit cargoDeps src;
    ALL_NIX = project.nix.all-nix.all-nix;
    nativeBuildInputs = with pkgs; [
      pkg-config
      cargo
      clippy
      rustPlatform.cargoSetupHook
      libsodium
      capnproto
    ];
    buildPhase = ''
      cargo doc
      mv target/doc $out
    '';
  };
  doc-tests = pkgs.stdenv.mkDerivation {
    name = "nixrs-doc-tests";
    inherit cargoDeps src;
    ALL_NIX = project.nix.all-nix.all-nix;
    nativeBuildInputs = with pkgs; [
      pkg-config
      cargo
      clippy
      rustPlatform.cargoSetupHook
      libsodium
      capnproto
    ];
    buildPhase = ''
      cargo test --doc | tee $out
    '';
  };
  crate2nix-check = let
    cargoNix = builtins.readFile ./Cargo.nix;
    cargoHash = builtins.hashString "sha256" cargoNix;
    time = toString builtins.currentTime;
    outputHash = builtins.hashString "sha256" "${cargoNix}${cargoHash}${time}\n";
  in pkgs.stdenv.mkDerivation {
    name = "nixrs-crate2nix-check";
    inherit src;
    inherit outputHash;
    outputHashAlgo = "sha256";
    outputHashMode = "flat";
    SSL_CERT_FILE = "${pkgs.cacert}/etc/ssl/certs/ca-bundle.crt";
    buildPhase = ''
      export CARGO_HOME=$TMP/cargo
      export HOME=$TMP
      mkdir -p $CARGO_HOME
      echo "[http]" > $CARGO_HOME/config.toml
      echo "cainfo = \"$SSL_CERT_FILE\"" >> $CARGO_HOME/config.toml
      cat $CARGO_HOME/config.toml
      ${pkgs.crate2nix}/bin/crate2nix generate
      cp Cargo.nix $out
      echo "${cargoHash}${time}" >> $out
    '';
  };
  treefmt = pkgs.stdenv.mkDerivation {
    name = "nixrs-treefmt";
    inherit cargoDeps src;
    ALL_NIX = project.nix.all-nix.all-nix;
    nativeBuildInputs = with pkgs; [
      treefmt
      rustfmt
      nixpkgs-fmt
    ];
    buildPhase = ''
      treefmt . --ci | tee $out
    '';
  };
  meta.ci.targets = [
    "clippy"
    "rustdoc"
    "doc-tests"
    "crate2nix-check"
    "treefmt"
  ];

  packages = let
    eligible = node: (node ? outPath) && ((node.meta.flake.exported or null) != null) && !(node.meta.broken or false);
    in project.gather (t: eligible t);
  checks = let
    eligibleCheck = node: (node ? outPath) && !(node.meta.broken or false);
    in project.gather (t: eligibleCheck t);
  check-all = pkgs.runCommand "check-all" {} ''
    mkdir -p $out
    ${pkgs.lib.concatMapStringsSep "\n"
      (p: "ln -s ${p.outPath} $out/${p.name}")
      project.checks}
  '';

  shell = pkgs.mkShell {
    name = "Nix.rs";
    buildInputs = [ pkgs.bashInteractive ];
    ALL_NIX = project.nix.all-nix.all-nix;
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
      just
      cloc
    ] ++ lib.optionals stdenv.isDarwin [
      darwin.apple_sdk.frameworks.CoreServices
      darwin.apple_sdk.frameworks.Security
      darwin.apple_sdk.frameworks.SystemConfiguration
      iconv
    ];
  };
}