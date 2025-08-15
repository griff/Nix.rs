{pkgs, lib, project, ...}: let
  crates = pkgs.callPackage ./Cargo.nix {
    defaultCrateOverrides = pkgs.defaultCrateOverrides // {
      nixrs = prev: {
        ALL_NIX = project.nix.all-nix.all-nix;
        UNIX_PROXY = "${project.nix.unix-proxy}/bin/unix-proxy";
      };
      nixrs-capnp = prev: {
        buildInputs = [pkgs.capnproto];
      };
      capnp-rpc-tokio = prev: {
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
        "capnp" "capnpc" "capnp-rpc" "capnp-futures"
      ]);
  };
  src = pkgs.lib.cleanSourceWith {
    name = "project";
    src = pkgs.nix-gitignore.gitignoreSource [] ./.;
    filter = pkgs.lib.cleanSourceFilter;
  };
  onlyCargoSrc = pkgs.lib.cleanSourceWith {
    name = "project-empty-cargo";
    inherit src;
    filter = path: type: let
      base = builtins.baseNameOf path;
      in type == "directory" || base == "Cargo.toml" || base == "Cargo.lock" || base == "build.rs";
  };
  emptySrc = pkgs.runCommand "empty-src" { src = onlyCargoSrc; } ''
    cp -r $src $out
    chmod -R +w $out
    for dir in $(find $out -type d) ; do
      if [[  -f $dir/Cargo.toml ]]; then
        if [[ -d $dir/src ]]; then
          touch $dir/src/lib.rs
        fi
        if [[ -f $dir/build.rs ]]; then
          echo 'fn  main() {}' > $dir/build.rs
        fi
        if [[ -d $dir/tests ]]; then
          touch $dir/tests/test.rs
        fi
      fi
    done
  '';
in {
  inherit crates src cargoDeps;

  clippy = pkgs.stdenv.mkDerivation {
    name = "nixrs-clippy";
    inherit cargoDeps src;
    ALL_NIX = project.nix.all-nix.all-nix;
    UNIX_PROXY = "${project.nix.unix-proxy}/bin/unix-proxy";
    nativeBuildInputs = with pkgs; [
      pkg-config
      cargo
      clippy
      rustPlatform.cargoSetupHook
      libsodium
      capnproto
    ];
    buildPhase = ''
      cargo clippy --tests --examples --benches --no-deps -- -Dwarnings 2>&1 | tee -a $out
    '';
  };
  rustdoc = pkgs.stdenv.mkDerivation {
    name = "nixrs-rustdoc";
    inherit cargoDeps src;
    ALL_NIX = project.nix.all-nix.all-nix;
    UNIX_PROXY = "${project.nix.unix-proxy}/bin/unix-proxy";
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
    UNIX_PROXY = "${project.nix.unix-proxy}/bin/unix-proxy";
    nativeBuildInputs = with pkgs; [
      pkg-config
      cargo
      clippy
      rustPlatform.cargoSetupHook
      libsodium
      capnproto
    ];
    buildPhase = ''
      cargo test --doc 2>&1 | tee -a $out
    '';
  };
  crate2nix-check = let
    cargoNix = builtins.readFile ./Cargo.nix;
    cargoHash = builtins.hashString "sha256" "${cargoNix}${emptySrc}";
    outputHash = builtins.hashString "sha256" "${cargoNix}${cargoHash}\n";
  in pkgs.stdenv.mkDerivation {
    name = "nixrs-crate2nix-check";
    src = src;
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
      ${pkgs.diffutils}/bin/diff -u ${./Cargo.nix} $out || true
      echo "${cargoHash}" >> $out
    '';
  };
  treefmt = pkgs.stdenv.mkDerivation {
    name = "nixrs-treefmt";
    inherit cargoDeps src;
    ALL_NIX = project.nix.all-nix.all-nix;
    UNIX_PROXY = "${project.nix.unix-proxy}/bin/unix-proxy";
    nativeBuildInputs = with pkgs; [
      treefmt
      rustfmt
      nixpkgs-fmt
    ];
    buildPhase = ''
      treefmt . --ci 2>&1 | tee -a $out
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
  check-names = lib.map (n: n.name) project.checks;
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
    UNIX_PROXY = "${project.nix.unix-proxy}/bin/unix-proxy";
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