{pkgs, lib, project, sources, ...}: let
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

  craneLib = import sources.crane { inherit pkgs; };
  craneLibNightly = craneLib.overrideToolchain (
    p:
    p.rust-bin.selectLatestNightlyWith (toolchain: toolchain.default)
  );
  src = pkgs.lib.cleanSourceWith {
    name = "nixrs-project";
    src = pkgs.nix-gitignore.gitignoreSource [] ./.;
    filter = pkgs.lib.cleanSourceFilter;
  };

  craneSrc = craneLib.cleanCargoSource src;
  commonArgs = {
    src = src;
    strictDeps = true;
    nativeBuildInputs = [
      pkgs.pkg-config
      pkgs.capnproto
    ];
    buildInputs = [
      pkgs.libsodium
    ];
  };
  cargoArtifacts = craneLib.buildDepsOnly commonArgs;
  cargoDocsRs = {
    packages ? [],
    cargoDocsRsExtraArgs ? "",
    cargoExtraArgs ? "--locked",
    ...
  }@origArgs: let
    args = builtins.removeAttrs origArgs [
      "packages"
      "cargoDocsRsExtraArgs"
      "cargoExtraArgs"
    ];
  in
  craneLibNightly.mkCargoDerivation (args // {
    pnameSuffix = "-docs-rs";

    doInstallCargoArtifacts = args.doInstallCargoArtifacts or false;

    docInstallRoot = args.docInstallRoot or "";
    CARGO_BUILD_TARGET = pkgs.stdenv.hostPlatform.config;

    buildPhaseCargoCommand = if packages == []
    then "cargo docs-rs ${cargoExtraArgs} --target $CARGO_BUILD_TARGET ${cargoDocsRsExtraArgs}"
    else ''
      ${pkgs.lib.concatMapStringsSep "\n"
      (p: "cargo docs-rs ${cargoExtraArgs} -p ${p} --target $CARGO_BUILD_TARGET ${cargoDocsRsExtraArgs}")
      packages}
    '';

    installPhaseCommand = ''
      echo initial ''${CARGO_BUILD_TARGET:-} $docInstallRoot
      if [ -z "''${docInstallRoot:-}" ]; then
        docInstallRoot="''${CARGO_TARGET_DIR:-target}/''${CARGO_BUILD_TARGET:-}/doc"
        echo set $docInstallRoot

        if ! [ -d "''${docInstallRoot}" ]; then
          docInstallRoot="''${CARGO_TARGET_DIR:-target}/doc"
          echo default $docInstallRoot
        fi
      fi

      echo actual $docInstallRoot
      mkdir -p $out/share
      mv "''${docInstallRoot}" $out/share
    '';

    nativeBuildInputs = (args.nativeBuildInputs or [ ]) ++ [ project.nix.cargo-docs-rs ];
  });

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
  inherit crates src craneLib craneLibNightly commonArgs cargoArtifacts;

  clippy = craneLib.cargoClippy (
    commonArgs
    // {
      inherit cargoArtifacts;
      cargoClippyExtraArgs = "--all-targets --no-deps -- --deny warnings";
    }
  );

  rustdoc = cargoDocsRs (
    commonArgs
    // {
      inherit cargoArtifacts;
      packages = ["nixrs" "nixrs-legacy" "nixrs-ssh-store" "nixrs-capnp" "capnp-rpc-tokio" ];
    }
  );

  doc-tests = craneLib.cargoDocTest (
    commonArgs
    // {
      inherit cargoArtifacts;
    }
  );

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

  treefmt = craneLib.mkCargoDerivation (commonArgs // {
    inherit cargoArtifacts;
    name = "nixrs-treefmt";
    doInstallCargoArtifacts = false;
    buildPhaseCargoCommand = ''
      treefmt . --ci 2>&1 | tee -a $out
    '';

    installPhaseCommand = ''
    '';

    nativeBuildInputs = with pkgs; [
      treefmt
      rustfmt
      nixpkgs-fmt
    ];
  });

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
      cargo
      rustc
      capnproto
      nix-output-monitor
      just
      cloc
    ] ++ lib.optionals stdenv.isDarwin [
      iconv
    ];
  };
}