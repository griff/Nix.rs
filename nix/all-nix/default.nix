{ pkgs, ... }:
pkgs.callPackage (
{ lib
, config
, stdenv
, aws-sdk-cpp
, boehmgc
, libgit2
, callPackage
, fetchFromGitHub
, fetchpatch
, fetchpatch2
, runCommand
, buildPackages
, Security
, storeDir ? "/nix/store"
, stateDir ? "/nix/var"
, confDir ? "/etc"
}:
let
  boehmgc-nix_2_3 = boehmgc.override { enableLargeConfig = true; };

  boehmgc-nix = boehmgc-nix_2_3.overrideAttrs (drv: {
    patches = (drv.patches or [ ]) ++ [
      # Part of the GC solution in https://github.com/NixOS/nix/pull/4944
      ./patches/boehmgc-coroutine-sp-fallback.patch
    ];
  });

  # old nix fails to build with newer aws-sdk-cpp and the patch doesn't apply
  aws-sdk-cpp-old-nix = (aws-sdk-cpp.override {
    apis = [ "s3" "transfer" ];
    customMemoryManagement = false;
  }).overrideAttrs (args: rec {
    # intentionally overriding postPatch
    version = "1.9.294";

    src = fetchFromGitHub {
      owner = "aws";
      repo = "aws-sdk-cpp";
      rev = version;
      hash = "sha256-Z1eRKW+8nVD53GkNyYlZjCcT74MqFqqRMeMc33eIQ9g=";
    };
    postPatch = ''
      # Avoid blanket -Werror to evade build failures on less
      # tested compilers.
      substituteInPlace cmake/compiler_settings.cmake \
        --replace '"-Werror"' ' '

      # Missing includes for GCC11
      sed '5i#include <thread>' -i \
        aws-cpp-sdk-cloudfront-integration-tests/CloudfrontOperationTest.cpp \
        aws-cpp-sdk-cognitoidentity-integration-tests/IdentityPoolOperationTest.cpp \
        aws-cpp-sdk-dynamodb-integration-tests/TableOperationTest.cpp \
        aws-cpp-sdk-elasticfilesystem-integration-tests/ElasticFileSystemTest.cpp \
        aws-cpp-sdk-lambda-integration-tests/FunctionTest.cpp \
        aws-cpp-sdk-mediastore-data-integration-tests/MediaStoreDataTest.cpp \
        aws-cpp-sdk-queues/source/sqs/SQSQueue.cpp \
        aws-cpp-sdk-redshift-integration-tests/RedshiftClientTest.cpp \
        aws-cpp-sdk-s3-crt-integration-tests/BucketAndObjectOperationTest.cpp \
        aws-cpp-sdk-s3-integration-tests/BucketAndObjectOperationTest.cpp \
        aws-cpp-sdk-s3control-integration-tests/S3ControlTest.cpp \
        aws-cpp-sdk-sqs-integration-tests/QueueOperationTest.cpp \
        aws-cpp-sdk-transfer-tests/TransferTests.cpp
      # Flaky on Hydra
      rm aws-cpp-sdk-core-tests/aws/auth/AWSCredentialsProviderTest.cpp
      # Includes aws-c-auth private headers, so only works with submodule build
      rm aws-cpp-sdk-core-tests/aws/auth/AWSAuthSignerTest.cpp
      # TestRandomURLMultiThreaded fails
      rm aws-cpp-sdk-core-tests/http/HttpClientTest.cpp
    '' + lib.optionalString aws-sdk-cpp.stdenv.hostPlatform.isi686 ''
      # EPSILON is exceeded
      rm aws-cpp-sdk-core-tests/aws/client/AdaptiveRetryStrategyTest.cpp
    '';

    patches = (args.patches or [ ]) ++ [ ./patches/aws-sdk-cpp-TransferManager-ContentEncoding.patch ];

    # only a stripped down version is build which takes a lot less resources to build
    requiredSystemFeatures = [ ];
  });

  aws-sdk-cpp-nix = (aws-sdk-cpp.override {
    apis = [ "s3" "transfer" ];
    customMemoryManagement = false;
  }).overrideAttrs {
    # only a stripped down version is build which takes a lot less resources to build
    requiredSystemFeatures = [ ];
  };

  libgit2-thin-packfile = libgit2.overrideAttrs (args: {
    nativeBuildInputs = args.nativeBuildInputs or []
      # gitMinimal does not build on Windows. See packbuilder patch.
      ++ lib.optionals (!stdenv.hostPlatform.isWindows) [
        # Needed for `git apply`; see `prePatch`
        buildPackages.gitMinimal
      ];
    # Only `git apply` can handle git binary patches
    prePatch = args.prePatch or ""
      + lib.optionalString (!stdenv.hostPlatform.isWindows) ''
        patch() {
          git apply
        }
      '';
    # taken from https://github.com/NixOS/nix/tree/master/packaging/patches
    patches = (args.patches or []) ++ [
      ./patches/libgit2-mempack-thin-packfile.patch
    ] ++ lib.optionals (!stdenv.hostPlatform.isWindows) [
      ./patches/libgit2-packbuilder-callback-interruptible.patch
    ];
  });

  common = args:
    callPackage
      (import ./common.nix ({ inherit lib fetchFromGitHub; } // args))
      {
        inherit Security storeDir stateDir confDir;
        boehmgc = boehmgc-nix;
        aws-sdk-cpp = if lib.versionAtLeast args.version "2.12pre" then aws-sdk-cpp-nix else aws-sdk-cpp-old-nix;
        libgit2 = if lib.versionAtLeast args.version "2.25.0" then libgit2-thin-packfile else libgit2;
      };

  # https://github.com/NixOS/nix/pull/7585
  patch-monitorfdhup = fetchpatch2 {
    name = "nix-7585-monitor-fd-hup.patch";
    url = "https://github.com/NixOS/nix/commit/1df3d62c769dc68c279e89f68fdd3723ed3bcb5a.patch";
    hash = "sha256-f+F0fUO+bqyPXjt+IXJtISVr589hdc3y+Cdrxznb+Nk=";
  };

  # Intentionally does not support overrideAttrs etc
  # Use only for tests that are about the package relation to `pkgs` and/or NixOS.
  addTestsShallowly = tests: pkg: pkg // {
    tests = pkg.tests // tests;
    # In case someone reads the wrong attribute
    passthru.tests = pkg.tests // tests;
  };

  addFallbackPathsCheck = pkg: addTestsShallowly
    { nix-fallback-paths =
        runCommand "test-nix-fallback-paths-version-equals-nix-stable" {
          paths = lib.concatStringsSep "\n" (builtins.attrValues (import ../../../../nixos/modules/installer/tools/nix-fallback-paths.nix));
        } ''
          # NOTE: name may contain cross compilation details between the pname
          #       and version this is permitted thanks to ([^-]*-)*
          if [[ "" != $(grep -vE 'nix-([^-]*-)*${lib.strings.replaceStrings ["."] ["\\."] pkg.version}$' <<< "$paths") ]]; then
            echo "nix-fallback-paths not up to date with nixVersions.stable (nix-${pkg.version})"
            echo "The following paths are not up to date:"
            grep -v 'nix-${pkg.version}$' <<< "$paths"
            echo
            echo "Fix it by running in nixpkgs:"
            echo
            echo "curl https://releases.nixos.org/nix/nix-${pkg.version}/fallback-paths.nix >nixos/modules/installer/tools/nix-fallback-paths.nix"
            echo
            exit 1
          else
            echo "nix-fallback-paths versions up to date"
            touch $out
          fi
        '';
    }
    pkg;
  all-nix = {
    nix_2_3 = ((common {
      version = "2.3.18";
      hash = "sha256-jBz2Ub65eFYG+aWgSI3AJYvLSghio77fWQiIW1svA9U=";
      patches = [
        patch-monitorfdhup
        ./proxy-patches/nix-2_3.patch
      ];
      self_attribute_name = "nix_2_3";
      maintainers = with lib.maintainers; [ flokli ];
    }).override { boehmgc = boehmgc-nix_2_3; }).overrideAttrs {
      # https://github.com/NixOS/nix/issues/10222
      # spurious test/add.sh failures
      enableParallelChecking = false;
    };

    nix_2_4 = common {
      version = "2.4";
      hash = "sha256-op48CCDgLHK0qV1Batz4Ln5FqBiRjlE6qHTiZgt3b6k=";
      # https://github.com/NixOS/nix/pull/5537
      patches = [
        ./patches/install-nlohmann_json-headers.patch
        ./proxy-patches/nix-2_3.patch
      ];
      meta.broken = true;
      self_attribute_name = "nix_2_4";
    };

    nix_2_5 = common {
      version = "2.5.1";
      hash = "sha256-GOsiqy9EaTwDn2PLZ4eFj1VkXcBUbqrqHehRE9GuGdU=";
      # https://github.com/NixOS/nix/pull/5536
      patches = [
        ./patches/install-nlohmann_json-headers.patch
        ./proxy-patches/nix-2_3.patch
      ];
      meta.broken = true;
      self_attribute_name = "nix_2_5";
    };

    nix_2_6 = common {
      version = "2.6.1";
      hash = "sha256-E9iQ7f+9Z6xFcUvvfksTEfn8LsDfzmwrcRBC//5B3V0=";
      patches = [
        ./proxy-patches/nix-2_3.patch
      ];
      meta.broken = true;
      self_attribute_name = "nix_2_6";
    };

    nix_2_7 = common {
      version = "2.7.0";
      hash = "sha256-m8tqCS6uHveDon5GSro5yZor9H+sHeh+v/veF1IGw24=";
      patches = [
        # remove when there's a 2.7.1 release
        # https://github.com/NixOS/nix/pull/6297
        # https://github.com/NixOS/nix/issues/6243
        # https://github.com/NixOS/nixpkgs/issues/163374
        (fetchpatch {
          url = "https://github.com/NixOS/nix/commit/c9afca59e87afe7d716101e6a75565b4f4b631f7.patch";
          sha256 = "sha256-xz7QnWVCI12lX1+K/Zr9UpB93b10t1HS9y/5n5FYf8Q=";
        })
        ./proxy-patches/nix-2_3.patch
      ];
      meta.broken = true;
      self_attribute_name = "nix_2_7";
    };

    nix_2_8 = common {
      version = "2.8.1";
      hash = "sha256-zldZ4SiwkISFXxrbY/UdwooIZ3Z/I6qKxtpc3zD0T/o=";
      patches = [
        ./proxy-patches/nix-2_3.patch
      ];
      meta.broken = true;
      self_attribute_name = "nix_2_8";
    };

    nix_2_9 = common {
      version = "2.9.2";
      hash = "sha256-uZCaBo9rdWRO/AlQMvVVjpAwzYijB2H5KKQqde6eHkg=";
      patches = [
        ./proxy-patches/nix-2_3.patch
      ];
      meta.broken = true;
      self_attribute_name = "nix_2_9";
    };

  # 2.10.3
    nix_2_10 = common {
      version = "2.10.2";
      hash = "sha256-/8zlkXoZEZd+LgJq5xw8h+u2STqeKLrGTARZklE3CP8=";
      patches = [
        ./patches/flaky-tests.patch
        ./proxy-patches/nix-2_3.patch
      ];
      meta.broken = true;
      self_attribute_name = "nix_2_10";
    };

    # 2.11.1
    # 2.12.1
    # 2.13.6
    # 2.14.1
    # 2.15.3
    # 2.16.3
    # 2.17.2

    nix_2_18 = common {
      version = "2.18.9";
      hash = "sha256-RrOFlDGmRXcVRV2p2HqHGqvzGNyWoD0Dado/BNlJ1SI=";
      patches = [
        ./proxy-patches/nix-2_3.patch
      ];
      meta.broken = true;
      self_attribute_name = "nix_2_18";
    };

    nix_2_19 = common {
      version = "2.19.7";
      hash = "sha256-CkT1SNwRYYQdN2X4cTt1WX3YZfKZFWf7O1YTEo1APfc=";
      patches = [
        ./proxy-patches/nix-2_3.patch
      ];
      meta.broken = true;
      self_attribute_name = "nix_2_19";
    };

    nix_2_20 = common {
      version = "2.20.9";
      hash = "sha256-b7smrbPLP/wcoBFCJ8j1UDNj0p4jiKT/6mNlDdlrOXA=";
      patches = [
        ./proxy-patches/nix-2_3.patch
      ];
      meta.broken = true;
      self_attribute_name = "nix_2_20";
    };

    nix_2_21 = common {
      version = "2.21.5";
      hash = "sha256-/+TLpd6hvYMJFoeJvVZ+bZzjwY/jP6CxJRGmwKcXbI0=";
      patches = [
        ./proxy-patches/nix-2_3.patch
      ];
      meta.broken = true;
      self_attribute_name = "nix_2_21";
    };

    nix_2_22 = common {
      version = "2.22.4";
      hash = "sha256-JWjJzMA+CeyImMgP2dhSBHQW4CS8wg7fc2zQ4WdKuBo=";
      patches = [
        ./proxy-patches/nix-2_3.patch
      ];
      meta.broken = true;
      self_attribute_name = "nix_2_22";
    };

    nix_2_23 = common {
      version = "2.23.4";
      hash = "sha256-rugH4TUicHEdVfy3UuAobFIutqbuVco8Yg/z81g7clE=";
      patches = [
        ./proxy-patches/nix-2_3.patch
      ];
      meta.broken = true;
      self_attribute_name = "nix_2_23";
    };

    # 2.24.14
    nix_2_24 = common {
      version = "2.24.14";
      hash = "sha256-SthMCsj6POjawLnJq9+lj/UzObX9skaeN1UGmMZiwTY=";
      patches = [
        ./proxy-patches/nix-2_24.patch
      ];
      self_attribute_name = "nix_2_24";
    };

    lix_2_91 = pkgs.lix.overrideAttrs {
      patches = [
        ./proxy-patches/lix-2_91.patch
      ];
      doCheck = false;
      doInstallCheck = false;
      meta.flake.exported = "lix_2_91";
    };
  };
  selected = [ "nix_2_3" "nix_2_24" "lix_2_91"]; 
  selected-nix = lib.getAttrs selected all-nix;
in lib.makeExtensible (self: (
  all-nix // {
    all-nix = runCommand "all-nix" { meta.flake.exported = "all-nix"; } ''
      mkdir $out
      cp -a ${./conf} $out/conf
      ${lib.concatStringsSep "\n" (lib.mapAttrsToList (n: d: ''
        ln -s ${toString d} $out/${n}
      '') (lib.filterAttrs (n: v: (lib.isDerivation v) && !(v.meta.broken or false)) selected-nix))}
    '';
    meta.ci.targets = (lib.attrNames all-nix) ++ [ "all-nix" ];
  }
))) {
  inherit (pkgs.darwin.apple_sdk.frameworks) Security;
}