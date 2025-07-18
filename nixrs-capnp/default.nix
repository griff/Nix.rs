{ pkgs, project, lib, ... }:
let
  crate = project.crates.workspaceMembers.nixrs-capnp.build.override {
    runTests = true;
  };
  mkDaemonTest = project.nixrs-daemon-tests.mk-daemon-test;
  integration-tests = mkDaemonTest {
    name = "nixrs-capnp";
    config = {
      program_path = "${crate}/bin/run-tests";
      conf_path = "${project.nix.all-nix.files.conf}/nix_2_3.conf";
    };
    configFile = "${./run-tests.json}";
  };
  integration-tests-old = pkgs.runCommand "nixrs-capnp-integration" {} ''
    ${pkgs.jq}/bin/jq "setpath([\"program_path\"]; \"${crate}/bin/run-tests\") | setpath([\"conf_path\"]; \"${../nix/all-nix/conf/nix_2_3.conf}\")"  ${./run-tests.json} > run-tests.json
    export NIX_IMPL=$PWD/run-tests.json
    ${project.nixrs-daemon-tests.bin}/bin/nixrs-daemon-tests > $out
  '';
in crate.overrideAttrs (old: rec {
  meta.ci = {
    targets = [ "integration-tests" ];
  };
  passthru = old.passthru // {
    inherit integration-tests;
  };
})
