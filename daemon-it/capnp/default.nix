{ pkgs, project, lib, ... }:
let
  crate = project.crates.workspaceMembers.daemon-it-capnp.build.override {
    runTests = true;
  };
  mkDaemonTest = project.daemon-it.mk-daemon-test;
in mkDaemonTest {
  name = "capnp";
  config = {
    program_path = "${crate}/bin/daemon-it-capnp";
    conf_path = ../legacy-nix/nix_2_3.conf;
    cmd_args = [];
    range = "1.10..1.37";
    op_log_prefix = false;
    chomp_log = false;
    skipped = [];
  };
  passthru.program = crate;
}
