{ pkgs, project, lib, ... }:
let
  crate = project.crates.workspaceMembers.daemon-it-capnp.build.override {
    runTests = true;
  };
  mkDaemonTest = project.daemon-it.mk-daemon-test;
in mkDaemonTest {
  name = "capnp";
  config = {
    program = "${crate}/bin/daemon-it-capnp";
    args = [];
    protocol_range = "1.10..1.37";
    quirks = [];
    skipped = [];
  };
  passthru.program = crate;
}
