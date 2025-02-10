{ project, lib, ... }:

project.crates.workspaceMembers.nixrs-capnp.build.override {
  runTests = true;
}
