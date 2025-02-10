{ project, lib, ... }:

project.crates.workspaceMembers.nixrs-io.build.override {
  runTests = true;
}
