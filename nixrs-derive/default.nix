{ project, lib, ... }:

project.crates.workspaceMembers.nixrs-derive.build.override {
  runTests = true;
}
