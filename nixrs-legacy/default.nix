{ project, lib, ... }:

project.crates.workspaceMembers.nixrs-legacy.build.override {
  runTests = true;
}
