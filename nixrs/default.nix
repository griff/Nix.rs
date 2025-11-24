{ project, lib, ... }:

project.crates.workspaceMembers.nixrs.build.override {
  runTests = true;
  features = ["daemon"];
}
