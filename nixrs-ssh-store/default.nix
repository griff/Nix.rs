{ project, pkgs, lib, ... }:

project.crates.workspaceMembers.nixrs-ssh-store.build.override {
  testInputs = [pkgs.openssh];
  testPreRun = ''
    command -v ssh
  '';
  runTests = true;
}
