{ project, pkgs, lib, ... }:

project.crates.workspaceMembers.nixrs-ssh-store.build.override {
  testInputs = [pkgs.openssh];
  testPreRun = ''
    command -v ssh
    cp -H ./tests/id_ed25519 ./tests/id_ed25519_r
    mv ./tests/id_ed25519_r ./tests/id_ed25519
    chmod 0400 ./tests/id_ed25519
  '';
  runTests = true;
}
