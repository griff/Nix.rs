{ pkgs, project, lib, ... }:
let
 test-lib = project.crates.workspaceMembers.nixrs-daemon-tests.build.override {
    runTests = true;
    testPreRun = ''

    '';
    testPostRun = ''
      mkdir -p $bin/bin
      cp $file $bin/bin/nixrs-daemon-tests
    '';
  };
in test-lib.test.overrideAttrs (finalAttrs: previousAttrs: {
  outputs = ["out" "bin"];
})