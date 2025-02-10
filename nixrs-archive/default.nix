{ project, lib, ... }:

project.crates.workspaceMembers.nixrs-archive.build.override {
  testPreRun = ''
    if [[ -L test-data/test-dir.nar ]]; then
      echo "Fixing test-data"
      rm -rf test-data
      cp -a ${./test-data} test-data
    fi
    ls -laR test-data
  '';
  runTests = true;
}
