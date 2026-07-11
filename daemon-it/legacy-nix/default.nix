{ pkgs, project, lib, ... }: let
  mkDaemonTest = project.daemon-it.mk-daemon-test;
  legacy-nix = project.nix.all-nix;
in {
  nix_2_3 = mkDaemonTest {
    name = "nix_2_3";
    config = {
      program = "${legacy-nix.nix_2_3}/bin/nix-daemon";
      args = [ "--process-ops" "--debug" "-vvvvvv" "--stdio" ];
      env.NIX_CONF = ./nix_2_3.conf;
      protocol_range = "1.10..1.22";
      quirks = ["ChompLog"];
      skipped = [
          "unittests::handshake_logs"
          "unittests::sesennst"
      ];
    };
  };

  nix_2_24 = mkDaemonTest {
    name = "nix_2_24";
    config = {
      program = "${legacy-nix.nix_2_24}/bin/nix-daemon";
      args = [
        "--extra-experimental-features" "mounted-ssh-store"
        "--process-ops"
        "--debug"
        "-vvvvvv"
        "--stdio"
      ];
      env.NIX_CONF = ./nix_2_3.conf;
      protocol_range = "1.10..1.37";
      quirks = ["LogPrefix" "ChompLog"];
      skipped = [
      ];
    };
  };

  lix_2_91 = mkDaemonTest {
    name = "lix_2_91";
    config = {
      program = "${legacy-nix.lix_2_91}/bin/nix-daemon";
      args = [
        "--process-ops"
        "--debug"
        "-vvvvvv"
        "--stdio"
      ];
      env.NIX_CONF = ./nix_2_3.conf;
      protocol_range = "1.10..1.36";
      quirks = ["LogPrefix" "ChompLog"];
      skipped = [
      ];
    };
  };

  lix_2_93 = mkDaemonTest {
    name = "lix_2_93";
    config = {
      program = "${legacy-nix.lix_2_93}/bin/nix-daemon";
      args = [
        "--process-ops"
        "--debug"
        "-vvvvvv"
        "--stdio"
      ];
      env.NIX_CONF = ./nix_2_3.conf;
      protocol_range = "1.10..1.36";
      quirks = ["LogPrefix" "ChompLog"];
      skipped = [
        "unittests::add_multiple_to_store"
      ];
    };
  };

  meta.ci.targets = [ "nix_2_3" "nix_2_24" "lix_2_93"];
}