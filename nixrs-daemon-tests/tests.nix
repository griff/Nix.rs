{ pkgs, project, lib, ... }: let
  mkDaemonTest = project.nixrs-daemon-tests.mk-daemon-test;
in (lib.mapAttrs (name: v: mkDaemonTest {
      inherit name;
      config = {program_path = "${v}/bin/nix-daemon";
                conf_path = "${project.nix.all-nix.files.conf}/nix_2_3.conf";};
      configFile = "${project.nix.all-nix.files.run-config}/${name}.json";
    })
  (lib.getAttrs project.nix.all-nix.unbroken project.nix.all-nix)) // {
  meta.ci.targets = project.nix.all-nix.unbroken;
}