
{pkgs, project, lib, ...}:
{ name, config, configFile ? null }: let
  raw-run-config = pkgs.writeText "${name}-raw-run-config.json" (builtins.toJSON config);
  run-config = if configFile == null then raw-run-config else pkgs.runCommand "${name}-run-config.json" {} ''
    ${pkgs.jq}/bin/jq --slurpfile changes ${raw-run-config} '. * $changes[0]' ${configFile} > $out
  '';
  crate = project.nixrs-daemon-tests.crate.bin;
in pkgs.runCommand "daemon-tests-${name}" { UNIX_PROXY = "${project.nix.unix-proxy}/bin/unix-proxy"; } ''
  export NIX_IMPL=${run-config}
  echo "Running tests for $NIX_IMPL"
  cat $NIX_IMPL
  ${crate}/bin/nixrs-daemon-tests 2>&1 | tee -a $out
''