{ project, ... }:
project.craneLib.cargoTest (project.commonArgs // {
  inherit (project) cargoArtifacts;
  name = "nixrs-derive-tests";
  cargoTestExtraArgs = "-p nixrs-derive-tests";
})