{lib, pkgs, sources, ...}: let
  legacy-nix = import "${sources.legacy-nix}/packages.nix" {
    inherit pkgs lib;
  };
in {
  nix_2_3 = legacy-nix.proxy_nix_2_3;
  nix_2_24 = legacy-nix.proxy_nix_2_24;
  nix_2_34 = legacy-nix.proxy_nix_2_34;
  lix_2_93 = legacy-nix.proxy_lix_2_93;
}