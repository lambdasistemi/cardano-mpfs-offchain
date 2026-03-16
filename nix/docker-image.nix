{ pkgs, project, version, ... }:

pkgs.dockerTools.buildImage {
  name = "ghcr.io/lambdasistemi/cardano-mpfs-offchain/mpfs-serve";
  tag = version;
  config = { EntryPoint = [ "mpfs-serve" ]; };
  copyToRoot = pkgs.buildEnv {
    name = "image-root";
    paths = [
      project.hsPkgs.cardano-mpfs-offchain.components.exes.mpfs-serve
    ];
  };
}
