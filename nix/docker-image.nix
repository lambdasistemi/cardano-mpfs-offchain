{ pkgs, project, version, mpfs-blueprint, ... }:

let
  blueprint-dir = pkgs.runCommand "mpfs-blueprint" { } ''
    mkdir -p $out/etc/mpfs
    cp ${mpfs-blueprint} $out/etc/mpfs/blueprint.json
  '';
in pkgs.dockerTools.buildImage {
  name = "ghcr.io/lambdasistemi/cardano-mpfs-offchain/mpfs-serve";
  tag = version;
  config = { EntryPoint = [ "mpfs-serve" ]; };
  copyToRoot = pkgs.buildEnv {
    name = "image-root";
    paths = [
      project.hsPkgs.cardano-mpfs-offchain.components.exes.mpfs-serve
      blueprint-dir
      pkgs.wget
    ];
  };
}
