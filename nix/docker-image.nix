{ pkgs, project, version, mpfs-blueprint, ... }:

let
  blueprint-dir = pkgs.runCommand "mpfs-blueprint" { } ''
    mkdir -p $out/etc/mpfs
    cp ${mpfs-blueprint} $out/etc/mpfs/blueprint.json
  '';
in pkgs.dockerTools.buildImage {
  name = "ghcr.io/lambdasistemi/cardano-mpfs-offchain/mpfs-serve";
  tag = version;
  config = {
    # Docker and the OCI image spec read `Entrypoint`; any other spelling is
    # silently ignored and yields an image with no entrypoint at all.
    Entrypoint = [ "mpfs-serve" ];
    Labels = {
      "org.opencontainers.image.revision" = version;
      "org.opencontainers.image.version" = version;
      "org.opencontainers.image.source" =
        "https://github.com/lambdasistemi/cardano-mpfs-offchain";
    };
  };
  copyToRoot = pkgs.buildEnv {
    name = "image-root";
    paths = [
      project.hsPkgs.cardano-mpfs-offchain.components.exes.mpfs-serve
      blueprint-dir
      pkgs.wget
    ];
  };
}
