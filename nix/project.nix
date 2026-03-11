{ CHaP, indexState, pkgs, mkdocs, asciinema, cardano-node-pkgs, mpfs-blueprint
, ... }:

let
  indexTool = { index-state = indexState; };
  fix-libs = { lib, pkgs, ... }: {
    packages.cardano-crypto-praos.components.library.pkgconfig =
      lib.mkForce [ [ pkgs.libsodium-vrf ] ];
    packages.cardano-crypto-class.components.library.pkgconfig =
      lib.mkForce [[ pkgs.libsodium-vrf pkgs.secp256k1 pkgs.libblst ]];
  };
  shell = { pkgs, ... }: {
    tools = {
      cabal = indexTool;
      cabal-fmt = indexTool;
      haskell-language-server = indexTool;
      hoogle = indexTool;
      fourmolu = indexTool;
      hlint = indexTool;
    };
    withHoogle = true;
    buildInputs = [
      pkgs.just
      pkgs.nixfmt-classic
      pkgs.shellcheck
      pkgs.mkdocs
      mkdocs.from-nixpkgs
      mkdocs.asciinema-plugin
      mkdocs.markdown-callouts
      asciinema.compress
      asciinema.resize
      pkgs.asciinema
      cardano-node-pkgs.cardano-node
      cardano-node-pkgs.cardano-cli
      pkgs.aiken
    ];
    shellHook = ''
      echo "Entering cardano-mpfs-offchain dev shell"
      export MPFS_BLUEPRINT="${mpfs-blueprint}"
    '';
  };

  mkProject = ctx@{ lib, pkgs, ... }: {
    name = "cardano-mpfs-offchain";
    src = ./..;
    compiler-nix-name = "ghc984";
    shell = shell { inherit pkgs; };
    modules = [ fix-libs ];
    inputMap = { "https://chap.intersectmbo.org/" = CHaP; };
  };

  project = pkgs.haskell-nix.cabalProject' mkProject;

  haddock = import ./haddock.nix { inherit pkgs project; };

in {
  devShells.default = project.shell;
  inherit project;
  packages.cardano-mpfs-offchain =
    project.hsPkgs.cardano-mpfs-offchain.components.library;
  packages.offchain-tests =
    project.hsPkgs.cardano-mpfs-offchain.components.tests.unit-tests;
  packages.e2e-tests =
    project.hsPkgs.cardano-mpfs-offchain.components.tests.e2e-tests;
  packages.mpfs-bootstrap-genesis =
    project.hsPkgs.cardano-mpfs-offchain.components.exes.mpfs-bootstrap-genesis;
  packages.haddock = haddock;
}
