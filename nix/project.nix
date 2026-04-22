{ CHaP, indexState, pkgs, mkdocs, asciinema, cardano-node-pkgs, mpfs-blueprint
, devnet-genesis, version ? "dev", ... }:

let
  indexTool = { index-state = indexState; };
  fix-libs = { lib, pkgs, ... }: {
    packages.cardano-crypto-praos.components.library.pkgconfig =
      lib.mkForce [ [ pkgs.libsodium-vrf ] ];
    packages.cardano-crypto-class.components.library.pkgconfig =
      lib.mkForce [[ pkgs.libsodium-vrf pkgs.secp256k1 pkgs.libblst ]];
    packages.lzma.components.library.libs = lib.mkForce [ pkgs.xz ];
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
      export E2E_GENESIS_DIR="${devnet-genesis}"
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
  packages.mpfs-serve =
    project.hsPkgs.cardano-mpfs-offchain.components.exes.mpfs-serve;
  packages.docker-image =
    import ./docker-image.nix { inherit pkgs project version mpfs-blueprint; };
  packages.mpfs-devnet-server =
    project.hsPkgs.cardano-mpfs-offchain.components.exes.mpfs-devnet-server;
  packages.devnet-genesis = devnet-genesis;
  packages.offchain-tests =
    project.hsPkgs.cardano-mpfs-offchain.components.tests.unit-tests;
  packages.e2e-tests =
    project.hsPkgs.cardano-mpfs-offchain.components.tests.e2e-tests;
  packages.haddock = haddock;
  packages.cardano-mpfs-swagger =
    project.hsPkgs.cardano-mpfs-offchain.components.exes.cardano-mpfs-swagger;
  checks.swagger-up-to-date = pkgs.runCommand "swagger-up-to-date" { } ''
          ${
            pkgs.lib.getExe
            project.hsPkgs.cardano-mpfs-offchain.components.exes.cardano-mpfs-swagger
          } > $TMPDIR/swagger.json
          diff -u ${../docs/assets/swagger.json} \
            $TMPDIR/swagger.json \
            || (echo "swagger.json is stale — run: \
    just update-swagger" && exit 1)
          touch $out
  '';
}
