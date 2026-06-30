{ CHaP, indexState, pkgs, system, mkdocs, asciinema, cardano-node-pkgs
, mpfs-blueprint, devnet-genesis, version ? "dev", ... }:

let
  isLinux = system == "x86_64-linux";
  indexTool = { index-state = indexState; };
  toolArgs = name:
    indexTool // pkgs.lib.optionalAttrs (name == "cabal-fmt") {
      cabalProjectLocal = ''
        allow-newer: cabal-fmt:base
      '';
    };
  tool = name: pkgs.haskell-nix.tool "ghc9123" name (toolArgs name);
  fix-libs = { lib, pkgs, ... }:
    {
      packages.cardano-crypto-praos.components.library.pkgconfig =
        lib.mkForce [ [ pkgs.libsodium-vrf ] ];
      packages.cardano-crypto-class.components.library.pkgconfig =
        lib.mkForce [[ pkgs.libsodium-vrf pkgs.secp256k1 pkgs.libblst ]];
      packages.cardano-lmdb.components.library.pkgconfig =
        lib.mkForce [ [ pkgs.lmdb ] ];
      packages.lzma.components.library.libs = lib.mkForce [ pkgs.xz ];
    } // lib.optionalAttrs isLinux {
      packages.blockio-uring.components.library.pkgconfig =
        lib.mkForce [ [ pkgs.liburing ] ];
    };
  shell = { pkgs, ... }: {
    tools = {
      cabal = toolArgs "cabal";
      cabal-fmt = toolArgs "cabal-fmt";
      haskell-language-server = toolArgs "haskell-language-server";
      hoogle = toolArgs "hoogle";
      fourmolu = toolArgs "fourmolu";
      hlint = toolArgs "hlint";
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
      pkgs.lmdb
      pkgs.xz
    ] ++ pkgs.lib.optionals isLinux [ pkgs.liburing ];
    shellHook = ''
      echo "Entering cardano-mpfs-offchain dev shell"
      export MPFS_BLUEPRINT="${mpfs-blueprint}"
      export E2E_GENESIS_DIR="${devnet-genesis}"
    '';
  };

  mkProject = ctx@{ lib, pkgs, ... }: {
    name = "cardano-mpfs-offchain";
    src = import ./clean-src.nix {
      inherit (pkgs) lib;
      src = ./..;
    };
    compiler-nix-name = "ghc9123";
    shell = shell { inherit pkgs; };
    modules = [ fix-libs ];
    inputMap = { "https://chap.intersectmbo.org/" = CHaP; };
  };

  project = pkgs.haskell-nix.cabalProject' mkProject;

  haddock = import ./haddock.nix { inherit pkgs project; };
  offchain-unit-tests =
    project.hsPkgs.cardano-mpfs-offchain.components.tests.unit-tests;
  client-unit-tests =
    project.hsPkgs.cardano-mpfs-client.components.tests.unit-tests;
  workflows-unit-tests =
    project.hsPkgs.cardano-mpfs-workflows.components.tests.unit-tests;
  cli-unit-tests = project.hsPkgs.cardano-mpfs-cli.components.tests.unit-tests;
  e2e-tests = project.hsPkgs.cardano-mpfs-offchain.components.tests.e2e-tests;
  format-runner = pkgs.writeShellApplication {
    name = "format";
    runtimeInputs = [
      (tool "fourmolu")
      (tool "cabal-fmt")
      pkgs.findutils
      pkgs.nixfmt-classic
    ];
    text = ''
      mapfile -d "" hs_files < <(find . -name '*.hs' -not -path './dist-newstyle/*' -not -path './.direnv/*' -print0)
      for _ in 1 2 3; do
        fourmolu -i "''${hs_files[@]}"
      done
      find . -name '*.cabal' -not -path './dist-newstyle/*' -print0 | xargs -0 cabal-fmt -i
      find . -name '*.nix' -not -path './dist-newstyle/*' -print0 | xargs -0 nixfmt
    '';
  };
  format-check-runner = pkgs.writeShellApplication {
    name = "format-check";
    runtimeInputs = [ (tool "fourmolu") (tool "cabal-fmt") pkgs.findutils ];
    text = ''
      mapfile -d "" hs_files < <(find . -name '*.hs' -not -path './dist-newstyle/*' -not -path './.direnv/*' -print0)
      fourmolu -m check "''${hs_files[@]}"
      find . -name '*.cabal' -not -path './dist-newstyle/*' -print0 | xargs -0 cabal-fmt -c
    '';
  };
  hlint-runner = pkgs.writeShellApplication {
    name = "hlint";
    runtimeInputs = [ (tool "hlint") pkgs.findutils ];
    text = ''
      find . -name '*.hs' -not -path './dist-newstyle/*' -not -path './.direnv/*' -print0 | xargs -0 hlint
    '';
  };
  unit-tests-runner = pkgs.writeShellApplication {
    name = "unit-tests";
    text = ''
      export MPFS_BLUEPRINT="${mpfs-blueprint}"
      exec ${offchain-unit-tests}/bin/unit-tests "$@"
    '';
  };
  client-unit-tests-runner = pkgs.writeShellApplication {
    name = "client-unit-tests";
    text = ''
      export MPFS_BLUEPRINT="${mpfs-blueprint}"
      exec ${client-unit-tests}/bin/unit-tests "$@"
    '';
  };
  workflows-unit-tests-runner = pkgs.writeShellApplication {
    name = "workflows-unit-tests";
    text = ''
      exec ${workflows-unit-tests}/bin/unit-tests "$@"
    '';
  };
  cli-unit-tests-runner = pkgs.writeShellApplication {
    name = "cli-unit-tests";
    text = ''
      exec ${cli-unit-tests}/bin/unit-tests "$@"
    '';
  };
  e2e-tests-runner = pkgs.writeShellApplication {
    name = "e2e-tests";
    runtimeInputs = [
      cardano-node-pkgs.cardano-node
      cardano-node-pkgs.cardano-cli
      pkgs.aiken
    ];
    text = ''
      export MPFS_BLUEPRINT="${mpfs-blueprint}"
      export E2E_GENESIS_DIR="${devnet-genesis}"
      exec ${e2e-tests}/bin/e2e-tests "$@"
    '';
  };

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
  packages.mpfs-bootstrap-genesis = devnet-genesis;
  packages.mpfs-cli = project.hsPkgs.cardano-mpfs-cli.components.exes.mpfs-cli;
  packages.devnet-genesis = devnet-genesis;
  packages.offchain-tests = offchain-unit-tests;
  packages.client-tests = client-unit-tests;
  packages.workflows-tests = workflows-unit-tests;
  packages.workflows-unit-tests-runner = workflows-unit-tests-runner;
  packages.e2e-tests = e2e-tests;
  packages.format = format-runner;
  packages.format-check = format-check-runner;
  packages.hlint = hlint-runner;
  packages.unit-tests-runner = unit-tests-runner;
  packages.client-unit-tests-runner = client-unit-tests-runner;
  packages.cli-unit-tests-runner = cli-unit-tests-runner;
  packages.e2e-tests-runner = e2e-tests-runner;
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
  apps.e2e-tests = {
    type = "app";
    program = "${e2e-tests-runner}/bin/e2e-tests";
  };
  apps.format = {
    type = "app";
    program = "${format-runner}/bin/format";
  };
  apps.format-check = {
    type = "app";
    program = "${format-check-runner}/bin/format-check";
  };
  apps.hlint = {
    type = "app";
    program = "${hlint-runner}/bin/hlint";
  };
  apps.unit-tests = {
    type = "app";
    program = "${unit-tests-runner}/bin/unit-tests";
  };
  apps.client-unit-tests = {
    type = "app";
    program = "${client-unit-tests-runner}/bin/client-unit-tests";
  };
  apps.workflows-unit-tests = {
    type = "app";
    program = "${workflows-unit-tests-runner}/bin/workflows-unit-tests";
  };
  apps.cli-unit-tests = {
    type = "app";
    program = "${cli-unit-tests-runner}/bin/cli-unit-tests";
  };
}
