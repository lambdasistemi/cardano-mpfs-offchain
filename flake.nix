{
  description = "Merkle Patricia Forestry offchain service";
  nixConfig = {
    extra-substituters = [ "https://cache.iog.io" ];
    extra-trusted-public-keys =
      [ "hydra.iohk.io:f/Ea+s+dFdN+3Y/G+FDgSq+a5NEWhJGzdjvKNGv0/EQ=" ];
  };
  inputs = {
    haskellNix.url =
      "github:input-output-hk/haskell.nix/8b447d7f57d62fab9249f79bb916bc891e29b9d0";
    nixpkgs.follows = "haskellNix/nixpkgs-unstable";
    flake-parts.url = "github:hercules-ci/flake-parts";
    mkdocs.url = "github:paolino/dev-assets?dir=mkdocs";
    asciinema.url = "github:paolino/dev-assets?dir=asciinema";
    iohkNix = {
      url = "github:input-output-hk/iohk-nix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    CHaP = {
      url = "github:intersectmbo/cardano-haskell-packages?ref=repo";
      flake = false;
    };
    cardano-node-clients = {
      url =
        "github:lambdasistemi/cardano-node-clients/e4b01cb9efdf88e99934cf7a09fed0e25bad1019";
    };
    cardano-node.follows = "cardano-node-clients/cardano-node";
    cardano-mpfs-onchain = {
      url =
        "github:cardano-foundation/cardano-mpfs-onchain/d352d25dbe821cd518e8d51d5cc069b015a56533";
    };
    ghc-wasm-meta.url =
      "gitlab:haskell-wasm/ghc-wasm-meta?host=gitlab.haskell.org";
    purescript-overlay = {
      url = "github:paolino/purescript-overlay/fix/remove-nodePackages";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    mkSpagoDerivation = {
      url = "github:jeslie0/mkSpagoDerivation";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = inputs@{ self, nixpkgs, flake-parts, haskellNix, mkdocs, asciinema
    , iohkNix, CHaP, cardano-node, cardano-mpfs-onchain, cardano-node-clients
    , ghc-wasm-meta, purescript-overlay, mkSpagoDerivation, ... }:
    let
      version = self.dirtyShortRev or self.shortRev;
      parts = flake-parts.lib.mkFlake { inherit inputs; } {
        systems = [ "x86_64-linux" "aarch64-darwin" ];
        perSystem = { system, ... }:
          let
            pkgs = import nixpkgs {
              overlays = [
                iohkNix.overlays.crypto
                haskellNix.overlay
                iohkNix.overlays.haskell-nix-crypto
                iohkNix.overlays.cardano-lib
                (_final: prev: { lzma = prev.xz; })
              ];
              inherit system;
            };
            cardano-node-pkgs = cardano-node.packages.${system};
            # Validator identity source for the server and SPA. The wasm
            # Haskell fork pins are separate library-code inputs; this unifies
            # only cage/request validator identity, not those rev pins.
            mpfs-blueprint = cardano-mpfs-onchain.packages.${system}.default;
            devnet-genesis =
              cardano-node-clients.packages.${system}.devnet-genesis;
            project = import ./nix/project.nix {
              indexState = "2026-04-17T00:00:00Z";
              inherit CHaP pkgs system cardano-node-pkgs mpfs-blueprint
                devnet-genesis version;
              mkdocs = mkdocs.packages.${system};
              asciinema = asciinema.packages.${system};
            };
            wasmTargets = import ./nix/wasm-targets.nix {
              inherit pkgs;
              libWasm = import ./nix/wasm { lib = pkgs.lib; };
              ghcWasmMeta = ghc-wasm-meta.packages.${system}.all_9_12;
              wasiSdk = ghc-wasm-meta.packages.${system}.wasi-sdk;
              chap = CHaP;
              src = ./.;
            };
            # Separate nixpkgs instance carrying the PureScript toolchain
            # overlays; kept apart from the haskell.nix `pkgs` above to avoid
            # cross-overlay interference (#291 browser SPA).
            psPkgs = import nixpkgs {
              inherit system;
              overlays = [
                purescript-overlay.overlays.default
                mkSpagoDerivation.overlays.default
              ];
            };
            cardanoAddressPkgs = import nixpkgs {
              inherit system;
              config.allowBroken = true;
            };
            cardanoAddressHaskellPackages =
              cardanoAddressPkgs.haskellPackages.override {
                overrides = _hfinal: hprev: {
                  bech32 =
                    cardanoAddressPkgs.haskell.lib.dontCheck hprev.bech32;
                  cardano-addresses = cardanoAddressPkgs.haskell.lib.dontCheck
                    hprev.cardano-addresses;
                };
              };
            cardanoAddress = cardanoAddressHaskellPackages.cardano-addresses;
            mpfs-spa = import ./nix/mpfs-spa.nix {
              pkgs = psPkgs;
              mpfsBlueprint = mpfs-blueprint;
              # Integration (298): feed the real reactor wasm built by the
              # wasm target instead of the placeholder, so the SPA bundles
              # and loads the live mpfs-cage-reactor.
              cageReactorWasm =
                "${wasmTargets.wasm-mpfs-verify}/mpfs-cage-reactor.wasm";
            };
            test-playwright-spa = pkgs.writeShellApplication {
              name = "test-playwright-spa";
              runtimeInputs = [
                pkgs.playwright-test
                pkgs.nodejs_20
                pkgs.python3
                pkgs.coreutils
                pkgs.bash
                pkgs.gnugrep
                cardano-node-pkgs.cardano-node
                cardano-node-pkgs.cardano-cli
              ];
              text = ''
                export MPFS_DEVNET_SERVER="${project.packages.mpfs-devnet-server}/bin/mpfs-devnet-server"
                export MPFS_BLUEPRINT="${mpfs-blueprint}"
                export E2E_GENESIS_DIR="${devnet-genesis}"
                export MPFS_SPA_SITE_DIR="${mpfs-spa}"
                export PLAYWRIGHT_BROWSERS_PATH="${pkgs.playwright-driver.browsers}"
                export PLAYWRIGHT_SKIP_BROWSER_DOWNLOAD=1
                exec bash ${./scripts/e2e-spa-devnet.sh}
              '';
            };
            test-playwright-spa-preprod = pkgs.writeShellApplication {
              name = "test-playwright-spa-preprod";
              runtimeInputs = [
                pkgs.playwright-test
                pkgs.nodejs_20
                pkgs.python3
                pkgs.coreutils
                pkgs.bash
                cardano-node-pkgs.cardano-cli
                cardanoAddress
              ];
              text = ''
                export MPFS_SPA_SITE_DIR="${mpfs-spa}"
                export PLAYWRIGHT_BROWSERS_PATH="${pkgs.playwright-driver.browsers}"
                export PLAYWRIGHT_SKIP_BROWSER_DOWNLOAD=1
                export MPFS_BASE_URL="''${MPFS_BASE_URL:-https://umpfs.plutimus.com}"
                export MPFS_SIGNER_WALLET="''${MPFS_SIGNER_WALLET:-/code/moog/tmp/requester.json}"
                exec bash ${./scripts/e2e-spa-preprod.sh}
              '';
            };
          in {
            packages = {
              inherit (project.packages)
                offchain-tests client-tests workflows-tests e2e-tests
                cardano-mpfs-offchain mpfs-serve mpfs-devnet-server
                mpfs-bootstrap-genesis docker-image haddock;
              inherit (wasmTargets) wasm-mpfs-verify;
              inherit mpfs-spa;
              default = project.packages.cardano-mpfs-offchain;
            };
            devShells = project.devShells // {
              mpfs-spa = psPkgs.mkShell {
                packages = [
                  psPkgs.purs
                  psPkgs.spago-unstable
                  psPkgs.purs-tidy-bin.purs-tidy-0_10_0
                  psPkgs.purescript-language-server
                  psPkgs.esbuild
                  psPkgs.nodejs_20
                  psPkgs.just
                ];
              };
            };
            checks = project.checks;
            apps = project.apps // {
              test-playwright-spa = {
                type = "app";
                program = "${test-playwright-spa}/bin/test-playwright-spa";
              };
              test-playwright-spa-preprod = {
                type = "app";
                program =
                  "${test-playwright-spa-preprod}/bin/test-playwright-spa-preprod";
              };
            };
          };
      };
    in {
      inherit (parts) packages devShells checks apps;
      inherit version;
    };
}
