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
    cardano-ledger-wasm = {
      url =
        "github:lambdasistemi/cardano-ledger-wasm/845877fde0907b58b150a2c8302033b4e73e9061";
    };
    ghc-wasm-meta.url =
      "gitlab:haskell-wasm/ghc-wasm-meta?host=gitlab.haskell.org";
  };

  outputs = inputs@{ self, nixpkgs, flake-parts, haskellNix, mkdocs, asciinema
    , iohkNix, CHaP, cardano-node, cardano-mpfs-onchain, cardano-node-clients
    , cardano-ledger-wasm, ghc-wasm-meta, ... }:
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
            # Validator identity source for the server and wasm clients. The
            # wasm Haskell fork pins are separate library-code inputs; this
            # unifies only cage/request validator identity, not those rev pins.
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
              libWasm = cardano-ledger-wasm.lib.wasm;
              ghcWasmMeta = ghc-wasm-meta.packages.${system}.all_9_12;
              wasiSdk = ghc-wasm-meta.packages.${system}.wasi-sdk;
              chap = CHaP;
              src = import ./nix/clean-src.nix { inherit (pkgs) lib; src = ./.; };
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
          in {
            packages = {
              inherit (project.packages)
                offchain-tests client-tests workflows-tests e2e-tests
                cardano-mpfs-offchain mpfs-serve mpfs-cli mpfs-devnet-server
                mpfs-bootstrap-genesis docker-image haddock;
              inherit (wasmTargets) wasm-mpfs-verify csmt-verify-wasm;
              default = project.packages.cardano-mpfs-offchain;
            };
            devShells = project.devShells;
            checks = project.checks;
            apps = project.apps;
          };
      };
    in {
      inherit (parts) packages devShells checks apps;
      inherit version;
    };
}
