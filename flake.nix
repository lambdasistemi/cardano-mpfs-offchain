{
  description = "Merkle Patricia Forestry offchain service";
  nixConfig = {
    extra-substituters = [ "https://cache.iog.io" ];
    extra-trusted-public-keys =
      [ "hydra.iohk.io:f/Ea+s+dFdN+3Y/G+FDgSq+a5NEWhJGzdjvKNGv0/EQ=" ];
  };
  inputs = {
    haskellNix.url = "github:input-output-hk/haskell.nix";
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
      url = "github:lambdasistemi/cardano-node-clients";
    };
    cardano-node.follows = "cardano-node-clients/cardano-node";
    cardano-mpfs-onchain = {
      url =
        "github:cardano-foundation/cardano-mpfs-onchain/023d352e850f866752927818da44861478ae99e5";
    };
    ghc-wasm-meta.url =
      "gitlab:haskell-wasm/ghc-wasm-meta?host=gitlab.haskell.org";
  };

  outputs = inputs@{ self, nixpkgs, flake-parts, haskellNix, mkdocs, asciinema
    , iohkNix, CHaP, cardano-node, cardano-mpfs-onchain, cardano-node-clients
    , ghc-wasm-meta, ... }:
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
              ];
              inherit system;
            };
            cardano-node-pkgs = cardano-node.packages.${system};
            mpfs-blueprint = cardano-mpfs-onchain.packages.${system}.default;
            devnet-genesis =
              cardano-node-clients.packages.${system}.devnet-genesis;
            project = import ./nix/project.nix {
              indexState = "2025-12-07T00:00:00Z";
              inherit CHaP pkgs cardano-node-pkgs mpfs-blueprint devnet-genesis
                version;
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
          in {
            packages = {
              inherit (project.packages)
                offchain-tests client-tests e2e-tests cardano-mpfs-offchain
                mpfs-serve mpfs-devnet-server mpfs-bootstrap-genesis
                docker-image haddock;
              inherit (wasmTargets) wasm-mpfs-verify;
              default = project.packages.cardano-mpfs-offchain;
            };
            inherit (project) devShells checks apps;
          };
      };
    in {
      inherit (parts) packages devShells checks apps;
      inherit version;
    };
}
