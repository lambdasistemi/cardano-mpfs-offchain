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
    cardano-node = { url = "github:IntersectMBO/cardano-node/10.5.4"; };
    cardano-mpfs-onchain = { url = "github:paolino/cardano-mpfs-onchain"; };
  };

  outputs = inputs@{ self, nixpkgs, flake-parts, haskellNix, mkdocs, asciinema
    , iohkNix, CHaP, cardano-node, cardano-mpfs-onchain, ... }:
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
            project = import ./nix/project.nix {
              indexState = "2025-12-07T00:00:00Z";
              inherit CHaP pkgs cardano-node-pkgs mpfs-blueprint;
              mkdocs = mkdocs.packages.${system};
              asciinema = asciinema.packages.${system};
            };
          in {
            packages = {
              inherit (project.packages)
                offchain-tests e2e-tests cardano-mpfs-offchain
                mpfs-bootstrap-genesis haddock;
              default = project.packages.cardano-mpfs-offchain;
            };
            inherit (project) devShells;
          };
      };
    in {
      inherit (parts) packages devShells;
      inherit version;
    };
}
