# Browser SPA build (#291). Reproducible spago + esbuild bundle via
# mkSpagoDerivation; npm deps (react, react-dom, @mui/material, @emotion/*)
# come from the committed package-lock.json through importNpmLock and are
# resolved into the bundle by spago's internal esbuild.
#
# `pkgs` here must already carry the purescript-overlay and
# mkSpagoDerivation overlays (see flake.nix psPkgs).
{ pkgs }:
let
  src = ../mpfs-spa;
  nodeModules = pkgs.importNpmLock.buildNodeModules {
    npmRoot = src;
    nodejs = pkgs.nodejs_20;
  };
in
pkgs.mkSpagoDerivation {
  pname = "mpfs-spa";
  version = "1.0.0";
  inherit src;
  spagoYaml = ../mpfs-spa/spago.yaml;
  spagoLock = ../mpfs-spa/spago.lock;
  nativeBuildInputs = [
    pkgs.purs
    pkgs.spago-unstable
    pkgs.esbuild
    pkgs.nodejs_20
  ];
  buildPhase = ''
    ln -s ${nodeModules}/node_modules node_modules
    spago bundle --offline --module Main --outfile dist/index.js
  '';
  installPhase = ''
    mkdir -p $out
    cp dist/index.html $out/
    cp dist/index.js $out/
    # Placeholder for the real cage-helper WASM artifact; dropped in when
    # %351 ships green wasm (PR #301). The SPA loads its CageHelpers from a
    # mock until then, so this file is intentionally empty for now.
    touch $out/mpfs.wasm
  '';
}
