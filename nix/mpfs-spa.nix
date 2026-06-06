# Browser SPA build (#291). Reproducible spago + esbuild bundle via
# mkSpagoDerivation. npm deps come from the committed package-lock.json.
#
# `pkgs` here must already carry the purescript-overlay and
# mkSpagoDerivation overlays (see flake.nix psPkgs).
{ pkgs, cageReactorWasm ?
  null # path to mpfs-cage-reactor.wasm; integration branch passes the real reactor artifact
}:
let
  src = ../mpfs-spa;
  nodeModules = pkgs.importNpmLock.buildNodeModules {
    npmRoot = src;
    nodejs = pkgs.nodejs_20;
  };
  placeholderReactorWasm =
    pkgs.runCommand "mpfs-cage-reactor-placeholder.wasm" { } ''
      printf '\000asm\001\000\000\000' > $out
    '';
  reactorWasm =
    if cageReactorWasm == null then placeholderReactorWasm else cageReactorWasm;
in pkgs.mkSpagoDerivation {
  pname = "mpfs-spa";
  version = "1.0.0";
  inherit src;
  spagoYaml = ../mpfs-spa/spago.yaml;
  spagoLock = ../mpfs-spa/spago.lock;
  nativeBuildInputs =
    [ pkgs.purs pkgs.spago-unstable pkgs.esbuild pkgs.nodejs_20 ];
  buildPhase = ''
    ln -s ${nodeModules}/node_modules node_modules

    mkdir -p dist src/assets
    cp ${reactorWasm} src/assets/mpfs-cage-reactor.wasm
    chmod -R u+w src/assets

    esbuild src/bootstrap.js \
      --bundle \
      --outfile=dist/deps.js \
      --format=iife \
      --platform=browser \
      --loader:.wasm=binary \
      --minify

    spago bundle --offline --module Main --outfile dist/index.js

    cat dist/deps.js dist/index.js > dist/bundle.js
    mv dist/bundle.js dist/index.js
    rm dist/deps.js
  '';
  installPhase = ''
    mkdir -p $out
    cp dist/index.html $out/
    cp dist/index.js $out/
    cp src/assets/mpfs-cage-reactor.wasm $out/
  '';
}
