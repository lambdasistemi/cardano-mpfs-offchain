# Browser SPA build (#291). Reproducible spago + esbuild bundle via
# mkSpagoDerivation. npm deps come from the committed package-lock.json.
#
# `pkgs` here must already carry the purescript-overlay and
# mkSpagoDerivation overlays (see flake.nix psPkgs).
{ pkgs, mpfsBlueprint, cageReactorWasm ?
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
    [ pkgs.purs pkgs.spago-unstable pkgs.esbuild pkgs.nodejs_20 pkgs.jq ];
  buildPhase = ''
    ln -s ${nodeModules}/node_modules node_modules

    extract_blueprint_value() {
      local field="$1"
      local title_prefix="$2"
      local description="$3"

      jq -er \
        --arg field "$field" \
        --arg title_prefix "$title_prefix" \
        --arg description "$description" \
        '[.validators[]
          | select(.title | startswith($title_prefix))
          | .[$field]]
         | unique
         | if length == 1 and .[0] != null and .[0] != ""
           then .[0]
           else error("expected exactly one " + $description)
           end' \
        ${mpfsBlueprint}
    }

    cageScriptHash="$(extract_blueprint_value hash state.state. cage-script-hash)"
    cageScriptBytes="$(extract_blueprint_value compiledCode state.state. cage-script-bytes)"
    requestScriptBytes="$(extract_blueprint_value compiledCode request.request. request-script-bytes)"

    substituteInPlace src/MpfsSpa/Config.purs \
      --replace-fail "__MPFS_CAGE_SCRIPT_HASH__" "$cageScriptHash" \
      --replace-fail "__MPFS_CAGE_SCRIPT_BYTES__" "$cageScriptBytes" \
      --replace-fail "__MPFS_REQUEST_SCRIPT_BYTES__" "$requestScriptBytes"

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
