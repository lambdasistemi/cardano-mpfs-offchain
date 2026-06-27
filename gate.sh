#!/usr/bin/env bash
set -euo pipefail

echo "== wasm keep-invariant =="
nix build --fallback --quiet .#wasm-mpfs-verify .#csmt-verify-wasm

echo "== flake check =="
nix flake check

echo "== CI build gate =="
nix build --quiet \
  .#offchain-tests \
  .#client-tests \
  .#workflows-tests \
  .#e2e-tests \
  .#cardano-mpfs-offchain \
  .#mpfs-cli \
  .#docker-image \
  .#checks.x86_64-linux.swagger-up-to-date \
  .#devShells.x86_64-linux.default.inputDerivation

echo "== CI test apps =="
nix run --quiet .#unit-tests
nix run --quiet .#client-unit-tests
nix run --quiet .#workflows-unit-tests
nix run --quiet .#cli-unit-tests
nix run --quiet .#e2e-tests

echo "== Cabal version matches manifest =="
manifest_version=$(sed -n 's/.*"\.":[[:space:]]*"\([^"]*\)".*/\1/p' .release-please-manifest.json)
if [ -z "$manifest_version" ]; then
  echo "::error::.release-please-manifest.json does not contain a root package version"
  exit 1
fi
pvp_version="$manifest_version"
if [[ "$pvp_version" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  pvp_version="${pvp_version}.0"
fi
for cabal_file in \
  cardano-mpfs-offchain/cardano-mpfs-offchain.cabal \
  cardano-mpfs-cli/cardano-mpfs-cli.cabal
do
  cabal_version=$(sed -n 's/^version:[[:space:]]*\([0-9][0-9.]*\).*/\1/p' "$cabal_file" | head -1)
  echo "manifest=$manifest_version pvp=$pvp_version $cabal_file=$cabal_version"
  if [ "$cabal_version" != "$pvp_version" ]; then
    echo "::error::$cabal_file version ($cabal_version) does not match release manifest PVP version ($pvp_version)"
    exit 1
  fi
done

echo "== retired SPA references =="
if grep -RIn 'mpfs-spa\|test-playwright-spa\|e2e-spa' \
  --include='*.nix' \
  --include='*.yml' \
  --include='*.yaml' \
  --include='justfile' \
  --include='*.md' \
  flake.nix nix justfile .github README.md docs; then
  echo "::error::retired SPA references remain"
  exit 1
fi
