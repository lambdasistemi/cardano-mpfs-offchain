#!/usr/bin/env bash
set -euo pipefail

check_cabal_version() {
  local manifest_version pvp_version cabal_file cabal_version
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
}

git diff --check
nix build --quiet .#cardano-mpfs-offchain .#wasm-mpfs-verify --fallback
just unit "CORS"
just ci
just e2e
check_cabal_version

echo "NOTE: dep-graph-drift is enforced by .github/workflows/deploy-docs.yaml on pull_request."
