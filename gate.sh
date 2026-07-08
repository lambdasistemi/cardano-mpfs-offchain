#!/usr/bin/env bash
set -euo pipefail

echo "== git diff --check =="
git diff --check

echo "== dev-shell cabal build =="
nix develop --quiet -c cabal build all -O0 --enable-tests --enable-benchmarks

echo "== affected client cage/verifier specs =="
nix develop --quiet -c just unit-client "bootCageTx"
nix develop --quiet -c just unit-client "requestInsertCageTx"
nix develop --quiet -c just unit-client "requestDeleteCageTx"
nix develop --quiet -c just unit-client "requestUpdateCageTx"
nix develop --quiet -c just unit-client "updateCageTx"
nix develop --quiet -c just unit-client "endCageTx"
nix develop --quiet -c just unit-client "retractCageTx"
nix develop --quiet -c just unit-client "rejectCageTx"
nix develop --quiet -c just unit-client "verifyEndFacts"
nix develop --quiet -c just unit-client "Read-side verifiers"
nix develop --quiet -c just unit-client "runEnvelope"

echo "== flake build/check targets =="
nix build --quiet \
  .#cardano-mpfs-offchain \
  .#wasm-mpfs-verify \
  .#checks.x86_64-linux.swagger-up-to-date

echo "== format-check =="
nix develop --quiet -c just format-check

echo "== hlint =="
nix develop --quiet -c just hlint
