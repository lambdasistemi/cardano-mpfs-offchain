#!/usr/bin/env bash
set -euo pipefail

echo "== focused reactor tests =="
nix develop --quiet -c just unit-client "runEnvelope"

echo "== wasm reactor build =="
nix build .#wasm-mpfs-verify --fallback

echo "== CI build mirror =="
nix build --quiet \
  .#cardano-mpfs-offchain \
  .#e2e-tests \
  .#wasm-mpfs-verify \
  .#checks.x86_64-linux.swagger-up-to-date \
  --fallback

echo "== native unit suites =="
just unit
just unit-client
just unit-workflows
just unit-cli

echo "== formatting and lint =="
just format-check
just hlint

echo "== e2e =="
just e2e

echo "gate ok"
