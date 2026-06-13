#!/usr/bin/env bash
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

git diff --check
nix develop --quiet -c just format-check
nix develop --quiet -c just unit-client "Verify"
nix build --quiet .#wasm-mpfs-verify
nix build --quiet .#mpfs-spa
nix develop --quiet -c just ci
