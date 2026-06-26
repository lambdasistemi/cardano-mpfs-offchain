#!/usr/bin/env bash
set -euo pipefail

tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT

git diff --check

nix build .#csmt-verify-wasm --fallback --out-link "$tmpdir/csmt-verify-wasm"

entries=$(find -L "$tmpdir/csmt-verify-wasm" -mindepth 1 -maxdepth 1 -printf '%f\n' | sort)
if [ "$entries" != "csmt-verify-wasm.wasm" ]; then
  printf 'expected only csmt-verify-wasm.wasm, found:\n%s\n' "$entries" >&2
  exit 1
fi

nix build .#wasm-mpfs-verify --fallback --out-link "$tmpdir/wasm-mpfs-verify"
nix run --quiet .#format-check
