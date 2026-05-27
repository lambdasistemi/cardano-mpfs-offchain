#!/usr/bin/env bash
set -euo pipefail

check_absent() {
  local description="${1:?description}"
  local pattern="${2:?pattern}"
  shift 2

  if rg -n "$pattern" "$@"; then
    printf 'gate failure: %s\n' "$description" >&2
    return 1
  fi
}

facts_update_present() {
  rg -n '"/facts/update"|FactsUpdateAPI|"facts" :> "update"|factsUpdateHandler|updateFacts' \
    docs/assets/swagger.json \
    cardano-mpfs-api/lib/Cardano/MPFS/API.hs \
    cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/API.hs \
    cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/Server.hs \
    cardano-mpfs-client/lib/Cardano/MPFS/Client/Http.hs \
    >/dev/null
}

git diff --check

if facts_update_present; then
  check_absent \
    "legacy update tx route returned after facts update route landed" \
    '"/tx/update"|TxUpdateAPI|txUpdateHandler|"tx" :> "update"|updateTx' \
    docs/assets/swagger.json \
    cardano-mpfs-api/lib/Cardano/MPFS/API.hs \
    cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/API.hs \
    cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/Server.hs \
    cardano-mpfs-client/lib/Cardano/MPFS/Client/Http.hs
fi

nix develop --quiet -c just ci
