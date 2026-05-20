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

check_present() {
  local description="${1:?description}"
  local pattern="${2:?pattern}"
  shift 2

  if ! rg -n "$pattern" "$@" >/dev/null; then
    printf 'gate failure: %s\n' "$description" >&2
    return 1
  fi
}

git diff --check

for route in \
  '"/facts/boot"' \
  '"/facts/request/insert"' \
  '"/facts/request/delete"' \
  '"/facts/end"'
do
  check_present \
    "facts Swagger path is missing: $route" \
    "$route" \
    docs/assets/swagger.json
done

check_absent \
  "legacy boot tx route returned" \
  '"/tx/boot"|TxBootAPI|txBootHandler|"tx" :> "boot"' \
  docs/assets/swagger.json \
  cardano-mpfs-api/lib/Cardano/MPFS/API.hs \
  cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/API.hs \
  cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/Server.hs

check_absent \
  "legacy request-insert tx route returned" \
  '"/tx/request/insert"|TxInsertAPI|txInsertHandler|"tx" :> "request" :> "insert"|requestInsertTx' \
  docs/assets/swagger.json \
  cardano-mpfs-api/lib/Cardano/MPFS/API.hs \
  cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/API.hs \
  cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/Server.hs \
  cardano-mpfs-client/lib/Cardano/MPFS/Client/Http.hs

check_absent \
  "legacy request-delete tx route returned" \
  '"/tx/request/delete"|TxDeleteAPI|txDeleteHandler|"tx" :> "request" :> "delete"|requestDeleteTx' \
  docs/assets/swagger.json \
  cardano-mpfs-api/lib/Cardano/MPFS/API.hs \
  cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/API.hs \
  cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/Server.hs \
  cardano-mpfs-client/lib/Cardano/MPFS/Client/Http.hs

check_absent \
  "legacy end tx route returned" \
  '"/tx/end"|TxEndAPI|txEndHandler|"tx" :> "end"' \
  docs/assets/swagger.json \
  cardano-mpfs-api/lib/Cardano/MPFS/API.hs \
  cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/API.hs \
  cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/Server.hs

nix develop --quiet -c just ci
