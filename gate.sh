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

check_absent \
  "legacy boot tx API/server route returned" \
  'TxBootAPI|txBootHandler|mkBootTxResponse|"tx" :> "boot"' \
  cardano-mpfs-api/lib/Cardano/MPFS/API.hs \
  cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/API.hs \
  cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/Server.hs \
  cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/Types.hs

check_absent \
  "legacy boot tx Swagger route/schema returned" \
  '"/tx/boot"|"UnsignedTxResponse"' \
  docs/assets/swagger.json

check_present \
  "facts boot Swagger path is missing" \
  '"/facts/boot"' \
  docs/assets/swagger.json

check_present \
  "BootFacts Swagger schema is missing" \
  '"BootFacts"' \
  docs/assets/swagger.json

check_absent \
  "legacy end tx API/server route returned" \
  'TxEndAPI|txEndHandler|"tx" :> "end"' \
  cardano-mpfs-api/lib/Cardano/MPFS/API.hs \
  cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/API.hs \
  cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/Server.hs

check_absent \
  "legacy end tx Swagger route returned" \
  '"/tx/end"' \
  docs/assets/swagger.json

check_present \
  "facts end Swagger path is missing" \
  '"/facts/end"' \
  docs/assets/swagger.json

check_absent \
  "legacy request-insert tx API/server route returned" \
  'TxInsertAPI|txInsertHandler|"tx" :> "request" :> "insert"|"tx/request/insert"|requestInsertTx' \
  cardano-mpfs-api/lib/Cardano/MPFS/API.hs \
  cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/API.hs \
  cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/Server.hs \
  cardano-mpfs-client/lib/Cardano/MPFS/Client/Http.hs \
  cardano-mpfs-client/test/Cardano/MPFS/Client/HttpSpec.hs

check_absent \
  "legacy request-insert Swagger route returned" \
  '"/tx/request/insert"' \
  docs/assets/swagger.json

check_present \
  "facts request-insert Swagger path is missing" \
  '"/facts/request/insert"' \
  docs/assets/swagger.json

check_present \
  "RequestInsertFacts Swagger schema is missing" \
  '"RequestInsertFacts"' \
  docs/assets/swagger.json

check_absent \
  "legacy request-delete tx API/server route returned" \
  'TxDeleteAPI|txDeleteHandler|"tx" :> "request" :> "delete"|"tx/request/delete"|requestDeleteTx' \
  cardano-mpfs-api/lib/Cardano/MPFS/API.hs \
  cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/API.hs \
  cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/Server.hs \
  cardano-mpfs-client/lib/Cardano/MPFS/Client/Http.hs \
  cardano-mpfs-client/test/Cardano/MPFS/Client/HttpSpec.hs

check_absent \
  "legacy request-delete Swagger route returned" \
  '"/tx/request/delete"' \
  docs/assets/swagger.json

check_present \
  "facts request-delete Swagger path is missing" \
  '"/facts/request/delete"' \
  docs/assets/swagger.json

check_present \
  "RequestDeleteFacts Swagger schema is missing" \
  '"RequestDeleteFacts"' \
  docs/assets/swagger.json

for retained_route in \
  TxRequestUpdateAPI \
  TxRejectAPI \
  TxUpdateAPI \
  TxRetractAPI \
  TxSweepAPI
do
  check_present \
    "retained write route alias is missing: $retained_route" \
    "$retained_route" \
    cardano-mpfs-api/lib/Cardano/MPFS/API.hs \
    cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/API.hs
done

check_absent \
  "facts verifiers import transaction grammar" \
  'Cardano\.Ledger\.Api\.Tx|TxView|verifyTxInputBinding|unsigned_tx_cbor|Tx ConwayEra' \
  cardano-mpfs-client/lib/Cardano/MPFS/Client/Facts.hs \
  cardano-mpfs-client/test/Cardano/MPFS/Client/BootFactsSpec.hs \
  cardano-mpfs-client/lib/Cardano/MPFS/Client/Verify/Completeness.hs

nix build --quiet .#client-tests
nix develop --quiet -c just ci
