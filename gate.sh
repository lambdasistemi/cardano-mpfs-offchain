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
  "legacy Real.Boot module exposure returned" \
  'Cardano\.MPFS\.TxBuilder\.Real\.Boot' \
  cardano-mpfs-offchain/cardano-mpfs-offchain.cabal \
  cardano-mpfs-offchain/lib \
  cardano-mpfs-offchain/test

for removed_file in \
  cardano-mpfs-offchain/lib/Cardano/MPFS/TxBuilder/Real/Boot.hs \
  cardano-mpfs-offchain/lib/Cardano/MPFS/TxBuilder/Real/Boot/Inputs.hs \
  cardano-mpfs-offchain/lib/Cardano/MPFS/TxBuilder/Real/Boot/Transaction.hs
do
  if [ -e "$removed_file" ]; then
    printf 'gate failure: removed legacy boot builder file exists: %s\n' "$removed_file" >&2
    exit 1
  fi
done

for non_boot_route in \
  TxDeleteAPI \
  TxRequestUpdateAPI \
  TxRejectAPI \
  TxUpdateAPI \
  TxRetractAPI \
  TxSweepAPI
do
  check_present \
    "non-boot write route alias is missing: $non_boot_route" \
    "$non_boot_route" \
    cardano-mpfs-api/lib/Cardano/MPFS/API.hs \
    cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/API.hs
done

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
  "boot facts verifier imports transaction grammar" \
  'Cardano\.Ledger\.Api\.Tx|TxView|verifyTxInputBinding|unsigned_tx_cbor|Tx ConwayEra' \
  cardano-mpfs-client/lib/Cardano/MPFS/Client/Facts.hs \
  cardano-mpfs-client/test/Cardano/MPFS/Client/BootFactsSpec.hs

check_absent \
  "end facts verifier imports transaction grammar" \
  'Cardano\.Ledger\.Api\.Tx|TxView|verifyTxInputBinding|unsigned_tx_cbor|Tx ConwayEra' \
  cardano-mpfs-client/lib/Cardano/MPFS/Client/Facts.hs \
  cardano-mpfs-client/lib/Cardano/MPFS/Client/Verify/Completeness.hs

check_absent \
  "request-insert facts verifier imports transaction grammar" \
  'Cardano\.Ledger\.Api\.Tx|TxView|verifyTxInputBinding|unsigned_tx_cbor|Tx ConwayEra' \
  cardano-mpfs-client/lib/Cardano/MPFS/Client/Facts.hs

nix develop --quiet -c just ci
