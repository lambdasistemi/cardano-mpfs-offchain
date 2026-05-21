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
  '"/facts/request/update"' \
  '"/facts/retract"' \
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
  "legacy request-update tx route returned" \
  '"/tx/request/update"|TxRequestUpdateAPI|txUpdateValueHandler|"tx" :> "request" :> "update"|requestUpdateTx' \
  docs/assets/swagger.json \
  cardano-mpfs-api/lib/Cardano/MPFS/API.hs \
  cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/API.hs \
  cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/Server.hs \
  cardano-mpfs-client/lib/Cardano/MPFS/Client/Http.hs \
  cardano-mpfs-client/lib/Cardano/MPFS/Client.hs

check_absent \
  "legacy end tx route returned" \
  '"/tx/end"|TxEndAPI|txEndHandler|"tx" :> "end"' \
  docs/assets/swagger.json \
  cardano-mpfs-api/lib/Cardano/MPFS/API.hs \
  cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/API.hs \
  cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/Server.hs

check_absent \
  "legacy retract tx route returned" \
  '"/tx/retract"|TxRetractAPI|txRetractHandler|"tx" :> "retract"' \
  docs/assets/swagger.json \
  cardano-mpfs-api/lib/Cardano/MPFS/API.hs \
  cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/API.hs \
  cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/Server.hs \
  cardano-mpfs-client/lib/Cardano/MPFS/Client/Http.hs

check_present \
  "request-update facts API is not wired" \
  'FactsRequestUpdateAPI' \
  cardano-mpfs-api/lib/Cardano/MPFS/API.hs

check_present \
  "request-update facts wire type is missing" \
  'RequestUpdateFacts' \
  cardano-mpfs-api/lib/Cardano/MPFS/API/Types/Facts.hs

check_present \
  "request-update facts server handler is missing" \
  'factsRequestUpdateHandler' \
  cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/Server.hs

check_present \
  "request-update facts assembly helper is missing" \
  'mkRequestUpdateFacts' \
  cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/Types/Facts.hs

check_present \
  "request-update verifier is not wired" \
  'VerifiedRequestUpdateFacts.*|verifyRequestUpdateFacts' \
  cardano-mpfs-client/lib/Cardano/MPFS/Client/Facts.hs

check_present \
  "request-update verifier tests are missing" \
  'verifyRequestUpdateFacts' \
  cardano-mpfs-client/test/Cardano/MPFS/Client/RequestUpdateFactsSpec.hs

check_present \
  "request-update cage helper is not wired" \
  'requestUpdateCageTx' \
  cardano-mpfs-client/lib/Cardano/MPFS/Client/Cage/Request.hs

check_present \
  "request-update cage tests are missing" \
  'requestUpdateCageTx|legacy-request-update.cbor' \
  cardano-mpfs-client/test/Cardano/MPFS/Client/Cage/RequestSpec.hs

check_present \
  "request-update facts matrix route is missing" \
  '/facts/request/update' \
  cardano-mpfs-offchain/e2e-test/Cardano/MPFS/E2E/FactsMatrixSpec.hs

check_present \
  "request-update facts matrix verifier is missing" \
  'verifyRequestUpdateFacts' \
  cardano-mpfs-offchain/e2e-test/Cardano/MPFS/E2E/FactsMatrixSpec.hs

check_present \
  "request-update facts matrix builder is missing" \
  'requestUpdateCageTx' \
  cardano-mpfs-offchain/e2e-test/Cardano/MPFS/E2E/FactsMatrixSpec.hs

check_present \
  "request-update live legacy-route absence is missing" \
  '/tx/request/update' \
  cardano-mpfs-offchain/e2e-test/Cardano/MPFS/E2E/FactsMatrixSpec.hs

if [ ! -s specs/266-request-update-fact-provider-pivot/test-vectors/legacy-request-update.cbor ]; then
  printf 'gate failure: legacy request-update CBOR vector is missing\n' >&2
  exit 1
fi

check_absent \
  "request-update verifier surface imports transaction grammar" \
  'Cardano\.Ledger\.Api\.Tx' \
  cardano-mpfs-client/lib/Cardano/MPFS/Client/Facts.hs \
  cardano-mpfs-client/test/Cardano/MPFS/Client/RequestUpdateFactsSpec.hs

nix develop --quiet -c just ci
