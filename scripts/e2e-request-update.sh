#!/usr/bin/env bash
# E2E test: update a key's value via request-update
#
# Usage: ./scripts/e2e-request-update.sh KEY OLD_VALUE NEW_VALUE
#
# Submits an update-value request, then processes it.

source "$(dirname "$0")/e2e-lib.sh"

KEY="${1:?Usage: e2e-request-update.sh KEY OLD_VALUE NEW_VALUE}"
OLD_VALUE="${2:?Usage: e2e-request-update.sh KEY OLD_VALUE NEW_VALUE}"
NEW_VALUE="${3:?Usage: e2e-request-update.sh KEY OLD_VALUE NEW_VALUE}"
KEY_HEX=$(to_hex "$KEY")
OLD_HEX=$(to_hex "$OLD_VALUE")
NEW_HEX=$(to_hex "$NEW_VALUE")

log "E2E Update Value: key=$KEY old=$OLD_VALUE new=$NEW_VALUE"
log "Token: $TOKEN_ID"

# 1. Record current root
OLD_ROOT=$(get_root)
step "Current root: $OLD_ROOT"

# 2. Submit update request
step "Submitting update-value request..."
build_sign_submit "$ORACLE_WALLET" POST /tx/request/update \
    "{\"token\": \"$TOKEN_ID\", \"key\": \"$KEY_HEX\", \"old_value\": \"$OLD_HEX\", \"new_value\": \"$NEW_HEX\", \"address\": \"$ORACLE_ADDR\"}"

# 3. Wait for request to appear
await_requests 1
step "Pending requests:"
get_requests | jq .

# 4. Process update
step "Building update tx..."
build_sign_submit "$ORACLE_WALLET" POST /tx/update \
    "{\"token\": \"$TOKEN_ID\", \"address\": \"$ORACLE_ADDR\"}"

# 5. Wait for confirmation
NEW_ROOT=$(await_tx "$OLD_ROOT")

log "UPDATE VALUE OK: $KEY = $OLD_VALUE -> $NEW_VALUE"
log "Root: $OLD_ROOT -> $NEW_ROOT"
