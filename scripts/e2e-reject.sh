#!/usr/bin/env bash
# E2E test: reject Phase 3 requests for a token
#
# Usage: ./scripts/e2e-reject.sh [TOKEN_ID]
#
# Rejects all expired (Phase 3) requests for the
# given token. The oracle keeps fees, requesters
# get refunds.

source "$(dirname "$0")/e2e-lib.sh"

TOKEN_OVERRIDE="${1:-}"
if [[ -n "$TOKEN_OVERRIDE" ]]; then
    TOKEN_ID="$TOKEN_OVERRIDE"
fi

log "E2E Reject: token=${TOKEN_ID:0:16}..."

# 1. Check pending requests
step "Checking pending requests..."
REQS=$(get_requests)
COUNT=$(echo "$REQS" | jq 'length')
step "Found $COUNT pending request(s)"

if [[ "$COUNT" -eq 0 ]]; then
    log "No pending requests to reject"
    exit 0
fi

echo "$REQS" | jq '.[].operation'

# 2. Record current root
OLD_ROOT=$(get_root)
step "Current root: $OLD_ROOT"

# 3. Submit reject tx
step "Building reject tx..."
build_sign_submit "$ORACLE_WALLET" POST /tx/reject \
    "{\"token\": \"$TOKEN_ID\", \"address\": \"$ORACLE_ADDR\"}"

# 4. Wait for requests to be consumed
step "Waiting for requests to be consumed..."
for i in $(seq 1 120); do
    NEW_COUNT=$(get_requests | jq 'length')
    if [[ "$NEW_COUNT" -lt "$COUNT" ]]; then
        step "Requests consumed: $COUNT -> $NEW_COUNT"
        break
    fi
    sleep 1
done

# 5. Verify root unchanged
NEW_ROOT=$(get_root)
if [[ "$NEW_ROOT" == "$OLD_ROOT" ]]; then
    log "REJECT OK: root unchanged ($OLD_ROOT)"
else
    log "WARNING: root changed! $OLD_ROOT -> $NEW_ROOT"
fi

log "Remaining requests: $(get_requests | jq 'length')"
