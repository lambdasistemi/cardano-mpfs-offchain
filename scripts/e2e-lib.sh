#!/usr/bin/env bash
# Shared E2E test library for preprod MPFS testing
#
# Source this file from test scripts:
#   source "$(dirname "$0")/e2e-lib.sh"

set -euo pipefail

# --- Configuration ---
API_URL="${MPFS_API_URL:-https://umpfs.plutimus.com}"
ORACLE_WALLET="${ORACLE_WALLET:-$HOME/secrets/preprod-wallet/wallet.json}"
REQUESTER_WALLET="${REQUESTER_WALLET:-$HOME/secrets/preprod-wallet/requester.json}"
WALLET_SIGN="nix run /code/cardano-wallet-sign-init#exe --"

# Hex-encoded serialized addresses (preprod)
ORACLE_ADDR="6072b0da51f4ae5f85ed89aa189698b78f3ab2b7a04da5b106b4cab2cf"
REQUESTER_ADDR="60a26fcfafd7913e44253c30264f82b4daa48d5ecd195ffe5e45f7f7cf"

TOKEN_ID="${MPFS_TOKEN_ID:-0cbe0684a8d40705ca0b86cb6f328bbef4495fe7fddb82235ae8eb61f5be9ce0}"

# --- Helpers ---

log() { echo "=== $*" >&2; }
step() { echo "--- $*" >&2; }
fail() { echo "FAIL: $*" >&2; exit 1; }

# Call the MPFS API. Usage: api GET /status
api() {
    local method="$1" path="$2"
    shift 2
    local url="${API_URL}${path}"
    if [[ "$method" == "GET" ]]; then
        curl -sf "$url" "$@"
    else
        curl -sf -X "$method" -H "Content-Type: application/json" "$url" "$@"
    fi
}

# Build, sign, and submit a transaction.
# Usage: build_sign_submit WALLET POST_PATH JSON_BODY
build_sign_submit() {
    local wallet="$1" method="$2" path="$3" body="$4"

    step "POST $path"
    local raw_response
    raw_response=$(api "$method" "$path" -d "$body")
    # API returns a JSON string (hex-encoded CBOR tx), strip quotes
    local tx_hex
    tx_hex=$(echo "$raw_response" | jq -r '.')
    if [[ -z "$tx_hex" || "$tx_hex" == "null" ]]; then
        echo "Build response: $raw_response" >&2
        fail "Failed to build tx at $path"
    fi

    step "Signing with $(basename "$wallet")..."
    local signed
    signed=$($WALLET_SIGN sign -w "$wallet" --tx "$tx_hex")

    step "Submitting..."
    local result
    result=$(api POST /tx/submit -d "{\"tx\": \"$signed\"}")
    echo "$result"
}

# Wait for a transaction to be confirmed (poll token state for root change).
# Usage: await_tx OLD_ROOT [TIMEOUT_SECONDS]
await_tx() {
    local old_root="$1"
    local timeout="${2:-120}"
    step "Waiting for tx confirmation (up to ${timeout}s)..."
    for i in $(seq 1 "$timeout"); do
        local current_root
        current_root=$(api GET "/tokens/$TOKEN_ID" | jq -r '.root')
        if [[ "$current_root" != "$old_root" ]]; then
            step "Confirmed! Root changed: ${old_root:0:16}... -> ${current_root:0:16}..."
            echo "$current_root"
            return 0
        fi
        sleep 1
    done
    fail "Tx not confirmed after ${timeout}s (root still $old_root)"
}

# Get current token root
get_root() {
    api GET "/tokens/$TOKEN_ID" | jq -r '.root'
}

# Get pending requests
get_requests() {
    api GET "/tokens/$TOKEN_ID/requests"
}

# Wait for at least N pending requests. Usage: await_requests N [TIMEOUT]
await_requests() {
    local expected="$1"
    local timeout="${2:-120}"
    step "Waiting for $expected pending request(s) (up to ${timeout}s)..."
    for i in $(seq 1 "$timeout"); do
        local count
        count=$(api GET "/tokens/$TOKEN_ID/requests" | jq 'length')
        if [[ "$count" -ge "$expected" ]]; then
            step "Got $count request(s)"
            return 0
        fi
        sleep 1
    done
    fail "Only $(api GET "/tokens/$TOKEN_ID/requests" | jq 'length') requests after ${timeout}s (expected $expected)"
}

# Convert a string to hex (for keys/values)
to_hex() {
    printf '%s' "$1" | xxd -p | tr -d '\n'
}
