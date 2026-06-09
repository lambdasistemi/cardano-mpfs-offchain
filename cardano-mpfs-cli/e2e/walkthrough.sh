#!/usr/bin/env bash
#
# End-to-end walkthrough for mpfs-cli against a local MPFS devnet.
#
# Prerequisites (run inside `nix develop`):
#   - MPFS server reachable at $MPFS_SERVER (default http://localhost:3000).
#     Start one with: nix run .#mpfs-devnet-server -- --port 3000
#   - $MPFS_SIGNER_WALLET or $OWNER_KEY: path to a Bech32 ed25519 .skey
#     (ed25519_sk1...) whose enterprise address is funded on the devnet.
#   - $MPFS_BLUEPRINT set (the dev shell sets it), or pass --cage-config.
#
# Usage:
#   MPFS_SERVER=http://localhost:3000 MPFS_SIGNER_WALLET=/path/owner.skey \
#     cardano-mpfs-cli/e2e/walkthrough.sh
#
set -euo pipefail

SERVER="${MPFS_SERVER:-http://localhost:3000}"
SIGNER="${MPFS_SIGNER_WALLET:-${OWNER_KEY:-}}"
PROCESS_TIME_MS="${PROCESS_TIME_MS:-30000}"
RETRACT_TIME_MS="${RETRACT_TIME_MS:-5000}"

if command -v mpfs-cli >/dev/null 2>&1; then
    CLI=(mpfs-cli)
else
    CLI=(cabal run -v0 mpfs-cli --)
fi

if ! command -v jq >/dev/null 2>&1; then
    echo "walkthrough: jq is required" >&2
    exit 1
fi

if [ -z "$SIGNER" ]; then
    echo "walkthrough: set MPFS_SIGNER_WALLET or OWNER_KEY to a funded Bech32 ed25519 .skey path" >&2
    exit 1
fi

export MPFS_SERVER="$SERVER"
export MPFS_SIGNER_WALLET="$SIGNER"

step() {
    local label="$1"
    shift
    echo "-- $label" >&2
    local out
    if ! out=$("$@" 2>"$LOG_DIR/${label// /-}.stderr"); then
        echo "walkthrough FAILED: '$label' exited non-zero" >&2
        cat "$LOG_DIR/${label// /-}.stderr" >&2
        exit 1
    fi
    if ! jq -e . >/dev/null <<<"$out"; then
        echo "walkthrough FAILED: '$label' stdout is not JSON" >&2
        printf '%s\n' "$out" >&2
        exit 1
    fi
    printf '%s\n' "$out"
}

expect_value() {
    local label="$1" json="$2" expected="$3"
    local actual
    actual=$(jq -r '.result.value // empty' <<<"$json")
    if [ "$actual" != "$expected" ]; then
        echo "walkthrough FAILED: $label expected value $expected, got $actual" >&2
        exit 1
    fi
}

expect_absent() {
    local label="$1" json="$2"
    if [ "$(jq -r '.result.present' <<<"$json")" != "false" ]; then
        echo "walkthrough FAILED: $label expected an absent fact proof" >&2
        printf '%s\n' "$json" >&2
        exit 1
    fi
}

LOG_DIR="${TMPDIR:-/tmp}/mpfs-cli-walkthrough.$RANDOM.$RANDOM"
mkdir -p "$LOG_DIR"
trap 'rm -rf "$LOG_DIR"' EXIT

KEY="$(printf 'mpfs-cli-e2e-key' | xxd -p | tr -d '\n')"
VALUE1="$(printf 'v1' | xxd -p | tr -d '\n')"
VALUE2="$(printf 'v2' | xxd -p | tr -d '\n')"
REJECT_KEY="$(printf 'mpfs-cli-e2e-reject' | xxd -p | tr -d '\n')"
REJECT_VALUE="$(printf 'reject-me' | xxd -p | tr -d '\n')"

echo "walkthrough: server=$SERVER signer_path=$SIGNER" >&2
echo "walkthrough: process_time_ms=$PROCESS_TIME_MS retract_time_ms=$RETRACT_TIME_MS" >&2

step "register-token" \
    "${CLI[@]}" --json register-token \
        --process-time-ms "$PROCESS_TIME_MS" \
        --retract-time-ms "$RETRACT_TIME_MS" >/dev/null

tokens_json=$(step "token list" "${CLI[@]}" --json token list)
TOKEN=$(jq -r '.result.tokens[-1].id // empty' <<<"$tokens_json")
if [ -z "$TOKEN" ] || [ "$TOKEN" = "null" ]; then
    echo "walkthrough FAILED: no token id from token list" >&2
    exit 1
fi
echo "walkthrough: token=$TOKEN" >&2

step "token get" "${CLI[@]}" --json token get "$TOKEN" >/dev/null
step "fact list empty" "${CLI[@]}" --json fact list "$TOKEN" >/dev/null

step "fact insert" \
    "${CLI[@]}" --json fact insert \
        --token "$TOKEN" --key "$KEY" --value "$VALUE1" >/dev/null
step "requests list after insert" \
    "${CLI[@]}" --json requests list "$TOKEN" >/dev/null
step "token process insert" \
    "${CLI[@]}" --json token process "$TOKEN" >/dev/null
inserted=$(step "fact get inserted" "${CLI[@]}" --json fact get "$TOKEN" "$KEY")
expect_value "inserted fact" "$inserted" "$VALUE1"

step "fact update" \
    "${CLI[@]}" --json fact update \
        --token "$TOKEN" --key "$KEY" \
        --old-value "$VALUE1" --new-value "$VALUE2" >/dev/null
step "token process update" \
    "${CLI[@]}" --json token process "$TOKEN" >/dev/null
updated=$(step "fact get updated" "${CLI[@]}" --json fact get "$TOKEN" "$KEY")
expect_value "updated fact" "$updated" "$VALUE2"

step "fact delete" \
    "${CLI[@]}" --json fact delete \
        --token "$TOKEN" --key "$KEY" --value "$VALUE2" >/dev/null
step "token process delete" \
    "${CLI[@]}" --json token process "$TOKEN" >/dev/null
deleted=$(step "fact get deleted" "${CLI[@]}" --json fact get "$TOKEN" "$KEY")
expect_absent "deleted fact" "$deleted"

step "fact insert reject candidate" \
    "${CLI[@]}" --json fact insert \
        --token "$TOKEN" --key "$REJECT_KEY" --value "$REJECT_VALUE" >/dev/null
step "requests list reject candidate" \
    "${CLI[@]}" --json requests list "$TOKEN" >/dev/null

sleep_seconds=$(((PROCESS_TIME_MS + RETRACT_TIME_MS + 3000 + 999) / 1000))
echo "walkthrough: waiting ${sleep_seconds}s for reject deadline" >&2
sleep "$sleep_seconds"

step "fact reject" "${CLI[@]}" --json fact reject --token "$TOKEN" >/dev/null
requests_after_reject=$(step "requests list after reject" "${CLI[@]}" --json requests list "$TOKEN")
if [ "$(jq '.result.requests | length' <<<"$requests_after_reject")" -ne 0 ]; then
    echo "walkthrough FAILED: pending requests remain after reject" >&2
    printf '%s\n' "$requests_after_reject" >&2
    exit 1
fi

step "token end" "${CLI[@]}" --json token end --token "$TOKEN" >/dev/null

echo "walkthrough: OK boot -> insert/process/get -> update/process/get -> delete/process/absent -> reject -> end" >&2
