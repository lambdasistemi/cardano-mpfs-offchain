#!/usr/bin/env bash
#
# End-to-end walkthrough for mpfs-cli against a local MPFS devnet.
#
# This is the live-boundary smoke for the write path: unit tests cover
# parsing, key decoding, and signing, but only a real server + node
# proves the workflow → sign → POST /submit → await chain. The script
# drives the actual binary and fails loudly if any step does not exit 0
# or does not emit JSON on stdout.
#
# Prerequisites (run inside `nix develop`):
#   - MPFS server reachable at $MPFS_SERVER (default http://localhost:3000).
#     Start one with:  nix run .#mpfs-devnet-server -- --port 3000
#   - $OWNER_KEY: path to a Bech32 ed25519 .skey (ed25519_sk1…) whose
#     enterprise address is funded on the devnet. The devnet's funded
#     genesis key is the one to use; export its Bech32 form here.
#   - $MPFS_BLUEPRINT set (the dev shell sets it), or pass --cage-config.
#
# Usage:
#   MPFS_SERVER=http://localhost:3000 OWNER_KEY=/path/owner.skey \
#     cardano-mpfs-cli/e2e/walkthrough.sh
#
set -euo pipefail

SERVER="${MPFS_SERVER:-http://localhost:3000}"

# The CLI invocation as an array (so a multi-word fallback word-splits
# correctly): the installed binary if present, else `cabal run`.
if command -v mpfs-cli >/dev/null 2>&1; then
    CLI=(mpfs-cli)
else
    CLI=(cabal run -v0 mpfs-cli --)
fi

if [ -z "${OWNER_KEY:-}" ]; then
    echo "walkthrough: set OWNER_KEY to a funded Bech32 ed25519 .skey path" >&2
    echo "  (the devnet genesis key, in ed25519_sk1… form)" >&2
    exit 1
fi

# Run a CLI command, assert exit 0 and non-empty JSON on stdout.
step() {
    local label="$1"
    shift
    echo "── $label" >&2
    local out
    if ! out=$("$@" 2>/dev/null); then
        echo "walkthrough FAILED: '$label' exited non-zero" >&2
        exit 1
    fi
    if [ -z "$out" ]; then
        echo "walkthrough FAILED: '$label' produced no stdout" >&2
        exit 1
    fi
    # Minimal JSON sanity: starts with { or [.
    case "$(printf '%s' "$out" | head -c1)" in
        '{' | '[') : ;;
        *)
            echo "walkthrough FAILED: '$label' stdout is not JSON: $out" >&2
            exit 1
            ;;
    esac
    printf '%s\n' "$out"
}

# Deterministic fact key/value for the run.
KEY="$(printf 'mpfs-cli-e2e' | xxd -p)"
VALUE="$(printf 'v1' | xxd -p)"

echo "walkthrough: server=$SERVER owner=$OWNER_KEY" >&2

# The asserted flow is the requester write path the CLI owns: boot a
# cage, observe it, and submit a fact request. Each goes through the
# full workflow → sign → POST /submit → await chain, so a green run
# proves the live boundary. (Steps not asserted here and why:
#   - `fact get` after `insert` 404s by design: an oracle must apply the
#     pending request before the fact materializes — the oracle path is
#     a non-CLI concern (see README "Scope").
#   - `token end` 409s while the request is pending; clearing it needs
#     `fact retract`, which currently hits a server-side 500 in
#     /facts/retract — tracked separately, not a CLI defect.)

step "register-token" \
    "${CLI[@]}" register-token --server "$SERVER" --owner-key "$OWNER_KEY" >/dev/null

# token list is read-only; the new token id is the last listed.
step "token list" "${CLI[@]}" token list --server "$SERVER" >/dev/null
TOKEN=$("${CLI[@]}" token list --server "$SERVER" 2>/dev/null \
    | tr -d '[]" ' | tr ',' '\n' | tail -n1)
if [ -z "$TOKEN" ]; then
    echo "walkthrough FAILED: no token id from 'token list'" >&2
    exit 1
fi
echo "   token=$TOKEN" >&2

step "fact insert" "${CLI[@]}" fact insert --server "$SERVER" \
    --token "$TOKEN" --key "$KEY" --value "$VALUE" --owner-key "$OWNER_KEY" >/dev/null

echo "walkthrough: OK (write path proven: boot + list + request)" >&2
