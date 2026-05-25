#!/usr/bin/env bash
set -euo pipefail

# Bootstrap gate for resolve-ticket PR (lambdasistemi/cardano-mpfs-offchain#247).
#
# This gate runs the standard format/lint/build/test spine. The
# ticket-specific cryptographic round-trip and any schema-migration
# assertions land in extension commits once Q-001 (option A/B/C) is
# answered. Dropped in the `chore: drop gate.sh (ready for review)`
# commit before mark-ready.

git diff --check

check_legacy_trie_lookup_patterns_absent() {
    local persistent_file="cardano-mpfs-offchain/lib/Cardano/MPFS/Trie/Persistent.hs"
    local pure_file="cardano-mpfs-offchain/lib/Cardano/MPFS/Trie/Pure.hs"
    local persistent_hash_count
    local pure_hash_count
    local legacy_render_count

    count_fixed() {
        rg -n -F "$1" "${@:2}" || true
    }

    persistent_hash_count=$(count_fixed 'Just (hashBS k)' "$persistent_file" | wc -l | tr -d ' ')
    pure_hash_count=$(count_fixed 'Just (hashBS k)' "$pure_file" | wc -l | tr -d ' ')
    legacy_render_count=$(
        count_fixed 'Just (renderMPFHash (mkMPFHash k))' "$persistent_file" "$pure_file" | wc -l | tr -d ' '
    )

    if [ "$persistent_hash_count" -ne 1 ]; then
        echo "FAIL: expected exactly one Persistent.hs 'Just (hashBS k)' occurrence (the speculative path); found $persistent_hash_count"
        count_fixed 'Just (hashBS k)' "$persistent_file"
        return 1
    fi

    if [ "$pure_hash_count" -ne 0 ]; then
        echo "FAIL: legacy Pure.hs 'Just (hashBS k)' fallback reappeared"
        count_fixed 'Just (hashBS k)' "$pure_file"
        return 1
    fi

    if [ "$legacy_render_count" -ne 0 ]; then
        echo "FAIL: legacy renderMPFHash/mkMPFHash-as-value fallback reappeared"
        count_fixed 'Just (renderMPFHash (mkMPFHash k))' "$persistent_file" "$pure_file"
        return 1
    fi
}

check_legacy_trie_lookup_patterns_absent

nix develop --quiet -c just ci
