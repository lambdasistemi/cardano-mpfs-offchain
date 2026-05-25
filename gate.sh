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

nix develop --quiet -c just ci
