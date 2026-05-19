#!/usr/bin/env bash
# gate.sh — mechanical gate for issue #275 (mpfs-v2 startup replay/recovery).
# Ephemeral; dropped in the final commit before the PR is marked ready.
set -euo pipefail

git diff --check

# Conventional Commits + `Tasks:` trailer gate over commits on this branch.
base_ref="origin/main"
range="${base_ref}..HEAD"
subj_re='^(feat|fix|docs|test|refactor|perf|build|ci|chore|style|revert)(\([^)]+\))?: .+'
tasks_re='^Tasks: T[0-9]+([[:space:]]*,[[:space:]]*T[0-9]+)*$'

fail=0
while read -r sha; do
  [ -z "$sha" ] && continue
  subj=$(git log -1 --format=%s "$sha")
  body=$(git log -1 --format=%B "$sha")
  type=$(printf '%s' "$subj" | sed -E 's/^([a-z]+)(\([^)]+\))?:.*/\1/')

  if ! printf '%s' "$subj" | grep -Eq "$subj_re"; then
    printf 'gate failure: commit %s subject is not Conventional Commits: %s\n' "$sha" "$subj" >&2
    fail=1
  fi

  # Behavior-changing types must carry a Tasks: trailer.
  case "$type" in
    feat|fix|refactor|perf|test)
      if ! printf '%s' "$body" | grep -Eq "$tasks_re"; then
        printf 'gate failure: commit %s missing Tasks: trailer\n' "$sha" >&2
        fail=1
      fi
      ;;
  esac
done < <(git log --format=%H "$range")

if [ "$fail" -ne 0 ]; then
  exit 1
fi

# Slice-specific checks will be extended as commits land.
# The plan will name the readiness contract (e.g. /status fail-closed during
# replay) and the devnet full-lifecycle smoke that proves it.

echo "gate: OK"
