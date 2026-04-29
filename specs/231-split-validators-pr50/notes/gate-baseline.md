# GATE baseline (T002)

## Upstream `origin/main` baseline

GitHub CI status for the tip of `main` at the time of this PR session:

- Commit: `1e5402fab7514328e8f2502e23549689a8bcc941`
  ([1e5402f — "feat(client): derive typed HTTP wrappers from shared API"](https://github.com/lambdasistemi/cardano-mpfs-offchain/commit/1e5402fab7514328e8f2502e23549689a8bcc941))
- Workflow `CI`: **success** (run on 2026-04-26)
- Workflow `Release`: **success**
- Workflow `Build and deploy documentation`: **success**

Per `pr` skill Setup step 4: "If it is red on the base, **do not** try
to fix it in this PR." `origin/main` is green; baseline established.
A fresh local re-run of `just ci && just e2e` against `origin/main`
is not required when the CI signal is current and green.

## Branch gate at review handoff

The initial local post-pin smoke transcript was not preserved in the
handoff. Do not infer that a clean `origin/main` checkout was rerun
locally in this session.

The branch has been validated by the project gate that matters for
review:

- Prior published head
  `5312a68f651212ea5ba49988c2dde586c4e20cf2`: GitHub CI **success**
  for Build Gate, build, deploy, and e2e.
  - CI run: https://github.com/lambdasistemi/cardano-mpfs-offchain/actions/runs/25102465510
  - Deploy run: https://github.com/lambdasistemi/cardano-mpfs-offchain/actions/runs/25102465548
- Working tree including the E2E output cleanup: local `just e2e`
  **success**, 22 examples / 0 failures.
- Working tree including the E2E output cleanup: local non-E2E gate:

  ```bash
  just ci
  ```

  Result: **success** — build, unit 369/0, unit-offchain 369/0,
  format-check, hlint, and cabal-fmt all passed.
- The CI command standard is now flake-output based for non-Docker
  verification: `nix run .#unit-tests`, `nix run .#format-check`,
  `nix run .#hlint`, and `nix run .#e2e-tests`. Docker remains
  `nix build .#docker-image`.

This file is therefore a baseline note plus final gate transcript, not
a claim that pre-existing failures were fixed in this PR.
