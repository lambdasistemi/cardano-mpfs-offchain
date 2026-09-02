# Pipeline-published `mpfs-serve` image

Artifact ceiling: 6,500 bytes / 120 lines.

## Outcome

The repository pipeline publishes the `mpfs-serve` OCI image at
`ghcr.io/lambdasistemi/cardano-mpfs-offchain/mpfs-serve`. Every published
artifact is tied to the exact clean source commit, is addressable by immutable
digest, and can be pulled and started without a repository checkout.

## Requirements

- **R-PUB-001 — Pipeline origin.** Publication builds from the checked-out
  workflow source. Operator worktrees and pre-built local archives are not
  publication inputs.
- **R-PUB-002 — Immutable identity.** Each successful publication reports an
  immutable `sha256:` digest and the exact 40-character source SHA.
- **R-PUB-003 — OCI provenance.** The image config contains
  `org.opencontainers.image.revision` equal to the exact 40-character source
  SHA, `org.opencontainers.image.version`, and
  `org.opencontainers.image.source` equal to the repository URL.
- **R-PUB-004 — Clean publish source.** A dirty, missing, shortened,
  placeholder, or malformed source revision prevents publication. A local
  development build may use an unmistakably non-qualifying identity such as
  `unknown` or `dirty-<revision>`, but it cannot pass the publish gate.
- **R-PUB-005 — Tag policy.** A main push publishes
  `sha-<40-character-SHA>`; a branch dispatch publishes
  `branch-<slug>-sha-<40-character-SHA>`; a release additionally publishes
  `v<version>`. No path publishes `latest`.
- **R-PUB-006 — Immutable release record.** A release publication records the
  release tag, exact commit, image repository, immutable digest, and published
  aliases in a release artifact without deleting or recreating a tag or
  release.
- **R-PUB-007 — Real startup proof.** Publication pulls the registry artifact
  by digest and exercises its configured entrypoint. Because `mpfs-serve` has
  no help/version mode, success means observing its expected missing-required-
  argument failure rather than treating any non-zero exit as success.
- **R-PUB-008 — Falsifiable checks.** The clean-tree guard and startup checker
  each have an executed negative control. A wrong failure mode or broken input
  makes verification non-zero; no verification command is masked.
- **R-PUB-009 — Existing CI retained.** Existing build, test, E2E,
  version-sync, Docker build, and Docker artifact upload behavior remains
  enabled.
- **R-PUB-010 — Least privilege.** Same-repository GHCR publication uses the
  workflow `GITHUB_TOKEN` with job-scoped `packages: write`; it does not mint
  or expose the GitHub App private key.

## Invariants

| ID | Severity | Failure meaning | Success meaning |
|---|---|---|---|
| I1-SOURCE-IDENTITY | ADVISORY | A tag or revision label can name a source other than the exact built commit. | The tag input and revision label both derive from and equal the clean full SHA. |
| I2-CLEAN-TREE | ADVISORY | Dirty, ambiguous, or placeholder source can pass the publish gate or resemble a clean SHA. | Dirty source and an explicit placeholder are both rejected by publication, demonstrated by negative controls. |
| I3-CHECKS-FAIL | ADVISORY | A broken startup input or wrong failure mode can leave verification green. | The same checker is observed rejecting a real broken input and accepts only the expected `mpfs-serve` startup failure. |
| I4-IMAGE-STARTS | ADVISORY | Registry presence is mistaken for executable startup. | The pulled digest runs its configured entrypoint and reaches `mpfs-serve` argument validation. |

## Rejections and non-goals

Reject publication before any registry copy when the worktree is dirty, the
identity is a placeholder, the SHA is not exactly 40 lowercase hexadecimal
characters, the tag violates the settled policy, or the archive labels
disagree with the source SHA. Reject a
post-copy run when digest inspection or startup proof fails.

This ticket does not change Haskell, add `--help`/`--version`, add HTTP routes,
deploy, modify release-please configuration, make the GHCR package public,
publish `latest`, or remove the existing workflow artifact upload.
