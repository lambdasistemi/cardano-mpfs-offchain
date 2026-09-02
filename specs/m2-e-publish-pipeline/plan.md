# Implementation plan

Artifact ceiling: 6,000 bytes / 120 lines.

## Strategy

Use the existing `CI` workflow for main and branch publication because its
`workflow_dispatch` definition already exists on the default branch; GitHub
can therefore dispatch the feature branch before merge. Add the release alias
to the existing release workflow, reusing repository-owned publication and
verification commands. Keep build identity inside the flake/image derivation
and registry mechanics outside the Nix sandbox.

The publishable image archive has one stable internal identity based on the
exact source SHA. A dirty local build instead receives an unmistakable
non-qualifying placeholder. Registry aliases are applied during publication so
a release alias and the immutable SHA alias can point at the same content. The
release record is a small uploaded receipt rather than a recreated or replaced
release.

## Live boundaries

- Nix evaluation makes dirty/local identity visibly non-qualifying; the
  publication boundary fails closed without a clean full revision.
- `skopeo` copies the Nix Docker archive to GHCR without assuming a Docker
  daemon.
- The container runtime is used only for post-publication startup proof. The
  workflow records which runtime is available and fails if none can exercise
  the pulled digest.
- GHCR authorization uses the same-repository `GITHUB_TOKEN` with
  `packages: write`. The GitHub App is not part of this boundary.

## Ordered slices

### S1 — Repository publication contract

Produce one bisect-safe repository commit containing the image identity and
labels, reusable publication/startup verification commands, focused negative
controls, main/dispatch publication wiring, release alias wiring, and the
release digest receipt. The commit is locally buildable and its static/focused
gate is falsified on the accepted base before implementation.

### S2 — Live branch evidence

Push the accepted S1 commit, dispatch the existing `CI` workflow on the feature
branch, and freeze the run URL, source SHA, immutable digest, external registry
inspection, startup result, negative controls, and PR CI state. This slice
changes no repository source and is admissible only from the exact accepted S1
SHA.

## Verification

- Local focused gate proves workflow/tag/label/guard/startup-check structure,
  YAML validity, and negative controls for dirty source, placeholder identity,
  and broken startup input.
- `nix build .#docker-image` succeeds from the clean candidate.
- The normal repository CI command remains green.
- A real branch-dispatch run publishes and verifies the digest.
- An independent external `skopeo inspect` confirms the digest and full
  revision label; an external runtime repeats the startup check.

## Constraints

No Haskell or route file is writable. Release-please files, deployment scripts,
production, `latest`, package visibility, published tags/releases, existing CI
jobs, and the existing artifact upload are outside the implementation fence.
