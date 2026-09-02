# Modules model

Artifact ceiling: 3,500 bytes / 80 lines.

| ID | Component | Responsibility | Dependency direction |
|---|---|---|---|
| M-IMAGE | Flake image output and `nix/docker-image.nix` | Distinguish clean full source identity from unmistakable local placeholders and embed standard OCI provenance. | Depends on flake-provided source identity and existing `mpfs-serve`/blueprint derivations only. |
| M-PUBLISH | Repository-owned image publication command | Require clean full source identity, validate source/tag inputs, build the repository image archive, copy it daemonlessly to GHCR aliases, and emit validated immutable digest/receipt fields. | Depends on M-IMAGE and registry tools; knows no workflow event syntax. |
| M-STARTUP | Repository-owned pulled-image startup checker | Exercise the configured entrypoint by digest and distinguish the expected missing-required-argument failure from unrelated failure. | Depends on a container runtime and an immutable image reference; does not build or publish. |
| M-CI | Existing `.github/workflows/ci.yml` | Preserve all current checks and artifact upload; publish only for main push or manual dispatch with settled tags and job-scoped package permission. | Converts GitHub event/ref data into validated M-PUBLISH inputs, then invokes M-STARTUP. |
| M-RELEASE | Existing `.github/workflows/release.yml` | When release-please creates a release, publish SHA and `v<version>` aliases and upload a non-clobbering digest receipt to that release. | Depends on release-please outputs and the same M-PUBLISH/M-STARTUP contract. |

M-CI and M-RELEASE may select aliases but cannot invent source identity; the
full SHA comes from the checked-out source and must agree with M-IMAGE.
