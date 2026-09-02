# Functions model

Artifact ceiling: 3,500 bytes / 80 lines.

| ID | Interface | Arguments | Result / effects | Constraints |
|---|---|---|---|---|
| F-IMAGE | Docker image derivation | `pkgs`, `project`, `revision: String`, `version: String`, `mpfs-blueprint` | Docker archive derivation | Embeds D-SOURCE labels and a stable internal tag; dirty/local identity is visibly non-qualifying. |
| F-PUBLISH | `scripts/publish-mpfs-serve-image` | `source_sha: String`, one or more `tag: String` | Copies one pipeline-built archive to each GHCR alias and writes D-PUBLICATION fields | Fails before copy on dirty/placeholder/invalid source, invalid tag, label mismatch, absent auth, malformed digest, or differing alias digests. No daemon requirement. |
| F-STARTUP | `scripts/check-mpfs-serve-image-startup` | `image_digest_ref: String` | Writes D-STARTUP observation; exit 0 only for expected startup diagnostic | Pulls/runs the configured entrypoint; rejects an unrelated non-zero failure and a zero exit. |
| F-CI-TAGS | CI event-to-alias mapping | `event_name`, `ref_name`, `source_sha` | One D-ALIAS | Main push selects `sha-<SHA>`; dispatch selects `branch-<slug>-sha-<SHA>`. |
| F-RELEASE-TAGS | release output-to-alias mapping | `tag_name`, `source_sha` | SHA alias plus release D-ALIAS | Release alias is exactly the release-please `v<version>` output and shares the digest. |
| F-RECEIPT | release receipt generation | D-PUBLICATION plus release tag | Immutable text artifact uploaded to the existing release | Contains commit, digest, repository, run URL, and aliases; upload does not delete/recreate or clobber. |
