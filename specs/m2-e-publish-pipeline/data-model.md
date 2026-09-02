# Data model

Artifact ceiling: 3,500 bytes / 80 lines.

## D-SOURCE — source identity

| Field | Type | Constraint |
|---|---|---|
| `revision` | clean lowercase hexadecimal string or local placeholder | A publishable value is exactly 40 characters and equal to the checked-out clean Git commit. A local value is unmistakably non-qualifying, such as `unknown` or `dirty-<revision>`. |
| `source` | HTTPS repository URL | Exactly `https://github.com/lambdasistemi/cardano-mpfs-offchain`. |
| `version` | OCI label string | Stable non-empty image version derived for the same source. |

No publishable D-SOURCE exists for a dirty tree, a placeholder, or an absent
full revision. Placeholder identity cannot be normalized into a SHA by the
publication boundary.

## D-ALIAS — registry tag

| Field | Type | Constraint |
|---|---|---|
| `kind` | `main-sha` \| `branch-sha` \| `release` | Determined by the authorized workflow event. |
| `tag` | OCI tag string | Matches the settled shape for its kind; never `latest`. |
| `revision` | D-SOURCE reference | The same full revision the archive labels carry. |

Branch slugs are deterministic lowercase OCI-safe representations of the full
branch ref name and are length-bounded so the complete tag is valid.

## D-PUBLICATION — immutable publication result

| Field | Type | Constraint |
|---|---|---|
| `repository` | image repository | The mandated GHCR repository. |
| `digest` | OCI digest | `sha256:` followed by exactly 64 lowercase hexadecimal characters. |
| `aliases` | non-empty set of D-ALIAS | Every alias resolves to `digest`. |
| `revision` | D-SOURCE reference | Equals the revision label returned by registry inspection. |
| `run_url` | HTTPS URL | Identifies the pipeline run that built and copied the image. |

## D-STARTUP — startup observation

| Field | Type | Constraint |
|---|---|---|
| `image` | repository plus immutable digest | Never only a mutable tag. |
| `exit` | integer | Non-zero because required server arguments are intentionally absent. |
| `output` | captured text | Contains the expected `mpfs-serve` missing-argument diagnostic. |
| `runtime` | runtime identity | Names the executable that pulled/started the image. |

A different non-zero failure is not a successful D-STARTUP observation.
