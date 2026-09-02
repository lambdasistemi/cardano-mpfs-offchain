# Tasks

Artifact ceiling: 2,500 bytes / 80 lines.

## S1 — Repository publication contract

- [x] T-PUB-001 Make image identity distinguish a clean full revision from an unmistakable local placeholder.
- [x] T-PUB-002 Add OCI revision, version, and source labels.
- [x] T-PUB-003 Add daemonless GHCR publication with settled input/tag guards.
- [x] T-PUB-004 Add pulled-image startup verification and a real broken-input negative control.
- [x] T-PUB-005 Publish SHA/main and branch-dispatch aliases from existing CI while retaining all existing jobs and artifact upload.
- [x] T-PUB-006 Publish release SHA and `v<version>` aliases and upload the immutable digest receipt.
- [x] T-PUB-007 Prove dirty source, placeholder identity, and broken startup input are rejected.

## S2 — Live branch evidence

- [ ] T-PUB-008 Dispatch the feature branch and capture run URL, source SHA, digest, labels, and startup evidence.
- [ ] T-PUB-009 Independently pull/inspect/start the digest outside CI and freeze raw output.
- [ ] T-PUB-010 Verify all PR-head checks are green and the draft PR describes the factual change without issue linkage.
