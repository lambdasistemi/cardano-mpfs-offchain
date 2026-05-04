# Specification Quality Checklist: Fact-provider pivot

**Purpose**: Validate specification completeness and quality before proceeding to planning
**Created**: 2026-05-04
**Feature**: [spec.md](../spec.md)

## Content Quality

- [x] No implementation details (languages, frameworks, APIs) — the spec talks about endpoints, facts bundles, snapshots, proofs, wallet policy, cross-target compilation. Module-level names are referenced because they are public API surface that the spec is committing to remove or relocate; that is FR territory, not implementation noise.
- [x] Focused on user value and business needs — the wallet-side trust model and operational viability under the pivot are the headline outcomes.
- [x] Written for non-technical stakeholders — concepts are stated in plain prose; protocol-level terms (UTxO, CSMT, MPF, snapshot) are unavoidable for this domain.
- [x] All mandatory sections completed — User Scenarios & Testing (4 stories with Given/When/Then), Requirements (11 FRs), Success Criteria (5 SCs), Assumptions all present.

## Requirement Completeness

- [x] No [NEEDS CLARIFICATION] markers remain — the architectural decisions were made in the design conversation that preceded this spec.
- [x] Requirements are testable and unambiguous — every FR specifies a check-able condition; FR-006/FR-007/FR-010 are greppable.
- [x] Success criteria are measurable — full-flow test (SC-001), source greps (SC-002, SC-003), regression test (SC-004), release-window observation (SC-005).
- [x] Success criteria are technology-agnostic — they describe outcomes, not internals.
- [x] All acceptance scenarios are defined — each user story carries Given/When/Then scenarios.
- [x] Edge cases are identified — unfunded address, unwarmed indexer, unknown token, unknown request UTxO, block-during-build, indexer corruption, snapshot-trusted-root mismatch.
- [x] Scope is clearly bounded — Assumptions section explicitly defers WASM artifact, multi-band snapshots, browser-wallet integration, third-party CLIs, on-chain changes.
- [x] Dependencies and assumptions identified — IndexerTx primitives from #253, TxBuild DSL in cardano-node-clients, MOOG as the validating consumer, no native pp signing in Cardano.

## Feature Readiness

- [x] All functional requirements have clear acceptance criteria — every FR maps to at least one acceptance scenario or edge case.
- [x] User scenarios cover primary flows — Story 1 (end-to-end client-build pivot), Story 2 (legacy endpoints removed), Story 3 (verifier surface shrinks), Story 4 (pp gap mitigation).
- [x] Feature meets measurable outcomes defined in Success Criteria — SC-001 through SC-005 each tie back to a story.
- [x] No implementation details leak into specification — checked.

## Notes

- This spec is a substantive architectural pivot, not an incremental slice. It supersedes the producer-side direction taken in #249 (PR #253 merged) at the public-API level, while preserving the IndexerTx infrastructure that PR introduced.
- The pivot is a hard cutover — both `cardano-mpfs-offchain` and `lambdasistemi/moog` move together. Coordinating across two repositories is the operational risk; FR-011 names it.
- The "unverified pp" gap is honestly documented; mitigation is wallet-policy caps. Future signed-pp protocols can land without a wire-contract change.
