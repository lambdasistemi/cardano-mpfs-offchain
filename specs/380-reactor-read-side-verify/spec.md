# Feature Specification: Reactor Read-Side Verify Ops

**Feature Branch**: `feat/380-reactor-read-side-verify`
**Issue**: [#380](https://github.com/lambdasistemi/cardano-mpfs-offchain/issues/380)
**Input**: Expose read-side verification operations through the
`cardano-mpfs-verify` reactor compiled as `wasm-mpfs-verify`.

## User Story

As a browser client using the WASM verifier, I can forward raw UMPFS
read responses to the verify reactor with a trusted root and receive the
same `verify_ok` / `verify_error` verdicts as native Haskell clients, so
the browser no longer reimplements proof decoding or drops proof fields.

## Functional Requirements

- **FR-001**: `Cardano.MPFS.Client.Verify.Reactor.runEnvelope` MUST
  dispatch `verify_tokens`, `verify_snapshot`, `verify_fact_inclusion`,
  and `verify_facts`.
- **FR-002**: Each new op MUST decode the raw response shape served by
  `https://umpfs.plutimus.com` under the existing envelope `facts`
  payload and the supplied `trusted_root`.
- **FR-003**: Ops that need local address-prefix derivation MUST parse
  `cage_config` with the same blueprint-derived shape as the existing
  `end` arm.
- **FR-004**: The new arms MUST wrap existing read-side verifier
  functions/primitives; they MUST NOT introduce new proof verification
  algorithms.
- **FR-005**: Honest real UMPFS responses MUST return `verify_ok`;
  tampered responses MUST return `verify_error: ...`.
- **FR-006**: The existing write-side reactor operations and verdict
  taxonomy MUST remain unchanged.
- **FR-007**: `nix build .#wasm-mpfs-verify --fallback` MUST compile the
  reactor with the new operations.

## Success Criteria

- Reactor tests cover each new read-side op using non-empty live UMPFS
  response fixtures captured from `umpfs.plutimus.com`.
- At least one tampering case per new operation returns `verify_error`.
- Focused client verifier tests, the PR gate, and the WASM build pass.

## Non-Goals

- Browser wiring in `cardano-mpfs-browser`.
- Changing MPF/CSMT verification semantics.
- Bumping `mts`, `csmt`, `cardano-ledger-wasm`, or any dependency pins.
- Modifying existing write-side reactor behavior except for shared
  helpers needed by the dispatch extension.
