# Implementation Plan: Value persistence for the fact lookup endpoint

**Branch**: `248-value-persistence` | **Date**: 2026-05-01 | **Spec**: [spec.md](./spec.md)
**Input**: Feature specification from `/specs/248-value-persistence/spec.md`

## Summary

`GET /tokens/:id/facts/:key` must return the bytes the requester originally inserted in its `value` field, instead of `mkMPFHash key`. The fix persists raw value bytes in a new RocksDB column family that lives alongside the existing trie state, written and rolled back inside the chain-follower's existing atomic-block + inverse-op machinery so the new state stays in lockstep with the merkle tree. The HTTP read path keeps its single `Trie.lookup` interface, with the new column threaded through the existing trie manager.

## Technical Context

**Language/Version**: Haskell GHC 9.10.1 (existing toolchain)
**Primary Dependencies**: `cardano-ledger-*` (domain types), `chain-follower` (block stream + rollback), `rocksdb-kv-transactions` (atomic batch writes), `mts:mpf` (per-token MPF trie), `mts:csmt` (UTxO CSMT — orthogonal to this feature but shares the same RocksDB), `Cardano.MPFS.Indexer.Event` (`InvTrieInsert`/`InvTrieDelete` inverse-op machinery)
**Storage**: RocksDB column families. Existing trie-related families: `TrieNodes` (`KV HexKey (HexIndirect MPFHash)`), `TrieKV` (`KV HexKey MPFHash`, the value-hash mirror the merkle layer queries), `TrieMeta` (token visibility registry). This feature adds a fourth family for raw values.
**Testing**: hspec unit suite (`just unit`), devnet-subprocess e2e (`just e2e`) per Principle VI. The e2e fixture already inserts known `(key, value)` pairs through the request → process flow; the byte-equality assertion plugs into that.
**Target Platform**: Linux server (offchain). Verifier code paths (cardano-mpfs-client) untouched by this work, so GHC-WASM / GHC-JS cross-target obligations are not exercised here.
**Project Type**: web-service backend, single project (the existing `cardano-mpfs-offchain` library + `mpfs-serve` executable + e2e devnet harness).
**Performance Goals**: correctness, not throughput. No SLA on lookup latency; large-value performance does not need to match small-value performance per spec Edge Cases. Block-processing throughput must not regress meaningfully — the new write is one extra `KV.insert`/`KV.delete` inside the existing batch.
**Constraints**: Principle III (atomic block processing) is the load-bearing one. Every mutation introduced by this feature must land in the same RocksDB write batch as the corresponding trie mutation, or the rollback story collapses.
**Scale/Scope**: existing devnet/preprod scale. Storage growth is bounded by the on-chain protocol's accepted request value sizes; no new growth model.

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

| Principle | Applies? | Compliance |
|---|---|---|
| I. Ledger-Native Types | Neutral | This feature lives in the storage layer; no domain-type changes. |
| II. Records of Functions | Yes | `Trie m` is already a record-of-functions and stays that way. The `lookup` field's semantics change (returns raw value, not the bug-compatible sentinel) but the boundary shape is unchanged. No new typeclass introduced. |
| III. Atomic Block Processing | Yes — load-bearing | FR-003 / FR-004 explicitly reuse the existing atomic-block + inverse-op machinery. Every raw-value mutation rides inside the same `Transaction` the chain-follower already opens for the block. The existing `InvTrieInsert` / `InvTrieDelete` carriers stay byte-shape-identical: they already pass raw value bytes, and rollback already replays through `Trie.insert` / `Trie.delete` — once those mirror to the new column, restoration is automatic. See research.md §1. |
| IV. External Signing | Neutral | No tx-building or signing changes. |
| V. Aiken Compatibility | Neutral | Trie hashing, proof encoding, and datum construction are unchanged. The merkle tree continues to store the value-hash exactly as it does today; the new column is purely an offchain mirror. |
| VI. Test Locally First | Yes | The byte-equality e2e test (FR-005 / SC-001) runs against the existing devnet-subprocess harness. No CI-only checks. Unit-level coverage of the inverse-op machinery exists today; we extend it rather than introduce a new test runner. |
| VII. Nix Reproducibility | Neutral | Standard `nix develop` + `just ci` flow. |
| VIII. Pure Offline Verification | Neutral, with downstream effect | This feature does not touch the verifier. It does fix a bug that made the verifier reject honest responses (the lookup returning the wrong bytes blocked `verifyFactPresentResponse` in slice 3). The verifier stays pure; the input it receives stops lying. |
| IX. One Verifier, Many Targets | Neutral | No verifier code edited; cross-target matrix obligations unchanged. |
| X. Lean as Source of Truth | Neutral | The Lean model in `lean/Phase4` deliberately covers verifier state machines (the trust-replay envelope), not server-side storage. The Haskell predicates relevant to this fix (`replayFactPresent_*` in slice 3) stay valid because the wire shape they describe is unchanged; this work makes the server's `value` field finally match what those predicates always assumed. No Lean amendments required. |

**Result**: PASS. No principle violated, no Complexity Tracking entries needed.

**Post-design re-check (after Phase 1)**: PASS, unchanged. The Phase 0
research reduced the planned blast radius by demonstrating that
`InvTrieInsert` / `InvTrieDelete` carriers do not need extension
(research.md §1) — the existing wire format and rollback decoder stay
byte-shape-identical, which strengthens compliance with Principle III.
Phase 1 contracts (`contracts/trie-lookup.md`) document the only
boundary change: the meaning of `Trie.lookup`'s return value, with the
record-of-functions shape itself unchanged (Principle II preserved).
No new dependencies, no new typeclasses, no verifier-side changes.

## Project Structure

### Documentation (this feature)

```text
specs/248-value-persistence/
├── plan.md              # This file (/speckit.plan output)
├── spec.md              # Feature spec (/speckit.specify + /speckit.clarify output)
├── research.md          # Phase 0 output — inverse-op extension, read-path bridge wiring
├── data-model.md        # Phase 1 output — TrieRawValues column + key derivation
├── contracts/           # Phase 1 output — Trie.lookup semantic change, HTTP wire shape unchanged
├── quickstart.md        # Phase 1 output — developer/operator quickstart
└── tasks.md             # Phase 2 output (/speckit.tasks — NOT created here)

checklists/
└── requirements.md      # Spec quality checklist (/speckit.specify output)
```

### Source Code (repository root)

```text
cardano-mpfs-offchain/
├── lib/
│   └── Cardano/
│       └── MPFS/
│           ├── Indexer/
│           │   ├── Columns.hs       # ADD: TrieRawValues constructor on AllColumns
│           │   ├── Codecs.hs        # ADD: identity prism for the new column (raw passthrough)
│           │   └── Event.hs         # UNCHANGED: InvTrieInsert/Delete carriers already pass raw bytes; semantics are corrected upstream by Trie.lookup returning the real value
│           ├── Trie.hs              # MODIFY: lookup semantics — returns the raw value, not Just (hashBS k)
│           ├── Trie/
│           │   ├── Persistent.hs    # MODIFY: unifiedInsert/Delete/Lookup + persistent/speculative variants mirror to the new column; mkPersistentTrieManager / withPersistentTrieManager grow a 4th CF parameter
│           │   └── Pure.hs          # MODIFY: in-memory pure implementation returns the inserted raw bytes from lookup (mirror via separate IORef map)
│           └── Application.hs       # MODIFY: cageColumnFamilies grows by one entry; CF index pattern updated
├── e2e-test/
│   └── Cardano/MPFS/E2E/
│       └── ProofsSpec.hs            # MODIFY: replace assertFactEnvelope's "non-empty hex" check with byte-equality on the inserted value
└── test/
    └── Cardano/MPFS/Indexer/
        └── RollbackSpec.hs          # ADD/MODIFY: rollback fixture covers Insert/Update/Delete × single-block / multi-block reorgs leaving raw values byte-identical to the rolled-back tip's view
```

**Structure Decision**: single Haskell project, modifying the existing `cardano-mpfs-offchain` library plus its e2e test suite. No new packages. The fact endpoint's wire shape is unchanged so `cardano-mpfs-api` and `cardano-mpfs-client` are untouched.

## Complexity Tracking

> Constitution Check passed with no violations; this section intentionally empty.
