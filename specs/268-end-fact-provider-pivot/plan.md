# Implementation Plan: End fact-provider pivot

**Branch**: `268-end-fact-provider-pivot` | **Date**: 2026-05-18 | **Spec**: [spec.md](./spec.md)

## Summary

Ship the end slice of the fact-provider pivot. The server adds `POST /facts/end`, returning only proof-bearing facts for the token state UTxO, owner funding UTxOs, an empty per-cage request-set completeness witness, and unverified protocol parameters. The client verifies those facts against a trusted UTxO root and locally builds the burn transaction with `endCageTx`. The legacy server-built `POST /tx/end` path is removed.

MOOG coordination follows the boot decision: this PR records boundary status and links the MPFS-v2 canary/replacement track; it does not require a production migration of the old MOOG caller.

## Technical Context

**Language/Version**: Haskell GHC 9.10.1.

**Packages**: `cardano-mpfs-api` owns wire DTOs and Servant API, `cardano-mpfs-client` owns pure verification and local construction, and `cardano-mpfs-offchain` owns HTTP handler and indexer reads.

**Proof dependencies**: Existing CSMT inclusion replay plus `CSMT.Verify.verifyCompletenessProof` for the empty request-set witness.

**Gate**: Baseline `./gate.sh` passed on 2026-05-18 before edits.

## Constitution Check

- Ledger-native types: PASS. `endCageTx` returns `Tx ConwayEra` and decodes ledger `TxOut` bytes only at builder boundaries.
- Records of functions: PASS. No new service typeclass.
- Atomic reads: PASS by adding end-specific `IndexerTx` primitives and composing them under one `runIndexerTx ctx`.
- Client-side construction: PASS. The server returns no end transaction CBOR.
- Aiken compatibility: PASS with required regression tests for burn/redeemer shape and submit-valid budgets.
- Local verification: PASS. Focused tests and `./gate.sh` remain required.
- Nix reproducibility: PASS. Commands run through `nix develop` or existing Just recipes.
- Pure offline verification: PASS. `verifyEndFacts` performs only proof checks and local prefix derivation.
- One verifier, many targets: PASS. No transaction grammar import is added to the facts verifier surface.

## Work Breakdown

1. Spec and task artifacts with corrected MOOG boundary language.
2. Break the API package wire DTOs out of the monolithic `API.Types`
   surface before adding the end facts shape.
3. Wire type, verifier, and request-set completeness primitive.
4. Client-side `endCageTx` and focused builder tests.
5. Server hard swap: `POST /facts/end`, indexer reads, route removal, Swagger.
6. Gate extension, PR metadata, and final verification.

## Design Decisions

- `EndFacts` carries the token id so `verifyEndFacts` and `endCageTx` are self-contained.
- `verifyEndFacts` takes `CageConfig` because the request-set completeness prefix is locally derived from `(request validator bytes, state policy id, token id, network)`.
- End requires `request_set.entries == []`. A valid completeness proof over non-empty entries is still rejected for this operation.
- `readStateUtxoAt` scans the state-validator address prefix in the UTxO CSMT and filters for the cage policy id plus token asset name.
- `readRequestSetAt` generates a completeness proof for the per-cage request address prefix. It is used by the end handler to prove emptiness.
- New facts DTOs must not grow `Cardano.MPFS.API.Types`. The first
  implementation slice splits shared primitives and per-operation facts
  into smaller modules, with `API.Types` kept as a compatibility
  re-export only if downstream imports require it.

## Files

```text
cardano-mpfs-api/lib/Cardano/MPFS/API.hs
cardano-mpfs-api/lib/Cardano/MPFS/API/Types.hs              # compatibility only
cardano-mpfs-api/lib/Cardano/MPFS/API/Types/Common.hs
cardano-mpfs-api/lib/Cardano/MPFS/API/Types/Facts.hs
cardano-mpfs-client/lib/Cardano/MPFS/Client/Facts.hs
cardano-mpfs-client/lib/Cardano/MPFS/Client/Verify.hs
cardano-mpfs-client/lib/Cardano/MPFS/Client/Verify/Completeness.hs
cardano-mpfs-client/lib/Cardano/MPFS/Client/Cage/Config.hs
cardano-mpfs-client/lib/Cardano/MPFS/Client/Cage/End.hs
cardano-mpfs-client/test/Cardano/MPFS/Client/EndFactsSpec.hs
cardano-mpfs-client/test/Cardano/MPFS/Client/Cage/EndSpec.hs
cardano-mpfs-offchain/lib/Cardano/MPFS/Indexer/Reads.hs
cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/Server.hs
cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/Types.hs        # compatibility only
cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/Types/Facts.hs
cardano-mpfs-offchain/test/Cardano/MPFS/HTTP/EndFactsSpec.hs
docs/assets/swagger.json
gate.sh
```
