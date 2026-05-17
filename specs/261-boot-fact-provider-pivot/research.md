# Research: Boot fact-provider pivot

## Decision 1: Put BootFacts in the shared API/client surface

**Decision**: Add boot facts wire DTOs in `cardano-mpfs-api` and expose
client-facing mirrors through `cardano-mpfs-client`.

**Rationale**: The server and MOOG both need the same JSON contract. The
existing repository already keeps Servant API types in
`cardano-mpfs-api`, while `cardano-mpfs-client` owns pure verifier
contracts and typed client wrappers.

**Alternatives considered**:

- Server-local DTOs in `cardano-mpfs-offchain`: rejected because MOOG
  would need a duplicate contract.
- Client-only DTOs in `cardano-mpfs-client`: rejected because the server
  API package already owns the shared Servant contract.

## Decision 2: BootFacts has only snapshot, wallet UTxOs, and pp

**Decision**: `BootFacts` contains `snapshot`, `wallet_utxos`, and
`protocol_parameters`. No state UTxO, request UTxO, or MPF fact appears
in the boot slice.

**Rationale**: Boot is Tier 1. It mints the initial cage token from
wallet funding and does not consume pre-existing MPFS state.

**Alternatives considered**:

- Reuse the old unsigned tx response: rejected because it keeps the
  server as transaction-shape authority.
- Include state or trie facts for future operations: rejected because
  this child must not migrate non-boot endpoints.

## Decision 3: Reuse existing `readSnapshot` and `readWalletInputsAt`

**Decision**: Build `factsBootHandler` from one `runIndexerTx ctx` block
that reads `readSnapshot` and `readWalletInputsAt`.

**Rationale**: The boot slice needs no new indexer primitive. The
existing wallet read already returns `ResolvedWalletInput` with the UTxO
reference, TxOut CBOR, and CSMT inclusion proof.

**Alternatives considered**:

- Add a boot-specific indexer primitive: rejected as unnecessary
  duplication.
- Read snapshot and wallet UTxOs in separate actions: rejected because
  facts must be coherent at one snapshot.

## Decision 4: Capture legacy boot CBOR before deleting Real.Boot

**Decision**: Store the byte-equivalence reference at
`specs/261-boot-fact-provider-pivot/test-vectors/legacy-boot.cbor`
before removing the legacy boot route/builder.

**Rationale**: The acceptance criterion is byte identity for equivalent
inputs. Once the legacy builder is removed from the live server path, the
durable proof must be a checked-in vector or fixture that does not depend
on resurrecting deleted code.

**Alternatives considered**:

- Keep `Real.Boot` only for tests: rejected because the slice requires
  server-side boot tx building to be removed.
- Compare only semantic fields: rejected because the parent pivot
  requires byte-equivalent `Tx ConwayEra` CBOR.

## Decision 5: `verifyBootFacts` returns an opaque VerifiedBootFacts

**Decision**: The verifier exports `verifyBootFacts` and the
`VerifiedBootFacts` type, but not the constructor.

**Rationale**: The type boundary must make "verified before build"
checkable. `bootCageTx` should not accept raw facts.

**Alternatives considered**:

- Return `()` from the verifier and pass raw facts to the builder:
  rejected because callers could accidentally skip verification.
- Export an unsafe constructor: rejected for public API; tests can use a
  dedicated internal helper if needed.

## Decision 6: Keep non-boot legacy endpoints in this slice

**Decision**: Remove only the legacy boot transaction route and boot
server-side builder path. Leave request, retract, end, update, reject,
and sweep behavior unchanged.

**Rationale**: Issue #261 is the first child in a per-endpoint sequence.
Migrating non-boot endpoints would break the parent ticket ordering and
inflate the review surface.

**Alternatives considered**:

- Remove all transaction endpoints now: rejected because that belongs to
  the full parent epic, not this boot child.
- Keep boot legacy compatibility until MOOG migrates: rejected because
  the parent requires hard swaps per operation.
