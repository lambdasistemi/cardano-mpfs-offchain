# Research: Proof-Carrying API Responses

## Existing Proof Primitives Already Exist

The offchain context already exposes the primitives needed for UTxO
verification:

- `resolveUtxo :: TxIn -> m (Maybe ByteString)`
- `utxoProof :: TxIn -> m (Maybe ByteString)`
- `utxoRoot :: m (Maybe ByteString)`

The HTTP layer already exposes direct debugging endpoints for the same
data:

- `GET /utxo/:txId/:txIx`
- `GET /utxo/:txId/:txIx/proof`
- `GET /utxo/root`

This means the feature does not require new indexer persistence or new
cryptographic formats for UTxO proofs. The missing work is bundling these
proofs into the higher-level API responses.

## Root Endpoint Duplication Is Intentional

The service already exposes `GET /utxo/root`, and this should stay true
even after `GET /status` starts returning the same root.

Decision: treat the duplication as intentional, not accidental.

Why:

- `GET /status` is the ergonomic entry point for the high-level
  proof-carrying client flow.
- `GET /utxo/root` is the low-level debugging and compatibility endpoint
  that mirrors the underlying CSMT source of truth more directly.
- In isolated deployments, a client may have no separate CSMT service to
  consult, so this service must still be able to act as the Merkle-root
  source.

Constraint: both endpoints must read from the same indexed state, and
tests should assert they return the same root for the same snapshot.

## Query Endpoints Need Structured Response Objects

The affected query endpoints currently return scalar or flat payloads:

- `GET /tokens/:id` → `TokenStateJSON`
- `GET /tokens/:id/facts/:key` → `Hex`
- `GET /tokens/:id/proofs/:key` → `Hex`
- `GET /tokens/:id/requests` → `[RequestJSON]`
- `GET /status` → chain tip + checkpoint only

That shape cannot carry:

- the verification snapshot (`utxo_root`, checkpoint metadata)
- the resolved `TxOut` bytes for the relevant UTxO
- the UTxO-CSMT inclusion proof
- the MPF inclusion proof alongside the business payload

Decision: change the existing affected endpoints to structured JSON
objects rather than adding parallel `/v2` endpoints.

Why rejected:

- Parallel endpoints would duplicate Swagger surface area and test
  coverage.
- The issue asks to augment existing responses, not add an alternate API.
- Direct UTxO proof endpoints already exist for low-level debugging, so
  a second high-level API family would be redundant.

## Transaction Responses Need Builder-Level Metadata

Current transaction endpoints serialize a bare `Tx ConwayEra` to hex in
`HTTP.Server`, while `TxBuilder` returns only `m (Tx ConwayEra)`.

That is enough to sign and submit, but not enough to produce a
trust-minimized verification bundle because the HTTP layer cannot recover
all of the logical facts the builder used, especially:

- which token state UTxO was trusted
- which request UTxOs were selected from a larger set
- which trie keys/values need MPF proofs
- how bundled proofs map to individual requests in batched update/reject
  flows

Decision: enrich the transaction-builder boundary to return a structured
bundle, not just the unsigned transaction.

Likely shape:

- unsigned transaction
- witnessed consumed inputs (`TxIn`, resolved `TxOut`, UTxO proof)
- snapshot metadata (`utxo_root`, checkpoint identifiers)
- optional MPF proof section for trie-dependent operations

Alternative rejected: parse the finished transaction in `HTTP.Server`
and then derive proofs there. That can recover spent `TxIn`s, but it
cannot reliably reconstruct the builder's logical proof intent for MPF
data.

## Snapshot Consistency Must Be Explicit

The issue requires clients to verify responses against an independently
obtained UTxO-CSMT root. The service already exposes `/utxo/root`, but
inline proof-bearing responses need their own snapshot metadata so a
client can reject mixed-root bundles.

Decision: every proof-bearing response should carry the root (and, where
already available, checkpoint metadata) for the indexed snapshot that
the bundled proofs target.

Consequence:

- `GET /status` becomes the primary discovery endpoint for the current
  root.
- Inline query and transaction responses should either carry the same
  root directly or include equivalent snapshot metadata that unambiguously
  identifies it.

## No Indexer Write-Path Changes Required

The feature changes the HTTP contract and transaction-builder interface,
not the block-processing or RocksDB write path.

That keeps the constitutional risk low:

- Atomic block processing is unchanged.
- The server still returns unsigned transactions only.
- Proof bytes come from existing UTxO-CSMT and trie code paths rather
  than a new custom proof format.

## Boot Is a Special Case

`POST /tx/boot` still needs proof-bearing input witnesses for consumed
wallet UTxOs, but it has no pre-existing trie state to prove.

Decision: boot responses include UTxO witnesses and snapshot metadata,
but omit MPF proof sections entirely instead of inventing empty
placeholders.
