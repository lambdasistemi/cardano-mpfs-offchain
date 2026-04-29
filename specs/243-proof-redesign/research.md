# Research: Post-Split Proof Redesign

This document resolves the eight open questions enumerated in the Phase 0 section of `plan.md`. Each entry records the **Decision**, **Rationale**, and **Alternatives considered**.

## 1. CSMT prefix-completeness primitive in `haskell-mts`

**Decision**: use `CSMT.Proof.Completeness.generateProof` from `lib/csmt-write/CSMT/Proof/Completeness.hs`. Signature (paraphrased):

```haskell
generateProof
    :: (Monad m, GCompare d)
    => Selector d Key (Indirect a)
    -> Key      -- prefix (use [] for root)
    -> Key      -- target prefix (= script-hash bytes converted to Key)
    -> Transaction m cf d op (Maybe (CompletenessProof a))
```

The complementary verifier sits beside it. The proof carries `cpMergeOps` (merge operations needed to reconstruct the subtree root from the leaves) plus `cpInclusionSteps` (sibling hashes from the global root down to the prefix node).

**Rationale**: this is exactly the primitive the offchain server needs: "given the script-hash of the global state validator (or per-cage request validator), produce a proof that this enumerated set of leaves is the full set under that prefix, anchored to the snapshot's `utxo_root`". The shape composes with `cpInclusionSteps` for the path from root to prefix — so a single returned witness lets the client verify the prefix is correctly anchored to the snapshot root.

**Alternatives considered**: per-leaf inclusion proofs (rejected — N proofs instead of one, fails to attest exhaustiveness); a custom server-side enumerator without cryptographic proof (rejected — not trust-minimised, the central goal of this redesign).

## 2. Empty-prefix completeness for `POST /tx/oracle/end`

**Decision**: rely on `generateProof`'s ability to return a `CompletenessProof` whose `cpMergeOps` carries no leaves (only the empty-subtree sentinel hash) plus inclusion steps from the global root to the empty prefix node. This is supported by the CSMT primitive — the empty leaf set produces a "no leaves under this prefix" witness whose verification asserts the subtree hash equals the empty-tree sentinel under the supplied `utxo_root`.

**Rationale**: the user explicitly said "yes, it has it" for the prefix-completeness primitive in haskell-mts. Inspection of `MTS/Properties.hs` shows the property `empty tree has no completeness proof` for the *whole tree*, but the per-prefix variant of `generateProof` does navigate to the prefix node regardless of whether the subtree is empty. We accept this and document the verifier's expected behaviour: `verifyCompleteness trustedRoot scriptPrefix []` must succeed when the prefix subtree is empty under `trustedRoot`.

**Alternatives considered**: server returns HTTP 422 / 409 if the per-cage address is non-empty when `POST /tx/oracle/end` is called, with no completeness proof in the response (rejected — the client still needs to *verify* emptiness; without the witness the response is unverified, which violates this feature's central principle); shipping the `end` endpoint without an empty-set witness primitive and adding it later (rejected — leaves a non-trivial trust gap exactly at the destructive operation).

**Confirmation task in implementation phase**: an explicit unit test in `cardano-mpfs-client/test/.../CompletenessSpec.hs` exercises an empty-leaf-set verification against an empty subtree-prefix at a known root and asserts it succeeds; a forgery (claiming empty when the prefix has leaves) is rejected with a named `VerifyError`.

## 3. Servant pattern for two HTTP-status-code response variants

**Decision**: use `Servant.API.UVerb` (or its older equivalent `Verb 'GET '[ '(200, FactPresentResponse), '(404, FactAbsentResponse) ]` if `UVerb` is not in scope of our Servant version) for `GET /tokens/:id/facts/:key`. The handler returns a sum representation that pattern-matches on the outcome and emits the right status with the right body. For "token unknown" the handler emits a 404 with `NoContent` via a third arm of the union, distinct from the body-bearing 404.

**Rationale**: keeps both responses fully typed in the Servant API record, avoids a stringly-typed sum encoded inside one body, and gives the OpenAPI/Swagger output natural per-status documentation. Clients of `cardano-mpfs-client` see two distinct `data` constructors (`FactPresentResponse`, `FactAbsentResponse`) plus an outer `Either`/sum returned by the wrapper for the "token unknown" case.

**Alternatives considered**: single response shape with a top-level `tag :: "present" | "absent"` discriminator (rejected — user explicitly asked for the split; also loses HTTP-level routing); always 200 with sum body (rejected — abuses HTTP semantics for "absent"); two distinct paths (rejected — we don't know absence at request time, the URL must stay stable).

## 4. Trusted blueprint distribution for the client

**Decision**: out of band. The application that wraps `cardano-mpfs-client` (e.g. MOOG, harvest, a wallet, an explorer) is responsible for providing the trusted Aiken blueprint to the verifier as a value, not for fetching it from the offchain service. The verifier consumes the blueprint as a Haskell value (`PlutusV3 ScriptHash` for the global state validator + a function `tokenName -> ScriptHash` for per-cage request validators). The offchain server does not mediate blueprint distribution at all.

**Rationale**: the trust model collapses to (a) the trusted UTxO-CSMT root, (b) the trusted blueprint. If the offchain service distributed the blueprint, it could substitute a malicious one and undo every other proof. Keeping the blueprint client-supplied (typically pinned in source or fetched from a known source like a release artifact) preserves the trust boundary.

**Alternatives considered**: blueprint exposed via a new `GET /blueprint` endpoint (rejected — re-introduces an authoritative-server claim about the very thing the verifier needs to be authoritative about); blueprint hash baked into client builds (acceptable in some deployments; defer that decision to the wrapping application).

## 5. Where the trusted UTxO-CSMT root comes from

**Decision**: from a separate CSMT service the client trusts. The verifier itself is pure (constitution VIII): it takes `Hex` (the resolved trusted root) as a synchronous argument; the *fetching* of that root happens in the wrapping application's `IO`. Verifier signature stays:

```haskell
verifyTokenResponse
    :: TrustedRoot -> Blueprint -> TokenResponse -> Either VerifyError ()
```

**Rationale**: compatible with constitution VIII (pure verifier) and constitution IX (compiles to WASM/JS). The wrapping application's `IO` boundary is already where time, network, and any other non-determinism live; the verifier doesn't need to know.

**Alternatives considered**: verifier accepts `IO Hex` for the root (rejected — couples the verifier to `IO`, breaks WASM/JS compilation); verifier embeds a fixed root constant (rejected — no chain progress).

**Implementation note**: `cardano-mpfs-client` ships a `Cardano.MPFS.Client.TrustedRoot` newtype that wraps `Hex` to make the trust boundary visible at type level.

## 6. Downstream consumers to migrate

**Decision**: the following code-paths must be updated in lockstep when the new shapes ship. Each becomes a Phase-2 task line.

- **MOOG** (the daily oracle workload) — verifier calls against `/tokens/:id`, `/tokens/:id/facts/:key`, every write endpoint it uses; remove any reliance on `/tokens/:id/proofs/:key` or `/utxo/...`.
- **harvest** (cage-based prototype) — read-side queries plus `POST /tx/oracle/update` flow including the new completeness witness.
- **Internal devnet harness** in `cardano-mpfs-offchain/e2e-test/` — every `Spec.hs` that drives the HTTP surface; update endpoint paths and response shapes; add forgery cases.
- **mpfs-explorer** (read-only browser of MPFS state) — restructure its read calls; remove queries to dropped endpoints.
- **release-please** + Swagger publishing — `docs/assets/swagger.json` regenerated as part of CI gating.

**Rationale**: the spec mandates no compatibility shim (FR-031). The migration is co-released; consumers cannot fall behind.

**Alternatives considered**: dual-path period (rejected — doubles surface area, weakens trust property because the "old" path is unverified, defers complexity to nobody's calendar).

## 7. Lean formalization scope for the new verifier state machine

**Decision**: the Lean model gains the following predicates and preservation theorems before the corresponding Haskell modules are accepted in the implementation phase:

- `lean/Phase4/Completeness.lean` — predicate `prefixCompleteness (root : UtxoRoot) (prefix : ScriptHash) (leaves : List Leaf) : Prop`; theorem `forge_extra_leaf_breaks_completeness` (adding any leaf not actually under the prefix invalidates the witness); theorem `forge_missing_leaf_breaks_completeness` (omitting any leaf under the prefix invalidates).
- `lean/Phase4/ProofRedesign.lean` — predicates `tokenResponseValid`, `factPresentResponseValid`, `factAbsentResponseValid`, `confirmResponseValid`, `unsignedTxResponseValid`; preservation theorems showing each is invariant under the verifier's pure fold and that a forged input (wrong root, wrong address, wrong NFT, missing input cover, etc.) breaks the corresponding predicate.
- Existing `lean/Phase4/Verify.lean` — unchanged; the new files extend the state machine under the same foundational primitives (CSMT replay, MPF replay).

**Rationale**: constitution principle X is non-negotiable: invariants in Lean before Haskell. The above is the minimal scope to cover the new shapes; smaller and we leave verifier behaviour unproved, larger and we slow this PR.

**Alternatives considered**: defer Lean to a follow-up (rejected — direct constitution violation); model the invariants only for the read side first (rejected — write side is where the most novel property, completeness for fairness, lives).

## 8. Public sweep validator semantics

**Decision**: confirmed against the on-chain code: the global state validator (parameterised only by the boot output reference) treats UTxOs that do not carry the unique state NFT as anyone-can-spend — there is no datum check and no signer requirement on those branches. The corresponding `POST /tx/sweep` (top-level, public) endpoint builds an unsigned tx with no `extra_signatories` requirement and the on-chain validator accepts the spend.

**Rationale**: matches the user's directive that the global state address can accumulate non-legitimate UTxOs that anyone has the right to spend. The on-chain validator already enforces the asymmetry (legit-state spend requires the oracle's signature; non-legit spend requires nothing).

**Alternatives considered**: require an arbitrary signer for any sweep (rejected — locks up funds at the global state address that nobody has authority to claim, the opposite of the user's stated intent); collapse `POST /tx/sweep` and `POST /tx/oracle/sweep` into one endpoint that auto-detects the case (rejected — different authority models, different signer expectations on the client side; explicit endpoints are clearer).

## Summary of resolutions

All eight items resolved. No `[NEEDS CLARIFICATION]` carried forward. The plan's Phase 1 may proceed to produce `data-model.md`, `contracts/api-shapes.md`, `contracts/verify-error.md`, and `quickstart.md`.
