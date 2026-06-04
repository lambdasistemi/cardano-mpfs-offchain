# Tasks — #307 read-side verifiers

## Slice 1 — `verifyTokenState`

- [X] T001 Implement `verifyTokenState :: TrustedRoot -> TokenResponse
  -> Either VerifyError VerifiedTokenState` in
  `cardano-mpfs-verify/lib/Cardano/MPFS/Client/Verify/Read.hs` (opaque
  `VerifiedTokenState` + `verifiedTokenState` extractor, exports),
  reachable from the client test umbrella. Honest fixture accepts;
  forgeries (wrong trusted root, tampered state `tx_out`, tampered
  inclusion proof) reject with the matching `VerifyError`. Tests in
  `cardano-mpfs-client/test/...`.

## Slice 2 — `verifyTokenFacts`

- [X] T002 Implement `verifyTokenFacts :: TrustedRoot -> FactsResponse
  -> Either VerifyError VerifiedTokenFacts` in the same module (opaque
  `VerifiedTokenFacts` + `verifiedTokenFacts` extractor, exports):
  verify the embedded state anchoring (reuse slice 1), reconstruct the
  MPF root from `frsFacts` via `buildComposeFromList`/`scanMPFCompose`/
  `mpfRootFromNode`, assert it equals `root (wtsState frsState)`.
  Honest fixture accepts; completeness forgeries (drop a fact, add a
  spurious fact, tamper a fact value) reject.
