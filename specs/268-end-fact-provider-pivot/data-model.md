# Data Model: End fact-provider pivot

## EndFacts

```haskell
data EndFacts = EndFacts
    { efSnapshot :: VerificationSnapshot
    , efToken :: TokenIdJSON
    , efStateUtxo :: UtxoEntry
    , efWalletUtxos :: [UtxoEntry]
    , efRequestSet :: UtxoSetWitness
    , efProtocolParameters :: UnverifiedPParams
    }
```

`efRequestSet.entries` must be empty for end. The completeness proof is still required so the verifier can distinguish "no requests exist" from "the server omitted requests".

## VerifiedEndFacts

```haskell
newtype VerifiedEndFacts = VerifiedEndFacts EndFacts

verifyEndFacts
    :: CageConfig
    -> TrustedRoot
    -> EndFacts
    -> Either VerifyError VerifiedEndFacts
```

The constructor is not exported. `endCageTx` consumes `VerifiedEndFacts`.

## Request-Set Prefix

The verifier derives:

```text
requestAddr = requestAddrFromCfg cageConfig token network
prefix = blake2b256(serialiseAddr requestAddr) as a CSMT Key
```

No server-provided prefix is trusted.

## Indexer Reads

```haskell
type ResolvedStateUtxo = (TxIn, ByteString, ByteString)

readStateUtxoAt
    :: Addr -> PolicyID -> TokenId -> IndexerTx (Maybe ResolvedStateUtxo)

readRequestSetAt
    :: Addr -> IndexerTx UtxoSetWitnessBytes
```

The exact helper names may vary, but both reads stay inside one `IndexerTx` composition in the handler.
