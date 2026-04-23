-- |
-- Module      : Cardano.MPFS.Client.Verify.Replay
-- Description : Pure CSMT / MPF proof replay against advertised roots.
--
-- Haskell mirror of the Lean preservation theorems in
-- @Phase4.Verify@ (`replay_binds_key`, `replay_binds_value`,
-- `replay_preserves_root_trust`). Every per-endpoint verifier in
-- "Cardano.MPFS.Client.Verify" threads a single advertised root
-- through every witness / trie-fact; here we cryptographically
-- replay each witness and emit structured 'VerifyError' values
-- so downstream consumers know which dotted field failed and
-- why.
--
-- Both replay primitives share an error-emission order per
-- @specs\/178-crypto-proof-replay\/contracts\/verify-error.md@:
-- structural → binding → root.
module Cardano.MPFS.Client.Verify.Replay
    ( VerifyError (..)
    , replayWitnessedUtxo
    , replayTrieFact
    ) where

import Codec.CBOR.Decoding qualified as CBOR
import Codec.CBOR.Read qualified as CBOR
import Data.ByteString (ByteString)
import Data.ByteString.Base16 qualified as Base16
import Data.ByteString.Lazy qualified as BL
import Data.Text (Text)
import Data.Text.Encoding qualified as T
import Data.Word (Word64)

import CSMT.Core.CBOR (parseProof)
import CSMT.Core.Hash
    ( Hash (..)
    , keyToByteString
    )
import CSMT.Core.Proof
    ( InclusionProof (..)
    )
import CSMT.Verify (verifyInclusionProof)
import CSMT.Verify.Blake2b (blake2b256)
import MPF.Verify
    ( verifyAikenExclusionProof
    , verifyAikenInclusionProof
    )

import Cardano.MPFS.Client.Bundle
    ( TrieFact (..)
    , TxIn (..)
    , WitnessedUtxo (..)
    )
import Cardano.MPFS.Client.Snapshot (Hex (..))

-- | Structured error returned when a proof-bearing bundle fails
-- verification. The @field@ text of each case is a dotted path
-- rooted at the endpoint name (e.g. @"boot.funding[0].utxo_proof"@)
-- so downstream tests can assert on the exact field granularity.
--
-- The 'CsmtReplayFailed' and 'MpfReplayFailed' constructors
-- emit reasons drawn from a fixed vocabulary documented in
-- @contracts\/verify-error.md@.
data VerifyError
    = -- | Field path and the offending value that failed hex decoding.
      MalformedHex Text Text
    | -- | Field path, expected byte length, actual byte length.
      WrongHexLength Text Int Int
    | -- | The snapshot's chainpoint block id decoded to zero bytes.
      EmptyBlockId
    | -- | The top-level transaction CBOR failed to hex-decode.
      MalformedTxCbor Text
    | -- | CSMT replay failed at this dotted field path with a
      -- reason drawn from the fixed vocabulary.
      CsmtReplayFailed Text Text
    | -- | MPF replay failed at this dotted field path with a
      -- reason drawn from the fixed vocabulary.
      MpfReplayFailed Text Text
    deriving stock (Eq, Show)

-- | Replay a 'WitnessedUtxo' against a trusted CSMT root.
-- Mirrors the @replayWitness@ transition in @Phase4.Verify@:
-- emits 'Left' (with a dotted field path and a fixed reason)
-- when the proof fails any of the three checks, 'Right ()'
-- otherwise.
--
-- Checks are ordered structural → binding → root:
--
-- 1. the proof CBOR decodes (@"malformed proof CBOR"@),
-- 2. the advertised @tx_in@\/@tx_out@ match the in-proof
--    @proofKey@\/@proofValue@ (@"key binding mismatch"@ and
--    @"value binding mismatch"@),
-- 3. the proof replays against @trustedRoot@
--    (@"root mismatch"@).
replayWitnessedUtxo
    :: Text
    -- ^ dotted field path, e.g.
    -- @"retract.state_ref"@. The @utxo_proof@ suffix is
    -- appended by this function.
    -> ByteString
    -- ^ trusted CSMT root bytes (32-byte Blake2b-256 hash).
    -> WitnessedUtxo
    -> Either VerifyError ()
replayWitnessedUtxo path trustedRoot WitnessedUtxo{..} = do
    proofBs <- decodeHex proofPath utxoProof
    txOutBs <- decodeHex (path <> ".tx_out") txOut
    txIdBs <- decodeHex (path <> ".tx_in.tx_id") (txId txIn)
    parsed <- case parseProof proofBs of
        Just p -> Right p
        Nothing ->
            Left (CsmtReplayFailed proofPath "malformed proof CBOR")
    let advertisedValueHash = blake2b256 txOutBs
        Hash proofValueBytes = proofValue parsed
        proofKeyBytes = keyToByteString (proofKey parsed)
    checkValueBinding advertisedValueHash proofValueBytes
    checkKeyBinding proofKeyBytes txIdBs (txIx txIn)
    checkRootReplay proofBs
  where
    proofPath = path <> ".utxo_proof"
    checkValueBinding advertised proofVal
        | advertised == proofVal = Right ()
        | otherwise =
            Left
                ( CsmtReplayFailed
                    proofPath
                    "value binding mismatch"
                )
    checkKeyBinding proofKeyBs advertisedTxIdBs advertisedTxIx =
        case decodeTxInBytes proofKeyBs of
            Just (k, ix)
                | k == advertisedTxIdBs
                , ix == advertisedTxIx ->
                    Right ()
            _ ->
                Left
                    ( CsmtReplayFailed
                        proofPath
                        "key binding mismatch"
                    )
    checkRootReplay proofBs
        | verifyInclusionProof trustedRoot proofBs = Right ()
        | otherwise =
            Left (CsmtReplayFailed proofPath "root mismatch")

-- | Replay a 'TrieFact' against a trusted MPF trie root.
-- Mirrors @replayTrieFact@ in @Phase4.Verify@. Dispatches on
-- 'TrieFact.value':
--
-- * @Just v@ — inclusion claim; uses
--   'verifyAikenInclusionProof' against the advertised
--   key\/value.
-- * @Nothing@ — exclusion claim; uses
--   'verifyAikenExclusionProof' against the advertised key.
--
-- The Aiken verifier folds (key, value) into the proof while
-- recomputing the root, so a single @"root mismatch"@ covers
-- both the cryptographic root check and the binding to the
-- advertised key\/value. Shape mismatches (inclusion-proof
-- bytes under an absence claim, or vice versa) fall through
-- to a structured reason.
replayTrieFact
    :: Text
    -- ^ dotted field path, e.g.
    -- @"update.trie_read[0]"@. The @mpf_proof@ suffix is
    -- appended by this function.
    -> ByteString
    -- ^ trusted MPF root bytes.
    -> TrieFact
    -> Either VerifyError ()
replayTrieFact path trustedRoot TrieFact{..} = do
    proofBs <- decodeHex proofPath mpfProof
    keyBs <- decodeHex (path <> ".key") key
    case value of
        Just v -> do
            valueBs <- decodeHex (path <> ".value") v
            let okInclusion =
                    verifyAikenInclusionProof
                        trustedRoot
                        keyBs
                        valueBs
                        proofBs
                okExclusion =
                    verifyAikenExclusionProof
                        trustedRoot
                        keyBs
                        proofBs
            if okInclusion
                then Right ()
                else
                    if okExclusion
                        then
                            Left
                                ( MpfReplayFailed
                                    proofPath
                                    "exclusion proof for inclusion claim"
                                )
                        else
                            Left
                                ( MpfReplayFailed
                                    proofPath
                                    "root mismatch"
                                )
        Nothing -> do
            let okExclusion =
                    verifyAikenExclusionProof
                        trustedRoot
                        keyBs
                        proofBs
                -- Shape-mismatch probe: an inclusion proof
                -- would not validate via
                -- 'verifyAikenExclusionProof', so its failure
                -- mode is unambiguous relative to a genuine
                -- exclusion proof. Hash the key as the probe
                -- value so we do not leak distinguishing
                -- bytes.
                okInclusion =
                    verifyAikenInclusionProof
                        trustedRoot
                        keyBs
                        keyBs
                        proofBs
            if okExclusion
                then Right ()
                else
                    if okInclusion
                        then
                            Left
                                ( MpfReplayFailed
                                    proofPath
                                    "inclusion proof for absence claim"
                                )
                        else
                            Left
                                ( MpfReplayFailed
                                    proofPath
                                    "root mismatch"
                                )
  where
    proofPath = path <> ".mpf_proof"

-- | Decode the raw bytes of a @Shelley.TxIn@ as a CBOR
-- 2-element list of @(txIdBytes, txIx)@. The MPFS server
-- stores UTxO keys as @cborEncode (Shelley.TxIn ...)@, whose
-- CBOR layout is a 2-element list of @[bytestring, uint]@.
-- Returns 'Nothing' if the bytes do not conform.
decodeTxInBytes :: ByteString -> Maybe (ByteString, Word64)
decodeTxInBytes bs =
    case CBOR.deserialiseFromBytes decoder (BL.fromStrict bs) of
        Right (_, result) -> Just result
        _ -> Nothing
  where
    decoder :: CBOR.Decoder s (ByteString, Word64)
    decoder = do
        _ <- CBOR.decodeListLen
        txIdBytes <- CBOR.decodeBytes
        txIxWord <- CBOR.decodeWord64
        pure (txIdBytes, txIxWord)

-- | Hex-decode a 'Hex' field and wrap failures as
-- 'MalformedHex' at the given path.
decodeHex :: Text -> Hex -> Either VerifyError ByteString
decodeHex field (Hex txt) =
    case Base16.decode (T.encodeUtf8 txt) of
        Right bs -> Right bs
        Left _ -> Left (MalformedHex field txt)
