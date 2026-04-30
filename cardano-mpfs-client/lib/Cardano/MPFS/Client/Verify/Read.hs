{-# LANGUAGE LambdaCase #-}

-- |
-- Module      : Cardano.MPFS.Client.Verify.Read
-- Description : Verifiers for the read-side responses.
--
-- Two trust-minimised verifiers for @GET \/tokens\/:id\/facts\/:key@:
--
--  * 'verifyFactPresentResponse' — the trie contains the key
--    (HTTP 200). Replays the state UTxO inclusion proof against
--    the externally-supplied 'TrustedRoot', recovers the trie
--    root from the decoded @state_utxo.txout_cbor@'s inline
--    datum, then replays an MPF inclusion proof against that
--    trie root with the advertised @key@/@value@.
--
--  * 'verifyFactAbsentResponse' — the cage exists but the trie
--    does not contain the key (HTTP 404 with body). Same state
--    UTxO replay, but instead of an inclusion proof the verifier
--    runs an MPF /exclusion/ proof against the recovered trie
--    root with the advertised @key@.
--
-- Both verifiers delegate cryptographic soundness to upstream
-- @csmt-verify@ (CSMT inclusion) and @mpf-verify@
-- (MPF inclusion / exclusion). This module enforces the
-- structural binding (snapshot ↔ trusted root, datum ↔ trie
-- root, key/value ↔ proof) and emits structured 'VerifyError'
-- values rooted at @"fact_present"@ / @"fact_absent"@ dotted
-- field paths.
module Cardano.MPFS.Client.Verify.Read
    ( verifyFactPresentResponse
    , verifyFactAbsentResponse
    ) where

import Codec.CBOR.Term qualified as CBOR
import Data.ByteString qualified as BS
import Data.ByteString.Base16 qualified as Base16
import Data.Text (Text)
import Data.Text.Encoding qualified as TE

import MPF.Verify
    ( verifyAikenExclusionProof
    , verifyAikenInclusionProof
    )

import Cardano.MPFS.API.Encoding qualified as Wire
import Cardano.MPFS.API.Types
    ( FactAbsentResponse (..)
    , FactPresentResponse (..)
    , UtxoEntry (..)
    , VerificationSnapshot (..)
    )
import Cardano.MPFS.Client.Snapshot qualified as Client
import Cardano.MPFS.Client.TrustedRoot (TrustedRoot (..))
import Cardano.MPFS.Client.Verify.Replay
    ( VerifyError (..)
    , replayUtxoEntry
    )
import Cardano.MPFS.Client.Verify.TxView
    ( TxOutView (..)
    , decodeTxOutView
    )

-- | Verify a @GET \/tokens\/:id\/facts\/:key@ HTTP 200
-- response.
--
-- Five checks, in order:
--
--  1. @snapshot.utxo_root@ matches the externally-supplied
--     'TrustedRoot' byte-for-byte.
--  2. The @state_utxo@'s CSMT inclusion proof replays
--     against the trusted root.
--  3. The @state_utxo.txout_cbor@ decodes; the trie root
--     is recovered from its inline State datum.
--  4. The MPF inclusion proof verifies the advertised
--     @(key, value)@ against the recovered trie root.
verifyFactPresentResponse
    :: TrustedRoot
    -> Wire.Hex
    -- ^ key path captured from the URL (raw bytes).
    -> FactPresentResponse
    -> Either VerifyError ()
verifyFactPresentResponse trustedRoot keyHex response = do
    let snapshotPath =
            "fact_present.snapshot.utxo_root"
        statePath = "fact_present.state_utxo"
    rootBs <-
        bindSnapshotRoot
            snapshotPath
            trustedRoot
            (fprSnapshot response)
    replayUtxoEntry statePath rootBs (fprStateUtxo response)
    trieRootBs <-
        recoverTrieRoot
            "fact_present.state_utxo.txout_cbor"
            (ueTxOutCbor (fprStateUtxo response))
    let mpfProofBs =
            Wire.unHex (fprMpfInclusionProof response)
        keyBs = Wire.unHex keyHex
        valueBs = Wire.unHex (fprValue response)
    if verifyAikenInclusionProof
        trieRootBs
        keyBs
        valueBs
        mpfProofBs
        then Right ()
        else
            Left
                ( MpfInclusionInvalid
                    "fact_present.mpf_inclusion_proof"
                )

-- | Verify a @GET \/tokens\/:id\/facts\/:key@ HTTP 404
-- (with body) response.
--
-- Mirror of 'verifyFactPresentResponse' for the absence
-- case: same state UTxO replay and trie-root recovery,
-- but the cryptographic primitive is
-- 'verifyAikenExclusionProof'.
verifyFactAbsentResponse
    :: TrustedRoot
    -> Wire.Hex
    -- ^ key path captured from the URL (raw bytes).
    -> FactAbsentResponse
    -> Either VerifyError ()
verifyFactAbsentResponse trustedRoot keyHex response = do
    let snapshotPath =
            "fact_absent.snapshot.utxo_root"
        statePath = "fact_absent.state_utxo"
    rootBs <-
        bindSnapshotRoot
            snapshotPath
            trustedRoot
            (farSnapshot response)
    replayUtxoEntry statePath rootBs (farStateUtxo response)
    trieRootBs <-
        recoverTrieRoot
            "fact_absent.state_utxo.txout_cbor"
            (ueTxOutCbor (farStateUtxo response))
    let mpfProofBs =
            Wire.unHex (farMpfExclusionProof response)
        keyBs = Wire.unHex keyHex
    if verifyAikenExclusionProof
        trieRootBs
        keyBs
        mpfProofBs
        then Right ()
        else
            Left
                ( MpfExclusionInvalid
                    "fact_absent.mpf_exclusion_proof"
                )

-- | Bind the response's @snapshot.utxo_root@ to the
-- externally-supplied 'TrustedRoot', returning the root
-- bytes when they match (32-byte length-checked) or
-- 'WrongHexLength' / 'TrustedRootMismatch' otherwise.
bindSnapshotRoot
    :: Text
    -> TrustedRoot
    -> VerificationSnapshot
    -> Either VerifyError BS.ByteString
bindSnapshotRoot path (TrustedRoot (Wire.Hex trustedBs)) snap = do
    checkLength "trusted_root" trustedBs
    let Wire.Hex snapshotBs = vsUtxoRoot snap
    checkLength path snapshotBs
    if snapshotBs == trustedBs
        then Right trustedBs
        else Left (TrustedRootMismatch path)
  where
    checkLength field bs
        | BS.length bs == 32 = Right ()
        | otherwise =
            Left (WrongHexLength field 32 (BS.length bs))

-- | Decode the state UTxO's @txout_cbor@ as a Babbage
-- output, extract its inline datum, and recover the
-- trie root from the State datum's second field.
--
-- The on-chain shape under @cardano-mpfs-cage@ is:
--
-- > CageDatum.StateDatum
-- >     = Constr 1
-- >         [ Constr 0
-- >             [ owner    :: B    -- 28 bytes
-- >             , root     :: B    -- 32 bytes
-- >             , max_fee  :: I
-- >             , process  :: I
-- >             , retract  :: I
-- >             ]
-- >         ]
--
-- Encoded as PlutusData CBOR via @TTagged (121+n)@ for
-- small constructor tags. We pattern-match on that
-- exact shape and return the @root@ bytes; any other
-- shape emits 'StateDatumMalformed'.
recoverTrieRoot
    :: Text -> Wire.Hex -> Either VerifyError BS.ByteString
recoverTrieRoot path (Wire.Hex bytes) = do
    let clientHex =
            Client.Hex
                ( TE.decodeUtf8
                    (Base16.encode bytes)
                )
    txOutView <- decodeTxOutView path clientHex
    case txOutInlineDatum txOutView of
        Nothing ->
            Left
                ( StateDatumMalformed
                    (path <> ".datum.inline (missing)")
                )
        Just datumTerm ->
            extractTrieRoot path datumTerm

-- | Pull the trie root bytes out of a decoded inline
-- datum term. Tolerates both indefinite- and
-- definite-length CBOR list encodings produced by
-- different builders.
extractTrieRoot
    :: Text -> CBOR.Term -> Either VerifyError BS.ByteString
extractTrieRoot path = \case
    CBOR.TTagged 122 (CBOR.TList [stateTerm]) ->
        extractFromState path stateTerm
    CBOR.TTagged 122 (CBOR.TListI [stateTerm]) ->
        extractFromState path stateTerm
    _ ->
        Left
            ( StateDatumMalformed
                (path <> ".datum (expected Constr 1)")
            )
  where
    extractFromState p = \case
        CBOR.TTagged 121 (CBOR.TList fields) ->
            extractRootField p fields
        CBOR.TTagged 121 (CBOR.TListI fields) ->
            extractRootField p fields
        _ ->
            Left
                ( StateDatumMalformed
                    ( p
                        <> ".datum (expected inner \
                           \Constr 0)"
                    )
                )

    extractRootField p = \case
        (_owner : CBOR.TBytes rootBs : _rest)
            | BS.length rootBs == 32 -> Right rootBs
            | otherwise ->
                Left
                    ( WrongHexLength
                        (p <> ".datum.root")
                        32
                        (BS.length rootBs)
                    )
        _ ->
            Left
                ( StateDatumMalformed
                    ( p
                        <> ".datum.root (expected \
                           \32-byte bytestring)"
                    )
                )
