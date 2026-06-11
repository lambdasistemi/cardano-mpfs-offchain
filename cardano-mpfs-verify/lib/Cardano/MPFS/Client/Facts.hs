{-# LANGUAGE LambdaCase #-}

-- |
-- Module      : Cardano.MPFS.Client.Facts
-- Description : Client surface for facts-only proof responses.
--
-- Reuses the shared API wire DTOs and adds the opaque
-- post-verification witness returned by facts-only verifiers.
module Cardano.MPFS.Client.Facts
    ( BootFacts (..)
    , RequestInsertFacts (..)
    , RequestDeleteFacts (..)
    , RequestUpdateFacts (..)
    , RejectFacts (..)
    , RetractFacts (..)
    , EndFacts (..)
    , UpdateFacts (..)
    , FactPresentFacts (..)
    , FactAbsentFacts (..)
    , UnverifiedPParams (..)
    , VerifiedBootFacts
    , VerifiedRequestInsertFacts
    , VerifiedRequestDeleteFacts
    , VerifiedRequestUpdateFacts
    , VerifiedUpdateFacts
    , VerifiedRetractFacts
    , VerifiedRejectFacts
    , VerifiedEndFacts
    , VerifiedFactPresentFacts
    , VerifiedFactAbsentFacts
    , verifiedBootFacts
    , verifiedRequestInsertFacts
    , verifiedRequestDeleteFacts
    , verifiedRequestUpdateFacts
    , verifiedUpdateFacts
    , verifiedRetractFacts
    , verifiedRejectFacts
    , verifiedEndFacts
    , verifiedFactPresentFacts
    , verifiedFactAbsentFacts
    , verifyBootFacts
    , verifyRequestInsertFacts
    , verifyRequestDeleteFacts
    , verifyRequestUpdateFacts
    , verifyUpdateFacts
    , verifyRetractFacts
    , verifyRejectFacts
    , verifyEndFacts
    , verifyFactPresentFacts
    , verifyFactAbsentFacts
    ) where

import Codec.CBOR.Term qualified as CBOR
import Control.Monad (when)
import Data.ByteString qualified as BS
import Data.ByteString.Base16 qualified as Base16
import Data.Foldable (traverse_)
import Data.Text qualified as T
import Data.Text.Encoding qualified as T

import Cardano.MPFS.API.Encoding (Hex (..))
import Cardano.MPFS.API.Types
    ( FactResponse (..)
    , FactWitness (..)
    , ProofResponse (..)
    , TxInJSON (..)
    , WitnessedTokenState (..)
    , WitnessedUtxo (..)
    )
import Cardano.MPFS.API.Types.Common
    ( ChainPointJSON (..)
    , UnverifiedPParams (..)
    , UtxoEntry (..)
    , UtxoEntryRefOnly (..)
    , UtxoRef (..)
    , UtxoSetWitness (..)
    , VerificationSnapshot (..)
    )
import Cardano.MPFS.API.Types.Facts
    ( BootFacts (..)
    , EndFacts (..)
    , RejectFacts (..)
    , RequestDeleteFacts (..)
    , RequestInsertFacts (..)
    , RequestUpdateFacts (..)
    , RetractFacts (..)
    , TrieFact (..)
    , UpdateFacts (..)
    )
import Cardano.MPFS.Client.Bundle qualified as ClientWire
import Cardano.MPFS.Client.Cage.Config
    ( CageConfig
    )
import Cardano.MPFS.Client.Cage.Identity
    ( requestSetPrefixFromCfg
    )
import Cardano.MPFS.Client.Snapshot qualified as ClientSnapshot
import Cardano.MPFS.Client.TrustedRoot
    ( TrustedRoot (..)
    )
import Cardano.MPFS.Client.Verify.Completeness
    ( verifyUtxoSetCompleteness
    )
import Cardano.MPFS.Client.Verify.MPF
    ( computeAikenProofRoot
    )
import Cardano.MPFS.Client.Verify.Replay
    ( VerifyError (..)
    , replayTrieFact
    , replayUtxoEntry
    , replayWitnessedUtxo
    )
import Cardano.MPFS.Client.Verify.Snapshot
    ( verifyVerificationSnapshot
    )
import Cardano.MPFS.Client.Verify.TxView
    ( TxOutView (..)
    , decodeTxOutView
    , stateRootBytesFromWitness
    , verifyStateRootBinding
    )

-- | Opaque witness that boot facts have been checked against the
-- caller-supplied trusted root. The constructor is intentionally
-- not exported, so public callers cannot bypass 'verifyBootFacts'.
newtype VerifiedBootFacts = VerifiedBootFacts BootFacts
    deriving stock (Eq, Show)

-- | Opaque witness that request-insert facts have been checked
-- against the caller-supplied trusted root.
newtype VerifiedRequestInsertFacts
    = VerifiedRequestInsertFacts RequestInsertFacts
    deriving stock (Eq, Show)

-- | Opaque witness that request-delete facts have been checked
-- against the caller-supplied trusted root.
newtype VerifiedRequestDeleteFacts
    = VerifiedRequestDeleteFacts RequestDeleteFacts
    deriving stock (Eq, Show)

-- | Opaque witness that request-update facts have been checked
-- against the caller-supplied trusted root.
newtype VerifiedRequestUpdateFacts
    = VerifiedRequestUpdateFacts RequestUpdateFacts
    deriving stock (Eq, Show)

-- | Opaque witness that update facts have been checked against
-- the caller-supplied trusted root and the advertised trie root.
newtype VerifiedUpdateFacts = VerifiedUpdateFacts UpdateFacts
    deriving stock (Eq, Show)

-- | Opaque witness that retract facts have been checked
-- against the caller-supplied trusted root. The constructor is
-- intentionally not exported, so public callers cannot bypass
-- 'verifyRetractFacts'.
newtype VerifiedRetractFacts
    = VerifiedRetractFacts RetractFacts
    deriving stock (Eq, Show)

-- | Opaque witness that reject facts have been checked
-- against the caller-supplied trusted root. Constructor is
-- not exported; public callers go through 'verifyRejectFacts'.
newtype VerifiedRejectFacts
    = VerifiedRejectFacts RejectFacts
    deriving stock (Eq, Show)

-- | Opaque witness that end facts have been checked against the
-- caller-supplied trusted root and locally-derived request prefix.
newtype VerifiedEndFacts = VerifiedEndFacts EndFacts
    deriving stock (Eq, Show)

-- | Client-side verifier input for @GET /tokens/:id/facts/:key@.
-- The server response intentionally keeps the path key out of the
-- JSON body, so callers pair the decoded response with the key they
-- requested before replaying the MPF proof.
data FactPresentFacts = FactPresentFacts
    { fpfKey :: Hex
    , fpfResponse :: FactResponse
    }
    deriving stock (Eq, Show)

-- | Client-side verifier input for @GET /tokens/:id/proofs/:key@
-- when the key is absent from the trie.
data FactAbsentFacts = FactAbsentFacts
    { fafKey :: Hex
    , fafResponse :: ProofResponse
    }
    deriving stock (Eq, Show)

-- | Opaque witness that a present fact response has been checked
-- against the caller-supplied trusted root and the token trie root.
newtype VerifiedFactPresentFacts
    = VerifiedFactPresentFacts FactPresentFacts
    deriving stock (Eq, Show)

-- | Opaque witness that an absent fact response has been checked
-- against the caller-supplied trusted root and the token trie root.
newtype VerifiedFactAbsentFacts
    = VerifiedFactAbsentFacts FactAbsentFacts
    deriving stock (Eq, Show)

-- | Extract the verified facts after 'verifyBootFacts' has
-- established the trusted-root and CSMT proof checks.
verifiedBootFacts :: VerifiedBootFacts -> BootFacts
verifiedBootFacts (VerifiedBootFacts facts) = facts

-- | Extract the verified facts after 'verifyRequestInsertFacts'
-- has established the trusted-root and CSMT proof checks.
verifiedRequestInsertFacts
    :: VerifiedRequestInsertFacts -> RequestInsertFacts
verifiedRequestInsertFacts (VerifiedRequestInsertFacts facts) =
    facts

-- | Extract the verified facts after 'verifyRequestDeleteFacts'
-- has established the trusted-root and CSMT proof checks.
verifiedRequestDeleteFacts
    :: VerifiedRequestDeleteFacts -> RequestDeleteFacts
verifiedRequestDeleteFacts (VerifiedRequestDeleteFacts facts) =
    facts

-- | Extract the verified facts after 'verifyRequestUpdateFacts'
-- has established the trusted-root and CSMT proof checks.
verifiedRequestUpdateFacts
    :: VerifiedRequestUpdateFacts -> RequestUpdateFacts
verifiedRequestUpdateFacts (VerifiedRequestUpdateFacts facts) =
    facts

-- | Extract the verified facts after 'verifyUpdateFacts'
-- has established trusted-root, CSMT, state-root, and MPF
-- checks.
verifiedUpdateFacts :: VerifiedUpdateFacts -> UpdateFacts
verifiedUpdateFacts (VerifiedUpdateFacts facts) = facts

-- | Extract the verified facts after 'verifyRetractFacts'
-- has established the trusted-root and CSMT proof checks for
-- the named request UTxO, state UTxO, and wallet UTxOs.
verifiedRetractFacts
    :: VerifiedRetractFacts -> RetractFacts
verifiedRetractFacts (VerifiedRetractFacts facts) =
    facts

-- | Extract the verified facts after 'verifyRejectFacts'
-- has established the trusted-root, CSMT proof, and
-- validity-slot checks.
verifiedRejectFacts
    :: VerifiedRejectFacts -> RejectFacts
verifiedRejectFacts (VerifiedRejectFacts facts) =
    facts

-- | Extract the verified facts after 'verifyEndFacts' has
-- established the trusted-root, inclusion proof, and
-- request-set completeness checks.
verifiedEndFacts :: VerifiedEndFacts -> EndFacts
verifiedEndFacts (VerifiedEndFacts facts) = facts

-- | Extract the verified facts after 'verifyFactPresentFacts' has
-- established the trusted-root, state-UTxO, and MPF inclusion checks.
verifiedFactPresentFacts
    :: VerifiedFactPresentFacts -> FactPresentFacts
verifiedFactPresentFacts (VerifiedFactPresentFacts facts) = facts

-- | Extract the verified facts after 'verifyFactAbsentFacts' has
-- established the trusted-root, state-UTxO, and MPF exclusion checks.
verifiedFactAbsentFacts :: VerifiedFactAbsentFacts -> FactAbsentFacts
verifiedFactAbsentFacts (VerifiedFactAbsentFacts facts) = facts

-- | Verify a facts-only boot response against an externally-supplied
-- trusted UTxO-CSMT root.
verifyBootFacts
    :: TrustedRoot
    -> BootFacts
    -> Either VerifyError VerifiedBootFacts
verifyBootFacts
    (TrustedRoot (Hex trustedBs))
    facts@BootFacts{..} = do
        checkLength "boot.trusted_root" trustedBs
        let snapshotPath = "boot.snapshot.utxo_root"
            Hex snapshotBs = vsUtxoRoot bfSnapshot
        checkLength snapshotPath snapshotBs
        if snapshotBs == trustedBs
            then Right ()
            else Left (TrustedRootMismatch snapshotPath)
        replayWalletUtxos trustedBs bfWalletUtxos
        Right (VerifiedBootFacts facts)
      where
        checkLength field bs
            | BS.length bs == 32 = Right ()
            | otherwise =
                Left (WrongHexLength field 32 (BS.length bs))
        replayWalletUtxos root entries =
            mapM_
                ( \(ix, entry) ->
                    replayUtxoEntry
                        ( "boot.wallet_utxos["
                            <> T.pack (show (ix :: Int))
                            <> "]"
                        )
                        root
                        entry
                )
                (zip [0 ..] entries)

-- | Verify a facts-only request-insert response against an
-- externally-supplied trusted UTxO-CSMT root.
verifyRequestInsertFacts
    :: TrustedRoot
    -> RequestInsertFacts
    -> Either VerifyError VerifiedRequestInsertFacts
verifyRequestInsertFacts
    (TrustedRoot (Hex trustedBs))
    facts@RequestInsertFacts{..} = do
        checkLength "request_insert.trusted_root" trustedBs
        let snapshotPath = "request_insert.snapshot.utxo_root"
            Hex snapshotBs = vsUtxoRoot rifSnapshot
        checkLength snapshotPath snapshotBs
        if snapshotBs == trustedBs
            then Right ()
            else Left (TrustedRootMismatch snapshotPath)
        replayWalletUtxos trustedBs rifWalletUtxos
        Right (VerifiedRequestInsertFacts facts)
      where
        checkLength field bs
            | BS.length bs == 32 = Right ()
            | otherwise =
                Left (WrongHexLength field 32 (BS.length bs))
        replayWalletUtxos root entries =
            mapM_
                ( \(ix, entry) ->
                    replayUtxoEntry
                        ( "request_insert.wallet_utxos["
                            <> T.pack (show (ix :: Int))
                            <> "]"
                        )
                        root
                        entry
                )
                (zip [0 ..] entries)

-- | Verify a facts-only request-delete response against an
-- externally-supplied trusted UTxO-CSMT root.
verifyRequestDeleteFacts
    :: TrustedRoot
    -> RequestDeleteFacts
    -> Either VerifyError VerifiedRequestDeleteFacts
verifyRequestDeleteFacts
    (TrustedRoot (Hex trustedBs))
    facts@RequestDeleteFacts{..} = do
        checkLength "request_delete.trusted_root" trustedBs
        let snapshotPath = "request_delete.snapshot.utxo_root"
            Hex snapshotBs = vsUtxoRoot rdfSnapshot
        checkLength snapshotPath snapshotBs
        if snapshotBs == trustedBs
            then Right ()
            else Left (TrustedRootMismatch snapshotPath)
        replayWalletUtxos trustedBs rdfWalletUtxos
        Right (VerifiedRequestDeleteFacts facts)
      where
        checkLength field bs
            | BS.length bs == 32 = Right ()
            | otherwise =
                Left (WrongHexLength field 32 (BS.length bs))
        replayWalletUtxos root entries =
            mapM_
                ( \(ix, entry) ->
                    replayUtxoEntry
                        ( "request_delete.wallet_utxos["
                            <> T.pack (show (ix :: Int))
                            <> "]"
                        )
                        root
                        entry
                )
                (zip [0 ..] entries)

-- | Verify a facts-only request-update response against an
-- externally-supplied trusted UTxO-CSMT root.
verifyRequestUpdateFacts
    :: TrustedRoot
    -> RequestUpdateFacts
    -> Either VerifyError VerifiedRequestUpdateFacts
verifyRequestUpdateFacts
    (TrustedRoot (Hex trustedBs))
    facts@RequestUpdateFacts{..} = do
        checkLength "request_update.trusted_root" trustedBs
        let snapshotPath = "request_update.snapshot.utxo_root"
            Hex snapshotBs = vsUtxoRoot rufSnapshot
        checkLength snapshotPath snapshotBs
        if snapshotBs == trustedBs
            then Right ()
            else Left (TrustedRootMismatch snapshotPath)
        replayWalletUtxos trustedBs rufWalletUtxos
        Right (VerifiedRequestUpdateFacts facts)
      where
        checkLength field bs
            | BS.length bs == 32 = Right ()
            | otherwise =
                Left (WrongHexLength field 32 (BS.length bs))
        replayWalletUtxos root entries =
            mapM_
                ( \(ix, entry) ->
                    replayUtxoEntry
                        ( "request_update.wallet_utxos["
                            <> T.pack (show (ix :: Int))
                            <> "]"
                        )
                        root
                        entry
                )
                (zip [0 ..] entries)

-- | Verify a facts-only update response against an
-- externally-supplied trusted UTxO-CSMT root. Replays the
-- inclusion proofs for the state UTxO, every request UTxO,
-- every wallet UTxO, binds the advertised trie root to the
-- state datum, and replays every MPF trie fact against that
-- root.
verifyUpdateFacts
    :: TrustedRoot
    -> UpdateFacts
    -> Either VerifyError VerifiedUpdateFacts
verifyUpdateFacts
    (TrustedRoot (Hex trustedBs))
    facts@UpdateFacts{..} = do
        checkLength "update.trusted_root" trustedBs
        let snapshotPath = "update.snapshot.utxo_root"
            Hex snapshotBs = vsUtxoRoot ufSnapshot
        checkLength snapshotPath snapshotBs
        if snapshotBs == trustedBs
            then Right ()
            else Left (TrustedRootMismatch snapshotPath)
        replayUtxoEntry "update.state_utxo" trustedBs ufStateUtxo
        replayRequestUtxos trustedBs ufRequestUtxos
        replayWalletUtxos trustedBs ufWalletUtxos
        let Hex trieRootBs = ufTrieRoot
        checkLength "update.trie_root" trieRootBs
        verifyStateRootBinding
            "update"
            (toClientWitnessedUtxoEntry ufStateUtxo)
            (toClientHex ufTrieRoot)
        verifyUpdateValidityUpperSlot facts
        replayUpdateTrieFacts trieRootBs ufRequestUtxos ufTrieFacts
        Right (VerifiedUpdateFacts facts)
      where
        checkLength field bs
            | BS.length bs == 32 = Right ()
            | otherwise =
                Left (WrongHexLength field 32 (BS.length bs))
        replayRequestUtxos root entries =
            mapM_
                ( \(ix, entry) ->
                    replayUtxoEntry
                        ( "update.request_utxos["
                            <> T.pack (show (ix :: Int))
                            <> "]"
                        )
                        root
                        entry
                )
                (zip [0 ..] entries)
        replayWalletUtxos root entries =
            mapM_
                ( \(ix, entry) ->
                    replayUtxoEntry
                        ( "update.wallet_utxos["
                            <> T.pack (show (ix :: Int))
                            <> "]"
                        )
                        root
                        entry
                )
                (zip [0 ..] entries)

data UpdateRequestChange = UpdateRequestChange
    { urcKey :: !BS.ByteString
    , urcOperation :: !UpdateOperation
    }

data UpdateOperation
    = UpdateInsert !BS.ByteString
    | UpdateDelete !BS.ByteString
    | UpdateReplace !BS.ByteString !BS.ByteString

replayUpdateTrieFacts
    :: BS.ByteString
    -> [UtxoEntry]
    -> [TrieFact]
    -> Either VerifyError ()
replayUpdateTrieFacts root requestEntries trieEntries
    | length requestEntries /= length trieEntries =
        Left
            $ TxBindingFailed
                "update.trie_facts"
                "length must match update.request_utxos"
    | otherwise =
        go root (zip3 [0 :: Int ..] requestEntries trieEntries)
  where
    go _ [] = Right ()
    go currentRoot ((ix, requestEntry, trieEntry) : rest) = do
        change <- updateRequestChangeFromEntry ix requestEntry
        nextRoot <-
            replayOneUpdateTrieFact
                currentRoot
                ix
                change
                trieEntry
        go nextRoot rest

updateRequestChangeFromEntry
    :: Int -> UtxoEntry -> Either VerifyError UpdateRequestChange
updateRequestChangeFromEntry ix entry = do
    let field =
            "update.request_utxos["
                <> T.pack (show ix)
                <> "]"
    datum <- inlineDatumFromEntry field entry
    (datumTag, datumFields) <- parseConstr (field <> ".datum") datum
    case (datumTag, datumFields) of
        (0, [requestTerm]) ->
            parseUpdateRequestChange field requestTerm
        (0, requestFields@[_token, _owner, _key, _op, _fee, _submitted]) ->
            parseUpdateRequestChangeFields field requestFields
        _ ->
            Left
                $ TxBindingFailed
                    (field <> ".datum")
                    "expected request datum"

parseUpdateRequestChange
    :: T.Text -> CBOR.Term -> Either VerifyError UpdateRequestChange
parseUpdateRequestChange field requestTerm = do
    (requestTag, requestFields) <-
        parseConstr (field <> ".datum.request") requestTerm
    case (requestTag, requestFields) of
        (0, fields@[_token, _owner, _key, _op, _fee, _submitted]) ->
            parseUpdateRequestChangeFields field fields
        _ ->
            Left
                $ TxBindingFailed
                    (field <> ".datum.request")
                    "expected request fields"

parseUpdateRequestChangeFields
    :: T.Text -> [CBOR.Term] -> Either VerifyError UpdateRequestChange
parseUpdateRequestChangeFields field = \case
    [_token, _owner, keyTerm, operationTerm, _fee, _submitted] ->
        UpdateRequestChange
            <$> termBytes (field <> ".datum.request.key") keyTerm
            <*> parseUpdateOperation
                (field <> ".datum.request.operation")
                operationTerm
    _ ->
        Left
            $ TxBindingFailed
                (field <> ".datum.request")
                "expected request fields"

parseUpdateOperation
    :: T.Text -> CBOR.Term -> Either VerifyError UpdateOperation
parseUpdateOperation field operationTerm = do
    (operationTag, operationFields) <- parseConstr field operationTerm
    case (operationTag, operationFields) of
        (0, [newValue]) ->
            UpdateInsert
                <$> termBytes (field <> ".insert.value") newValue
        (1, [oldValue]) ->
            UpdateDelete
                <$> termBytes (field <> ".delete.old_value") oldValue
        (2, [oldValue, newValue]) ->
            UpdateReplace
                <$> termBytes (field <> ".update.old_value") oldValue
                <*> termBytes (field <> ".update.new_value") newValue
        _ ->
            Left
                $ TxBindingFailed
                    field
                    "expected insert, delete, or update operation"

termBytes :: T.Text -> CBOR.Term -> Either VerifyError BS.ByteString
termBytes field = \case
    CBOR.TBytes bytes -> Right bytes
    _ -> Left (TxBindingFailed field "expected bytes")

replayOneUpdateTrieFact
    :: BS.ByteString
    -> Int
    -> UpdateRequestChange
    -> TrieFact
    -> Either VerifyError BS.ByteString
replayOneUpdateTrieFact
    currentRoot
    ix
    UpdateRequestChange{urcKey, urcOperation}
    TrieFact{tfKey = Hex factKey, tfValue, tfMpfProof = Hex proofBytes} = do
        when (factKey /= urcKey)
            $ Left
            $ TxBindingFailed
                (factPath <> ".key")
                "does not match request key"
        case urcOperation of
            UpdateInsert newValue -> do
                expectNoFactValue
                checkOldRoot False Nothing
                computeNextRoot True (Just newValue)
            UpdateDelete oldValue -> do
                expectFactValue oldValue
                checkOldRoot True (Just oldValue)
                computeNextRoot False Nothing
            UpdateReplace oldValue newValue -> do
                expectFactValue oldValue
                checkOldRoot True (Just oldValue)
                computeNextRoot True (Just newValue)
      where
        factPath =
            "update.trie_facts["
                <> T.pack (show ix)
                <> "]"
        proofPath = factPath <> ".mpf_proof"

        expectNoFactValue =
            case tfValue of
                Nothing -> Right ()
                Just _ ->
                    Left
                        $ TxBindingFailed
                            (factPath <> ".value")
                            "must be null for insert"

        expectFactValue expected =
            case tfValue of
                Just (Hex actual) | actual == expected -> Right ()
                _ ->
                    Left
                        $ TxBindingFailed
                            (factPath <> ".value")
                            "does not match request old value"

        checkOldRoot including value = do
            oldRoot <- computeRoot including value
            when (oldRoot /= currentRoot)
                $ Left
                $ MpfReplayFailed proofPath "root mismatch"

        computeNextRoot =
            computeRoot

        computeRoot including value =
            case computeAikenProofRoot including urcKey value proofBytes of
                Just root -> Right root
                Nothing ->
                    Left
                        $ MpfReplayFailed proofPath "root mismatch"

verifyUpdateValidityUpperSlot
    :: UpdateFacts
    -> Either VerifyError ()
verifyUpdateValidityUpperSlot facts@UpdateFacts{..} = do
    processTime <- updateStateProcessTime facts
    processSlots <- processTimeSlotBound processTime
    requestSubmittedAt <- updateRequestSubmittedAt facts
    traverse_ validateSubmittedAt requestSubmittedAt
    if slot <= 0
        then
            Left
                $ TxBindingFailed
                    "update.validity_upper_slot"
                    "must be positive"
        else
            if slot <= snapshotSlot
                then
                    Left
                        $ TxBindingFailed
                            "update.validity_upper_slot"
                            "must be greater than the snapshot slot"
                else
                    if slot
                        > snapshotSlot
                            + processSlots
                            + updateValiditySlotBuffer
                        then
                            Left
                                $ TxBindingFailed
                                    "update.validity_upper_slot"
                                    "too far beyond the snapshot slot"
                        else Right ()
  where
    slot = ufValidityUpperSlot
    snapshotSlot = fromIntegral (cpSlot (vsChainPoint ufSnapshot))

updateValiditySlotBuffer :: Integer
updateValiditySlotBuffer = 600

validateSubmittedAt :: Integer -> Either VerifyError ()
validateSubmittedAt value
    | value >= 0 = Right ()
    | otherwise =
        Left
            $ TxBindingFailed
                "update.request_utxos[].datum.request.submitted_at"
                "missing non-negative submitted_at"

processTimeSlotBound :: Integer -> Either VerifyError Integer
processTimeSlotBound value =
    Right ((max 0 value + 999) `div` 1000)

updateStateProcessTime
    :: UpdateFacts
    -> Either VerifyError Integer
updateStateProcessTime UpdateFacts{..} =
    stateProcessTimeFromEntry
        "update.state_utxo"
        ufStateUtxo

updateRequestSubmittedAt
    :: UpdateFacts
    -> Either VerifyError [Integer]
updateRequestSubmittedAt UpdateFacts{..} =
    case ufRequestUtxos of
        [] ->
            Left
                $ TxBindingFailed
                    "update.request_utxos"
                    "must not be empty"
        entries ->
            traverse
                (uncurry requestSubmittedAtFromEntry)
                (zip [0 :: Int ..] entries)

stateProcessTimeFromEntry
    :: T.Text -> UtxoEntry -> Either VerifyError Integer
stateProcessTimeFromEntry field entry = do
    datum <- inlineDatumFromEntry field entry
    (datumTag, datumFields) <- parseConstr (field <> ".datum") datum
    case (datumTag, datumFields) of
        (1, [stateTerm]) -> parseStateProcessTime field stateTerm
        _ ->
            Left
                $ TxBindingFailed
                    (field <> ".datum")
                    "expected state datum"

parseStateProcessTime
    :: T.Text -> CBOR.Term -> Either VerifyError Integer
parseStateProcessTime field stateTerm = do
    (stateTag, stateFields) <-
        parseConstr (field <> ".datum.state") stateTerm
    case (stateTag, stateFields) of
        (0, [_owner, _root, _fee, processTime, _retract]) ->
            termInteger
                (field <> ".datum.state.process_time")
                processTime
        _ ->
            Left
                $ TxBindingFailed
                    (field <> ".datum.state.process_time")
                    "missing process time"

requestSubmittedAtFromEntry
    :: Int -> UtxoEntry -> Either VerifyError Integer
requestSubmittedAtFromEntry ix entry = do
    let field =
            "update.request_utxos["
                <> T.pack (show ix)
                <> "]"
    datum <- inlineDatumFromEntry field entry
    (datumTag, datumFields) <- parseConstr (field <> ".datum") datum
    case (datumTag, datumFields) of
        (0, [requestTerm]) ->
            parseRequestSubmittedAt field requestTerm
        (0, requestFields@[_token, _owner, _key, _op, _fee, _submitted]) ->
            parseRequestSubmittedAtFields field requestFields
        _ ->
            Left
                $ TxBindingFailed
                    (field <> ".datum")
                    "expected request datum"

parseRequestSubmittedAt
    :: T.Text -> CBOR.Term -> Either VerifyError Integer
parseRequestSubmittedAt field requestTerm = do
    (requestTag, requestFields) <-
        parseConstr (field <> ".datum.request") requestTerm
    case (requestTag, requestFields) of
        (0, fields@[_token, _owner, _key, _op, _fee, _submitted]) ->
            parseRequestSubmittedAtFields field fields
        _ ->
            Left
                $ TxBindingFailed
                    (field <> ".datum.request.submitted_at")
                    "missing non-negative submitted_at"

parseRequestSubmittedAtFields
    :: T.Text -> [CBOR.Term] -> Either VerifyError Integer
parseRequestSubmittedAtFields field = \case
    [_token, _owner, _key, _op, _fee, submitted] ->
        termInteger
            (field <> ".datum.request.submitted_at")
            submitted
    _ ->
        Left
            $ TxBindingFailed
                (field <> ".datum.request.submitted_at")
                "missing non-negative submitted_at"

termInteger :: T.Text -> CBOR.Term -> Either VerifyError Integer
termInteger field = \case
    CBOR.TInt value -> Right (toInteger value)
    CBOR.TInteger value -> Right value
    _ -> Left (TxBindingFailed field "expected integer")

inlineDatumFromEntry
    :: T.Text -> UtxoEntry -> Either VerifyError CBOR.Term
inlineDatumFromEntry field UtxoEntry{ueTxOutCbor} = do
    TxOutView{txOutInlineDatum} <-
        decodeTxOutView
            (field <> ".tx_out")
            (toClientHex ueTxOutCbor)
    case txOutInlineDatum of
        Just datum -> Right datum
        Nothing ->
            Left
                $ TxBindingFailed
                    (field <> ".tx_out.datum")
                    "inline datum missing"

parseConstr
    :: T.Text -> CBOR.Term -> Either VerifyError (Integer, [CBOR.Term])
parseConstr field = \case
    CBOR.TTagged tag fieldsTerm -> do
        constr <- constructorIndex tag
        fields <- parseList field fieldsTerm
        Right (constr, fields)
    _ -> Left (TxBindingFailed field "expected constructor data")
  where
    constructorIndex tag
        | tag >= 121 && tag <= 127 =
            Right (toInteger tag - 121)
        | tag >= 1280 && tag <= 1400 =
            Right (toInteger tag - 1280 + 7)
        | tag == 102 =
            Left
                $ TxBindingFailed
                    field
                    "generic constructor data unsupported"
        | otherwise =
            Left (TxBindingFailed field "unsupported constructor tag")

parseList
    :: T.Text -> CBOR.Term -> Either VerifyError [CBOR.Term]
parseList field = \case
    CBOR.TList xs -> Right xs
    CBOR.TListI xs -> Right xs
    _ -> Left (TxBindingFailed field "expected list data")

-- | Verify a facts-only retract response against an
-- externally-supplied trusted UTxO-CSMT root. Replays the
-- inclusion proofs for the named request UTxO, the cage state
-- UTxO, and every requester wallet UTxO. The validity slot
-- bounds and protocol parameters are unverified inputs and
-- propagate as-is through 'VerifiedRetractFacts'.
verifyRetractFacts
    :: TrustedRoot
    -> RetractFacts
    -> Either VerifyError VerifiedRetractFacts
verifyRetractFacts
    (TrustedRoot (Hex trustedBs))
    facts@RetractFacts{..} = do
        checkLength "retract.trusted_root" trustedBs
        let snapshotPath = "retract.snapshot.utxo_root"
            Hex snapshotBs = vsUtxoRoot rfSnapshot
        checkLength snapshotPath snapshotBs
        if snapshotBs == trustedBs
            then Right ()
            else Left (TrustedRootMismatch snapshotPath)
        replayUtxoEntry
            "retract.request_utxo"
            trustedBs
            rfRequestUtxo
        replayUtxoEntry
            "retract.state_utxo"
            trustedBs
            rfStateUtxo
        replayWalletUtxos trustedBs rfWalletUtxos
        Right (VerifiedRetractFacts facts)
      where
        checkLength field bs
            | BS.length bs == 32 = Right ()
            | otherwise =
                Left (WrongHexLength field 32 (BS.length bs))
        replayWalletUtxos root entries =
            mapM_
                ( \(ix, entry) ->
                    replayUtxoEntry
                        ( "retract.wallet_utxos["
                            <> T.pack (show (ix :: Int))
                            <> "]"
                        )
                        root
                        entry
                )
                (zip [0 ..] entries)

-- | Verify a facts-only reject response against an
-- externally-supplied trusted UTxO-CSMT root. Replays the
-- inclusion proofs for the cage state UTxO, every rejectable
-- request UTxO, and every requester wallet UTxO, then checks
-- that the server-derived Phase 3 validity bounds are
-- internally consistent (lower > snapshot slot, upper > lower,
-- upper within the bounded horizon). Per Q-S2-001 the reject
-- envelope carries no trie facts, so no MPF replay runs here.
verifyRejectFacts
    :: TrustedRoot
    -> RejectFacts
    -> Either VerifyError VerifiedRejectFacts
verifyRejectFacts
    (TrustedRoot (Hex trustedBs))
    facts@RejectFacts{..} = do
        checkLength "reject.trusted_root" trustedBs
        let snapshotPath = "reject.snapshot.utxo_root"
            Hex snapshotBs = vsUtxoRoot rfSnapshot
        checkLength snapshotPath snapshotBs
        if snapshotBs == trustedBs
            then Right ()
            else Left (TrustedRootMismatch snapshotPath)
        case rfRequestUtxos of
            [] ->
                Left
                    $ TxBindingFailed
                        "reject.request_utxos"
                        "must not be empty"
            _ -> Right ()
        replayUtxoEntry "reject.state_utxo" trustedBs rfStateUtxo
        replayRequestUtxos trustedBs rfRequestUtxos
        replayWalletUtxos trustedBs rfWalletUtxos
        checkRejectValiditySlots
            rfSnapshot
            rfValidityLowerSlot
            rfValidityUpperSlot
        Right (VerifiedRejectFacts facts)
      where
        checkLength field bs
            | BS.length bs == 32 = Right ()
            | otherwise =
                Left (WrongHexLength field 32 (BS.length bs))
        replayRequestUtxos root entries =
            mapM_
                ( \(ix, entry) ->
                    replayUtxoEntry
                        ( "reject.request_utxos["
                            <> T.pack (show (ix :: Int))
                            <> "]"
                        )
                        root
                        entry
                )
                (zip [0 ..] entries)
        replayWalletUtxos root entries =
            mapM_
                ( \(ix, entry) ->
                    replayUtxoEntry
                        ( "reject.wallet_utxos["
                            <> T.pack (show (ix :: Int))
                            <> "]"
                        )
                        root
                        entry
                )
                (zip [0 ..] entries)

-- | Internal consistency checks on the server-derived reject
-- validity slot bounds. The constants below match the shape
-- 'verifyUpdateFacts' uses: 60 s grace + 600 s TTL = 660
-- slot-seconds. If the server picks a tighter bound the
-- envelope still passes; if it picks a looser one the verifier
-- fails fast so a Phase 2-rejecting tx is never built.
checkRejectValiditySlots
    :: VerificationSnapshot
    -> Integer
    -> Integer
    -> Either VerifyError ()
checkRejectValiditySlots snap lower upper = do
    let snapshotSlot =
            fromIntegral (cpSlot (vsChainPoint snap))
    if lower > snapshotSlot
        then Right ()
        else
            Left
                $ TxBindingFailed
                    "reject.validity_lower_slot"
                    "must be greater than the snapshot slot"
    if upper > lower
        then Right ()
        else
            Left
                $ TxBindingFailed
                    "reject.validity_upper_slot"
                    "must be greater than the lower slot"
    if upper - snapshotSlot <= rejectValidityHorizonSlots
        then Right ()
        else
            Left
                $ TxBindingFailed
                    "reject.validity_upper_slot"
                    "too far beyond the snapshot slot"

-- | 60 s grace + 600 s TTL converted to slots (1 slot per
-- second, matching the legacy reject and update settings).
rejectValidityHorizonSlots :: Integer
rejectValidityHorizonSlots = 60 + 600

-- | Verify a facts-only end response against an externally-supplied
-- trusted UTxO-CSMT root and a client-owned cage configuration.
verifyEndFacts
    :: CageConfig
    -> TrustedRoot
    -> EndFacts
    -> Either VerifyError VerifiedEndFacts
verifyEndFacts
    cfg
    (TrustedRoot (Hex trustedBs))
    facts@EndFacts{..} = do
        checkLength "end.trusted_root" trustedBs
        let snapshotPath = "end.snapshot.utxo_root"
            Hex snapshotBs = vsUtxoRoot efSnapshot
        checkLength snapshotPath snapshotBs
        if snapshotBs == trustedBs
            then Right ()
            else Left (TrustedRootMismatch snapshotPath)
        replayUtxoEntry "end.state_utxo" trustedBs efStateUtxo
        replayWalletUtxos trustedBs efWalletUtxos
        rejectRequestEntries (uswEntries efRequestSet)
        verifyUtxoSetCompleteness
            "end.request_set"
            trustedBs
            (requestSetPrefixFromCfg cfg efToken)
            efRequestSet
        Right (VerifiedEndFacts facts)
      where
        checkLength field bs
            | BS.length bs == 32 = Right ()
            | otherwise =
                Left (WrongHexLength field 32 (BS.length bs))
        replayWalletUtxos root entries =
            mapM_
                ( \(ix, entry) ->
                    replayUtxoEntry
                        ( "end.wallet_utxos["
                            <> T.pack (show (ix :: Int))
                            <> "]"
                        )
                        root
                        entry
                )
                (zip [0 ..] entries)
        rejectRequestEntries [] = Right ()
        rejectRequestEntries (entry : _) =
            Left
                ( CompletenessExtraLeaf
                    "end.request_set.entries[0]"
                    (uerRef entry)
                )

-- | Verify a present fact response against an externally-supplied
-- trusted UTxO-CSMT root and the key from the request path.
verifyFactPresentFacts
    :: TrustedRoot
    -> FactPresentFacts
    -> Either VerifyError VerifiedFactPresentFacts
verifyFactPresentFacts
    (TrustedRoot (Hex trustedBs))
    facts@FactPresentFacts
        { fpfKey
        , fpfResponse = FactResponse{..}
        } = do
        verifyFactSnapshot "fact_present" trustedBs frSnapshot
        replayFactState "fact_present" trustedBs frFact
        trieRoot <-
            factWitnessTrieRoot "fact_present.fact" frFact
        replayTrieFact
            "fact_present.fact"
            trieRoot
            ClientWire.TrieFact
                { ClientWire.key = toClientHex fpfKey
                , ClientWire.value = Just (toClientHex frValue)
                , ClientWire.mpfProof =
                    toClientHex (fwMpfProof frFact)
                }
        Right (VerifiedFactPresentFacts facts)

-- | Verify an absent fact response against an externally-supplied
-- trusted UTxO-CSMT root and the key from the request path.
verifyFactAbsentFacts
    :: TrustedRoot
    -> FactAbsentFacts
    -> Either VerifyError VerifiedFactAbsentFacts
verifyFactAbsentFacts
    (TrustedRoot (Hex trustedBs))
    facts@FactAbsentFacts
        { fafKey
        , fafResponse = ProofResponse{..}
        } = do
        verifyFactSnapshot "fact_absent" trustedBs prSnapshot
        replayFactState "fact_absent" trustedBs prFact
        trieRoot <-
            factWitnessTrieRoot "fact_absent.fact" prFact
        replayTrieFact
            "fact_absent.fact"
            trieRoot
            ClientWire.TrieFact
                { ClientWire.key = toClientHex fafKey
                , ClientWire.value = Nothing
                , ClientWire.mpfProof =
                    toClientHex (fwMpfProof prFact)
                }
        Right (VerifiedFactAbsentFacts facts)

verifyFactSnapshot
    :: T.Text
    -> BS.ByteString
    -> VerificationSnapshot
    -> Either VerifyError ()
verifyFactSnapshot prefix trustedBs snap@VerificationSnapshot{..} = do
    checkTrustedLength (prefix <> ".trusted_root") trustedBs
    verifyVerificationSnapshot (toClientSnapshot snap)
    let snapshotPath = prefix <> ".snapshot.utxo_root"
        Hex snapshotBs = vsUtxoRoot
    if snapshotBs == trustedBs
        then Right ()
        else Left (TrustedRootMismatch snapshotPath)
  where
    checkTrustedLength field bs
        | BS.length bs == 32 = Right ()
        | otherwise =
            Left (WrongHexLength field 32 (BS.length bs))

toClientSnapshot
    :: VerificationSnapshot -> ClientSnapshot.VerificationSnapshot
toClientSnapshot VerificationSnapshot{..} =
    ClientSnapshot.VerificationSnapshot
        { ClientSnapshot.utxoRoot = toClientHex vsUtxoRoot
        , ClientSnapshot.chainpoint =
            let ChainPointJSON{..} = vsChainPoint
            in  ClientSnapshot.ChainPoint
                    { ClientSnapshot.slot = cpSlot
                    , ClientSnapshot.blockId = toClientHex cpBlockId
                    }
        }

replayFactState
    :: T.Text
    -> BS.ByteString
    -> FactWitness
    -> Either VerifyError ()
replayFactState prefix trustedBs FactWitness{fwState} =
    replayWitnessedUtxo
        (prefix <> ".fact.state.utxo")
        trustedBs
        (toClientWitnessedUtxo (wtsUtxo fwState))

-- | Decode the on-chain trie root from the inline datum of the
-- witnessed state UTxO carried by a 'FactWitness'. The root is
-- provable material in @fwState.utxo.tx_out@, not a server-side
-- projection, so it is decoded locally rather than trusted.
factWitnessTrieRoot
    :: T.Text -> FactWitness -> Either VerifyError BS.ByteString
factWitnessTrieRoot field FactWitness{fwState} =
    stateRootBytesFromWitness
        field
        (toClientWitnessedUtxo (wtsUtxo fwState))

toClientWitnessedUtxo :: WitnessedUtxo -> ClientWire.WitnessedUtxo
toClientWitnessedUtxo WitnessedUtxo{..} =
    ClientWire.WitnessedUtxo
        { ClientWire.txIn =
            toClientTxIn wuTxIn
        , ClientWire.txOut = toClientHex wuTxOut
        , ClientWire.utxoProof = toClientHex wuProof
        }

toClientWitnessedUtxoEntry :: UtxoEntry -> ClientWire.WitnessedUtxo
toClientWitnessedUtxoEntry UtxoEntry{..} =
    ClientWire.WitnessedUtxo
        { ClientWire.txIn =
            toClientUtxoRef ueRef
        , ClientWire.txOut = toClientHex ueTxOutCbor
        , ClientWire.utxoProof = toClientHex ueInclusionProof
        }

toClientTxIn :: TxInJSON -> ClientWire.TxIn
toClientTxIn TxInJSON{..} =
    ClientWire.TxIn
        { ClientWire.txId = toClientHex tjTxId
        , ClientWire.txIx = tjTxIx
        }

toClientUtxoRef :: UtxoRef -> ClientWire.TxIn
toClientUtxoRef UtxoRef{..} =
    ClientWire.TxIn
        { ClientWire.txId = toClientHex urTxId
        , ClientWire.txIx = urTxIx
        }

toClientHex :: Hex -> ClientSnapshot.Hex
toClientHex (Hex bs) =
    ClientSnapshot.Hex (T.decodeUtf8 (Base16.encode bs))
