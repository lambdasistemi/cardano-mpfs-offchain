-- |
-- Module      : Cardano.MPFS.Client.Verify
-- Description : Offline verifiers for MPFS proof-bearing responses.
--
-- Snapshot well-formedness plus structural traversers for every
-- per-endpoint response envelope. Each traverser walks the named
-- 'WitnessedUtxo' roles its proof carries, confirms the inline hex
-- fields decode, and invokes 'verifyVerificationSnapshot' on the
-- bundled snapshot. Semantic checks that require decoding a
-- @TxOut@ CBOR or replaying a CSMT\/MPF proof arrive in later
-- slices.
module Cardano.MPFS.Client.Verify
    ( VerifyError (..)
    , verifyVerificationSnapshot

      -- * Per-endpoint verifiers
    , verifyBootTxResponse
    , verifyRequestTxResponse
    , verifyRetractTxResponse
    , verifyRejectTxResponse
    , verifyEndTxResponse
    , verifyUpdateTxResponse
    ) where

import Data.ByteString qualified as BS
import Data.ByteString.Base16 qualified as Base16
import Data.Text (Text)
import Data.Text qualified as T
import Data.Text.Encoding qualified as T

import Cardano.MPFS.Client.Bundle
    ( BootProof (..)
    , BootTxResponse (..)
    , EndProof (..)
    , EndTxResponse (..)
    , RejectProof (..)
    , RejectTxResponse (..)
    , RequestProof (..)
    , RequestTxResponse (..)
    , RetractProof (..)
    , RetractTxResponse (..)
    , TrieFact (..)
    , TxIn (..)
    , UpdateProof (..)
    , UpdateTxResponse (..)
    , WitnessedUtxo (..)
    )
import Cardano.MPFS.Client.Snapshot
    ( ChainPoint (..)
    , Hex (..)
    , VerificationSnapshot (..)
    )

-- | Structured error returned when a proof-bearing bundle fails
-- verification. The @field@ text of each case is a dotted path
-- rooted at the endpoint name (e.g. @"boot.funding[0].utxo_proof"@)
-- so the CLI can render which named role of which endpoint
-- tripped the check.
data VerifyError
    = -- | Field path and the offending value that failed hex decoding.
      MalformedHex Text Text
    | -- | Field path, expected byte length, actual byte length.
      WrongHexLength Text Int Int
    | -- | The snapshot's chainpoint block id decoded to zero bytes.
      EmptyBlockId
    | -- | The top-level transaction CBOR failed to hex-decode.
      MalformedTxCbor Text
    deriving stock (Eq, Show)

-- | Structural check for the snapshot that every proof-bearing response
-- must carry. Confirms the @utxo_root@ decodes as a 32-byte hash and
-- the chain-point block id decodes as a non-empty hash. Does not
-- contact any external service.
verifyVerificationSnapshot
    :: VerificationSnapshot -> Either VerifyError ()
verifyVerificationSnapshot VerificationSnapshot{..} = do
    checkHash32 "utxo_root" utxoRoot
    let ChainPoint{blockId} = chainpoint
    checkNonEmptyHash "chainpoint.block_id" blockId

-- | Verify the structural well-formedness of a @POST \/tx\/boot@
-- response: the unsigned tx CBOR decodes, the snapshot is
-- well-formed, and every funding witness has decodable
-- @tx_in@\/@tx_out@\/@utxo_proof@ bytes.
verifyBootTxResponse :: BootTxResponse -> Either VerifyError ()
verifyBootTxResponse (BootTxResponse t s (BootProof fs)) = do
    checkTxCbor "boot.tx" t
    verifyVerificationSnapshot s
    checkFunding "boot" fs

-- | Verify a @POST \/tx\/request\/{insert,delete,update}@ response.
verifyRequestTxResponse :: RequestTxResponse -> Either VerifyError ()
verifyRequestTxResponse (RequestTxResponse t s (RequestProof fs)) = do
    checkTxCbor "request.tx" t
    verifyVerificationSnapshot s
    checkFunding "request" fs

-- | Verify a @POST \/tx\/retract@ response.
verifyRetractTxResponse :: RetractTxResponse -> Either VerifyError ()
verifyRetractTxResponse
    (RetractTxResponse t s (RetractProof ri sr fs)) = do
        checkTxCbor "retract.tx" t
        verifyVerificationSnapshot s
        checkWitnessedUtxo "retract.request_in" ri
        checkWitnessedUtxo "retract.state_ref" sr
        checkFunding "retract" fs

-- | Verify a @POST \/tx\/reject@ response.
verifyRejectTxResponse :: RejectTxResponse -> Either VerifyError ()
verifyRejectTxResponse
    (RejectTxResponse t s (RejectProof st ris fs)) = do
        checkTxCbor "reject.tx" t
        verifyVerificationSnapshot s
        checkWitnessedUtxo "reject.state" st
        checkWitnessedUtxos "reject.request_ins" ris
        checkFunding "reject" fs

-- | Verify a @POST \/tx\/end@ response.
verifyEndTxResponse :: EndTxResponse -> Either VerifyError ()
verifyEndTxResponse (EndTxResponse t s (EndProof st fs)) = do
    checkTxCbor "end.tx" t
    verifyVerificationSnapshot s
    checkWitnessedUtxo "end.state" st
    checkFunding "end" fs

-- | Verify a @POST \/tx\/update@ response.
verifyUpdateTxResponse :: UpdateTxResponse -> Either VerifyError ()
verifyUpdateTxResponse
    (UpdateTxResponse t s (UpdateProof st rs fs tr tread)) = do
        checkTxCbor "update.tx" t
        verifyVerificationSnapshot s
        checkWitnessedUtxo "update.state" st
        checkWitnessedUtxos "update.requests" rs
        checkFunding "update" fs
        checkHash32 "update.trie_root" tr
        checkTrieFacts "update.trie_read" tread

checkFunding :: Text -> [WitnessedUtxo] -> Either VerifyError ()
checkFunding endpoint =
    checkWitnessedUtxos (endpoint <> ".funding")

checkWitnessedUtxos
    :: Text -> [WitnessedUtxo] -> Either VerifyError ()
checkWitnessedUtxos prefix =
    traverseIndexed prefix checkWitnessedUtxo

checkWitnessedUtxo :: Text -> WitnessedUtxo -> Either VerifyError ()
checkWitnessedUtxo prefix WitnessedUtxo{..} = do
    checkTxIn (prefix <> ".tx_in") txIn
    checkNonEmpty (prefix <> ".tx_out") txOut
    checkNonEmpty (prefix <> ".utxo_proof") utxoProof

checkTxIn :: Text -> TxIn -> Either VerifyError ()
checkTxIn prefix TxIn{..} =
    checkHash32 (prefix <> ".tx_id") txId

checkTrieFacts :: Text -> [TrieFact] -> Either VerifyError ()
checkTrieFacts prefix =
    traverseIndexed prefix checkTrieFact

checkTrieFact :: Text -> TrieFact -> Either VerifyError ()
checkTrieFact prefix TrieFact{..} = do
    checkNonEmpty (prefix <> ".key") key
    case value of
        Nothing -> Right ()
        Just v -> checkNonEmpty (prefix <> ".value") v
    checkNonEmpty (prefix <> ".mpf_proof") mpfProof

traverseIndexed
    :: Text
    -> (Text -> a -> Either VerifyError ())
    -> [a]
    -> Either VerifyError ()
traverseIndexed prefix f = go (0 :: Int)
  where
    go _ [] = Right ()
    go i (x : xs) = do
        f (prefix <> "[" <> T.pack (show i) <> "]") x
        go (i + 1) xs

checkTxCbor :: Text -> Hex -> Either VerifyError ()
checkTxCbor field (Hex txt) =
    case Base16.decode (T.encodeUtf8 txt) of
        Right bs
            | BS.null bs -> Left (MalformedTxCbor field)
            | otherwise -> Right ()
        Left _ -> Left (MalformedTxCbor field)

checkNonEmpty :: Text -> Hex -> Either VerifyError ()
checkNonEmpty field h = do
    bs <- decodeHex field h
    if BS.null bs
        then Left (WrongHexLength field 1 0)
        else Right ()

checkHash32 :: Text -> Hex -> Either VerifyError ()
checkHash32 field h = do
    bs <- decodeHex field h
    let got = BS.length bs
    if got == 32
        then Right ()
        else Left (WrongHexLength field 32 got)

checkNonEmptyHash :: Text -> Hex -> Either VerifyError ()
checkNonEmptyHash field h = do
    bs <- decodeHex field h
    if BS.null bs then Left EmptyBlockId else Right ()

decodeHex :: Text -> Hex -> Either VerifyError BS.ByteString
decodeHex field (Hex txt) =
    case Base16.decode (T.encodeUtf8 txt) of
        Right bs -> Right bs
        Left _ -> Left (MalformedHex field txt)
