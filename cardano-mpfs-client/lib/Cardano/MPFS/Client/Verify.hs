-- |
-- Module      : Cardano.MPFS.Client.Verify
-- Description : Offline verifiers for MPFS proof-bearing responses.
--
-- This slice ships the snapshot-level verifier. Verifiers for
-- 'WitnessedUtxo', 'FactWitness', and 'UnsignedTxWitnessBundle' are
-- added by later slices alongside the server-side response types they
-- consume.
module Cardano.MPFS.Client.Verify
    ( VerifyError (..)
    , verifyVerificationSnapshot
    ) where

import Cardano.MPFS.Client.Snapshot
    ( ChainPoint (..)
    , Hex (..)
    , VerificationSnapshot (..)
    )
import Data.ByteString qualified as BS
import Data.ByteString.Base16 qualified as Base16
import Data.Text (Text)
import Data.Text.Encoding qualified as T

-- | Structured error returned when a proof-bearing bundle fails
-- verification. Each case is tagged so the CLI and future verifiers can
-- render consistent pass/fail messages.
data VerifyError
    = -- | Field name and the offending value that failed hex decoding.
      MalformedHex Text Text
    | -- | Field name, expected byte length, actual byte length.
      WrongHexLength Text Int Int
    | EmptyBlockId
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
