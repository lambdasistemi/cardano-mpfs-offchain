-- |
-- Module      : Cardano.MPFS.Client.Snapshot
-- Description : Verification snapshot parsing for proof-bearing responses.
--
-- Every proof-bearing response served by cardano-mpfs-offchain carries a
-- 'VerificationSnapshot' that identifies the indexed state against which
-- its proofs are valid. This module defines the JSON contract for that
-- snapshot as consumed by the verification client.
module Cardano.MPFS.Client.Snapshot
    ( VerificationSnapshot (..)
    , ChainPoint (..)
    , Hex (..)
    , statusSnapshot
    ) where

import Data.Aeson
    ( FromJSON (..)
    , ToJSON (..)
    , Value (Null)
    , object
    , withObject
    , (.:)
    , (.:?)
    , (.=)
    )
import Data.Aeson.Types (Parser)
import Data.Text (Text)
import Data.Word (Word64)

-- | A hex-encoded byte string as carried by the MPFS API. The wrapper
-- preserves the on-wire representation; verifiers are responsible for
-- decoding when they need the raw bytes.
newtype Hex = Hex {unHex :: Text}
    deriving stock (Eq, Show)

instance FromJSON Hex where
    parseJSON = fmap Hex . parseJSON

instance ToJSON Hex where
    toJSON = toJSON . unHex

-- | Indexed chain point: the slot and block id the snapshot was
-- materialised at.
data ChainPoint = ChainPoint
    { slot :: Word64
    , blockId :: Hex
    }
    deriving stock (Eq, Show)

instance FromJSON ChainPoint where
    parseJSON = withObject "ChainPoint" $ \o ->
        ChainPoint <$> o .: "slot" <*> o .: "block_id"

instance ToJSON ChainPoint where
    toJSON ChainPoint{..} =
        object ["slot" .= slot, "block_id" .= blockId]

-- | The snapshot identifier baked into every proof-bearing response.
--
-- The 'utxo_root' is the UTxO-CSMT root the bundled UTxO proofs verify
-- against; the 'chainpoint' is the indexed chain point at which that
-- root was observed.
data VerificationSnapshot = VerificationSnapshot
    { utxoRoot :: Hex
    , chainpoint :: ChainPoint
    }
    deriving stock (Eq, Show)

instance FromJSON VerificationSnapshot where
    parseJSON = withObject "VerificationSnapshot" $ \o ->
        VerificationSnapshot <$> o .: "utxo_root" <*> o .: "chainpoint"

instance ToJSON VerificationSnapshot where
    toJSON VerificationSnapshot{..} =
        object ["utxo_root" .= utxoRoot, "chainpoint" .= chainpoint]

-- | Extract a snapshot from a @GET /status@ response.
--
-- @/status@ predates the proof-bearing response shape and stores the
-- snapshot fields flattened at the top level: @utxo_root@ is optional
-- (absent while the CSMT view is still warming up), and the chain
-- point lives under @tip_slot@/@tip_block_id@ with the snapshot
-- actually taken at @checkpoint_slot@/@checkpoint_block_id@.
statusSnapshot :: Value -> Parser (Maybe VerificationSnapshot)
statusSnapshot = withObject "StatusResponse" $ \o -> do
    mRoot <- o .:? "utxo_root"
    case mRoot of
        Nothing -> pure Nothing
        Just Null -> pure Nothing
        Just rootVal -> do
            root <- parseJSON rootVal
            cp <-
                ChainPoint
                    <$> o .: "checkpoint_slot"
                    <*> o .: "checkpoint_block_id"
            pure $ Just (VerificationSnapshot root cp)
