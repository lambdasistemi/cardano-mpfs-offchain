{-# LANGUAGE DataKinds #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}

-- |
-- Module      : Main
-- Description : Generate CBOR transaction test vectors
-- License     : Apache-2.0
--
-- Generates JSON test vectors containing CBOR-serialized
-- cage transactions for each operation type. Used by
-- cardano-mpfs-browser to test its CBOR decoder.
--
-- Each vector includes the operation name, a description,
-- and the hex-encoded CBOR of an unsigned transaction.
module Main (main) where

import Data.Aeson ((.=))
import Data.Aeson qualified as Aeson
import Data.Aeson.Encode.Pretty (encodePretty)
import Data.ByteString qualified as BS
import Data.ByteString.Base16 qualified as B16
import Data.ByteString.Lazy.Char8 qualified as BL8
import Data.ByteString.Short qualified as SBS
import Data.Maybe (fromJust)
import Data.Text (Text)
import Data.Text.Encoding qualified as TE

import Cardano.Crypto.Hash
    ( Blake2b_256
    , hashFromStringAsHex
    )
import Cardano.Ledger.Api.Tx (Tx)
import Cardano.Ledger.BaseTypes (TxIx (..))
import Cardano.Ledger.Binary (natVersion, serialize')
import Cardano.Ledger.Hashes (unsafeMakeSafeHash)
import Cardano.Ledger.Keys (KeyHash (..), KeyRole (..))
import Cardano.Ledger.Mary.Value (AssetName (..))
import Cardano.Ledger.TxIn (TxId (..), TxIn (..))
import Cardano.MPFS.Core.Types
    ( Coin (..)
    , ConwayEra
    , Operation (..)
    , Request (..)
    , Root (..)
    , TokenId (..)
    , TokenState (..)
    )
import Cardano.MPFS.Indexer.TxFixtures
    ( mkBootRequestTx
    , mkBootTx
    , mkBurnTx
    , mkPlainTx
    , mkRequestTx
    , mkRetractTx
    , mkUpdateTx
    )

-- ---------------------------------------------------------
-- Serialization
-- ---------------------------------------------------------

-- | Serialize a 'Tx ConwayEra' to hex-encoded CBOR.
serializeTxHex :: Tx ConwayEra -> Text
serializeTxHex =
    TE.decodeUtf8
        . B16.encode
        . serialize' (natVersion @11)

-- ---------------------------------------------------------
-- Test data
-- ---------------------------------------------------------

-- | A fixed test TxId (32 bytes of 0x11).
testTxId :: TxId
testTxId =
    TxId
        $ unsafeMakeSafeHash
        $ fromJust
        $ hashFromStringAsHex @Blake2b_256
            "1111111111111111111111111111111111\
            \111111111111111111111111111111"

-- | A second TxId (32 bytes of 0x22).
testTxId2 :: TxId
testTxId2 =
    TxId
        $ unsafeMakeSafeHash
        $ fromJust
        $ hashFromStringAsHex @Blake2b_256
            "2222222222222222222222222222222222\
            \222222222222222222222222222222"

-- | Test TxIn from 'testTxId'.
testTxIn :: TxIn
testTxIn = TxIn testTxId (TxIx 0)

-- | Second TxIn for multi-input transactions.
testTxIn2 :: TxIn
testTxIn2 = TxIn testTxId2 (TxIx 0)

-- | A third TxId (32 bytes of 0x33).
testTxId3 :: TxId
testTxId3 =
    TxId
        $ unsafeMakeSafeHash
        $ fromJust
        $ hashFromStringAsHex @Blake2b_256
            "3333333333333333333333333333333333\
            \333333333333333333333333333333"

-- | Third TxIn for multi-request updates.
testTxIn3 :: TxIn
testTxIn3 = TxIn testTxId3 (TxIx 0)

-- | Test token ID.
testTokenId :: TokenId
testTokenId =
    TokenId
        $ AssetName
        $ SBS.toShort
        $ BS.replicate 32 0xaa

-- | Test payment key hash (28 bytes of 0xcc).
testKeyHash :: KeyHash 'Payment
testKeyHash =
    KeyHash
        $ fromJust
        $ hashFromStringAsHex
            "cccccccccccccccccccccccccccc\
            \cccccccccccccccccccccccccccc"

-- | Test token state.
testTokenState :: TokenState
testTokenState =
    TokenState
        { owner = testKeyHash
        , root = Root (BS.replicate 32 0xbb)
        , maxFee = Coin 2_000_000
        , processTime = 300_000
        , retractTime = 600_000
        }

-- | Test request (insert).
testInsertRequest :: Request
testInsertRequest =
    Request
        { requestToken = testTokenId
        , requestOwner = testKeyHash
        , requestKey = BS.pack [0x01, 0x02, 0x03]
        , requestValue = Insert (BS.pack [0x04, 0x05])
        , requestFee = Coin 1_000_000
        , requestSubmittedAt = 1_700_000_000_000
        }

-- | Test request (delete).
testDeleteRequest :: Request
testDeleteRequest =
    Request
        { requestToken = testTokenId
        , requestOwner = testKeyHash
        , requestKey = BS.pack [0x01, 0x02, 0x03]
        , requestValue = Delete (BS.pack [0x04, 0x05])
        , requestFee = Coin 1_000_000
        , requestSubmittedAt = 1_700_000_000_000
        }

-- | Test request (update/modify value).
testUpdateRequest :: Request
testUpdateRequest =
    Request
        { requestToken = testTokenId
        , requestOwner = testKeyHash
        , requestKey = BS.pack [0x01, 0x02, 0x03]
        , requestValue =
            Update
                (BS.pack [0x04, 0x05])
                (BS.pack [0x06, 0x07])
        , requestFee = Coin 1_000_000
        , requestSubmittedAt = 1_700_000_000_000
        }

-- ---------------------------------------------------------
-- Vectors
-- ---------------------------------------------------------

-- | A single test vector.
data TxVector = TxVector
    { tvOp :: Text
    , tvDesc :: Text
    , tvTx :: Tx ConwayEra
    }

-- | All transaction vectors.
allVectors :: [TxVector]
allVectors =
    [ TxVector
        "boot"
        "Mint +1 token with StateDatum output"
        (mkBootTx testTokenId testTokenState testTxIn)
    , TxVector
        "burn"
        "Mint -1 token (end lifecycle)"
        (mkBurnTx testTokenId testTxIn)
    , TxVector
        "request-insert"
        "Submit insert request with RequestDatum"
        (mkRequestTx testInsertRequest testTxIn)
    , TxVector
        "request-delete"
        "Submit delete request with RequestDatum"
        (mkRequestTx testDeleteRequest testTxIn)
    , TxVector
        "request-update"
        "Submit update (modify) request with RequestDatum"
        (mkRequestTx testUpdateRequest testTxIn)
    , TxVector
        "update"
        "Process one request: spend state + 1 request input"
        ( mkUpdateTx
            testTokenId
            testTokenState
            (Root (BS.replicate 32 0xdd))
            [testTxIn2]
            testTxIn
        )
    , TxVector
        "update-multi"
        "Process multiple requests: state + 2 request inputs"
        ( mkUpdateTx
            testTokenId
            testTokenState
            (Root (BS.replicate 32 0xdd))
            [testTxIn2, testTxIn3]
            testTxIn
        )
    , TxVector
        "retract"
        "Retract a pending request"
        (mkRetractTx testTxIn testTxIn2)
    , TxVector
        "plain"
        "Plain transaction with no cage content"
        (mkPlainTx testTxIn)
    , TxVector
        "boot-request"
        "Boot + insert request in single transaction"
        ( mkBootRequestTx
            testTokenId
            testTokenState
            testInsertRequest
            testTxIn
        )
    ]

-- | Render a vector as JSON.
vectorToJson :: TxVector -> Aeson.Value
vectorToJson TxVector{..} =
    Aeson.object
        [ "operation" .= tvOp
        , "description" .= tvDesc
        , "cborHex" .= serializeTxHex tvTx
        ]

-- ---------------------------------------------------------
-- Main
-- ---------------------------------------------------------

main :: IO ()
main = do
    let vectors =
            Aeson.object
                ["vectors" .= map vectorToJson allVectors]
    BL8.putStrLn $ encodePretty vectors
