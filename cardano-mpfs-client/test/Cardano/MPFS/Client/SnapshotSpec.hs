module Cardano.MPFS.Client.SnapshotSpec (spec) where

import Cardano.MPFS.Client
    ( ChainPoint (..)
    , Hex (..)
    , VerificationSnapshot (..)
    , VerifyError (..)
    , statusSnapshot
    , verifyVerificationSnapshot
    )
import Data.Aeson qualified as Aeson
import Data.Aeson.Types qualified as Aeson
import Data.ByteString.Lazy.Char8 qualified as BL
import Data.Maybe (fromMaybe)
import Data.Text qualified as T
import Test.Hspec (Spec, describe, it, shouldBe, shouldSatisfy)

spec :: Spec
spec = do
    describe "VerificationSnapshot JSON" $ do
        it "round-trips via aeson" $ do
            let snap = mkSnapshot root32 block32 42
                encoded = Aeson.encode snap
            Aeson.eitherDecode encoded `shouldBe` Right snap

        it "parses a slice-2-shaped response" $ do
            let body =
                    BL.concat
                        [ "{\"anything\":1,"
                        , "\"utxo_root\":\""
                        , BL.pack (T.unpack root32)
                        , "\",\"chainpoint\":{\"slot\":100,"
                        , "\"block_id\":\""
                        , BL.pack (T.unpack block32)
                        , "\"}}"
                        ]
            Aeson.eitherDecode body
                `shouldBe` Right (mkSnapshot root32 block32 100)

    describe "statusSnapshot extractor" $ do
        it "returns Nothing when utxo_root is absent"
            $ parse statusSnapshot (statusJSON Nothing)
            `shouldBe` Right Nothing

        it "returns Nothing when utxo_root is null"
            $ parse statusSnapshot (statusJSON (Just "null"))
            `shouldBe` Right Nothing

        it "extracts the checkpoint as the snapshot chain point"
            $ parse statusSnapshot (statusJSON (Just (quote root32)))
            `shouldBe` Right (Just (mkSnapshot root32 block32 77))

    describe "verifyVerificationSnapshot" $ do
        it "accepts a well-formed snapshot"
            $ verifyVerificationSnapshot (mkSnapshot root32 block32 1)
            `shouldBe` Right ()

        it "rejects a root that is not 32 bytes"
            $ verifyVerificationSnapshot (mkSnapshot rootShort block32 1)
            `shouldSatisfy` isWrongLength "utxo_root"

        it "rejects a malformed-hex root"
            $ verifyVerificationSnapshot (mkSnapshot rootBad block32 1)
            `shouldSatisfy` isMalformed "utxo_root"

        it "rejects an empty chainpoint block id"
            $ verifyVerificationSnapshot (mkSnapshot root32 "" 1)
            `shouldSatisfy` (== Left EmptyBlockId)

mkSnapshot :: T.Text -> T.Text -> Word -> VerificationSnapshot
mkSnapshot r b s =
    VerificationSnapshot
        { utxoRoot = Hex r
        , chainpoint =
            ChainPoint{slot = fromIntegral s, blockId = Hex b}
        }

root32, block32, rootShort, rootBad :: T.Text
root32 = T.replicate 64 "a"
block32 = T.replicate 64 "b"
rootShort = T.replicate 10 "a"
rootBad = "zzzz"

statusJSON :: Maybe BL.ByteString -> BL.ByteString
statusJSON mRoot =
    BL.concat
        [ "{\"tip_slot\":100,"
        , "\"tip_block_id\":\""
        , BL.pack (T.unpack block32)
        , "\",\"checkpoint_slot\":77,"
        , "\"checkpoint_block_id\":\""
        , BL.pack (T.unpack block32)
        , "\",\"utxo_root\":"
        , fromMaybe "null" mRoot
        , "}"
        ]

quote :: T.Text -> BL.ByteString
quote t = BL.concat ["\"", BL.pack (T.unpack t), "\""]

parse
    :: (Aeson.Value -> Aeson.Parser a)
    -> BL.ByteString
    -> Either String a
parse p bs = do
    v <- Aeson.eitherDecode bs
    case Aeson.parse p v of
        Aeson.Error e -> Left e
        Aeson.Success a -> Right a

isWrongLength :: T.Text -> Either VerifyError () -> Bool
isWrongLength fld (Left (WrongHexLength f _ _)) = f == fld
isWrongLength _ _ = False

isMalformed :: T.Text -> Either VerifyError () -> Bool
isMalformed fld (Left (MalformedHex f _)) = f == fld
isMalformed _ _ = False
