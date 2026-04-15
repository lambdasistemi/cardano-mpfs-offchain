{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS_GHC -Wno-orphans #-}

module Cardano.MPFS.OnChainSpec (spec) where

import Cardano.MPFS.Core.Blueprint
    ( Blueprint (..)
    , extractScriptHash
    , loadBlueprint
    , validateData
    )
import Cardano.MPFS.Core.OnChain
    ( CageDatum (..)
    , Migration (..)
    , Mint (..)
    , MintRedeemer (..)
    , Neighbor (..)
    , OnChainOperation (..)
    , OnChainRequest (..)
    , OnChainRoot (..)
    , OnChainTokenId (..)
    , OnChainTokenState (..)
    , OnChainTxOutRef (..)
    , ProofStep (..)
    , RequestAction (..)
    , UpdateRedeemer (..)
    , cageScriptHash
    , deriveAssetName
    )
import Data.ByteString (ByteString)
import Data.ByteString qualified as BS
import Data.Map.Strict qualified as Map
import Data.Text (Text)
import Data.Text qualified as T
import PlutusCore.Data (Data (..))
import PlutusTx.Builtins.Internal
    ( BuiltinByteString (..)
    , BuiltinData (..)
    )
import PlutusTx.IsData.Class
    ( FromData (..)
    , ToData (..)
    )
import System.Environment (lookupEnv)
import Test.Hspec
    ( Spec
    , describe
    , it
    , runIO
    , shouldBe
    )
import Test.QuickCheck
    ( Arbitrary (..)
    , Gen
    , Property
    , elements
    , listOf
    , oneof
    , property
    , vectorOf
    , (===)
    )

-- ---------------------------------------------------------
-- Helpers
-- ---------------------------------------------------------

toData' :: (ToData a) => a -> Data
toData' x =
    let BuiltinData d = toBuiltinData x in d

fromData' :: (FromData a) => Data -> Maybe a
fromData' d = fromBuiltinData (BuiltinData d)

roundtrip
    :: (ToData a, FromData a, Eq a, Show a)
    => a
    -> Property
roundtrip x = fromData' (toData' x) === Just x

toHex :: ByteString -> Text
toHex =
    T.pack
        . concatMap
            ( \w ->
                let (hi, lo) = w `divMod` 16
                in  [hexChar hi, hexChar lo]
            )
        . BS.unpack
  where
    hexChar n
        | n < 10 = toEnum (fromIntegral n + 48)
        | otherwise =
            toEnum (fromIntegral n + 87)

-- ---------------------------------------------------------
-- Arbitrary instances
-- ---------------------------------------------------------

genBS :: Int -> Gen ByteString
genBS n = BS.pack <$> vectorOf n arbitrary

genBBS :: Int -> Gen BuiltinByteString
genBBS n = BuiltinByteString <$> genBS n

instance Arbitrary OnChainTokenId where
    arbitrary =
        OnChainTokenId <$> genBBS 32

instance Arbitrary OnChainOperation where
    arbitrary =
        oneof
            [ OpInsert <$> genBS 32
            , OpDelete <$> genBS 32
            , OpUpdate <$> genBS 32 <*> genBS 32
            ]

instance Arbitrary OnChainRoot where
    arbitrary = OnChainRoot <$> genBS 32

instance Arbitrary OnChainTxOutRef where
    arbitrary =
        OnChainTxOutRef
            <$> genBBS 32
            <*> ( fromIntegral
                    <$> (arbitrary :: Gen Int)
                )

instance Arbitrary OnChainRequest where
    arbitrary =
        OnChainRequest
            <$> arbitrary
            <*> genBBS 28
            <*> genBS 32
            <*> arbitrary
            <*> ( fromIntegral
                    <$> (arbitrary :: Gen Int)
                )
            <*> ( fromIntegral
                    <$> (arbitrary :: Gen Int)
                )

instance Arbitrary OnChainTokenState where
    arbitrary =
        OnChainTokenState
            <$> genBBS 28
            <*> arbitrary
            <*> ( fromIntegral
                    <$> (arbitrary :: Gen Int)
                )
            <*> ( fromIntegral
                    <$> (arbitrary :: Gen Int)
                )
            <*> ( fromIntegral
                    <$> (arbitrary :: Gen Int)
                )

instance Arbitrary CageDatum where
    arbitrary =
        oneof
            [ RequestDatum <$> arbitrary
            , StateDatum <$> arbitrary
            ]

instance Arbitrary Mint where
    arbitrary = Mint <$> arbitrary

instance Arbitrary Migration where
    arbitrary =
        Migration
            <$> genBBS 28
            <*> arbitrary

instance Arbitrary MintRedeemer where
    arbitrary =
        oneof
            [ Minting <$> arbitrary
            , Migrating <$> arbitrary
            , pure Burning
            ]

instance Arbitrary Neighbor where
    arbitrary =
        Neighbor
            <$> elements [0 .. 15]
            <*> genBS 4
            <*> genBS 32

instance Arbitrary ProofStep where
    arbitrary =
        oneof
            [ Branch
                <$> elements [0 .. 30]
                <*> genBS 64
            , Fork
                <$> elements [0 .. 30]
                <*> arbitrary
            , Leaf
                <$> elements [0 .. 30]
                <*> genBS 32
                <*> genBS 32
            ]

instance Arbitrary RequestAction where
    arbitrary =
        oneof
            [ UpdateAction <$> listOf arbitrary
            , pure Rejected
            ]

instance Arbitrary UpdateRedeemer where
    arbitrary =
        oneof
            [ pure End
            , Contribute <$> arbitrary
            , Modify
                <$> listOf arbitrary
            , Retract <$> arbitrary
            ]

-- ---------------------------------------------------------
-- Spec
-- ---------------------------------------------------------

spec :: Spec
spec = do
    describe "PlutusData roundtrip" $ do
        it "OnChainTokenId" $ property $ \x ->
            roundtrip (x :: OnChainTokenId)
        it "OnChainOperation" $ property $ \x ->
            roundtrip (x :: OnChainOperation)
        it "OnChainRoot" $ property $ \x ->
            roundtrip (x :: OnChainRoot)
        it "OnChainTxOutRef" $ property $ \x ->
            roundtrip (x :: OnChainTxOutRef)
        it "OnChainRequest" $ property $ \x ->
            roundtrip (x :: OnChainRequest)
        it "OnChainTokenState" $ property $ \x ->
            roundtrip (x :: OnChainTokenState)
        it "CageDatum" $ property $ \x ->
            roundtrip (x :: CageDatum)
        it "Mint" $ property $ \x ->
            roundtrip (x :: Mint)
        it "Migration" $ property $ \x ->
            roundtrip (x :: Migration)
        it "MintRedeemer" $ property $ \x ->
            roundtrip (x :: MintRedeemer)
        it "Neighbor" $ property $ \x ->
            roundtrip (x :: Neighbor)
        it "ProofStep" $ property $ \x ->
            roundtrip (x :: ProofStep)
        it "UpdateRedeemer" $ property $ \x ->
            roundtrip (x :: UpdateRedeemer)

    describe "Known value encoding" $ do
        it "Burning encodes to Constr 2 []"
            $ toData' Burning
            `shouldBe` Constr 2 []
        it "End encodes to Constr 0 []"
            $ toData' End
            `shouldBe` Constr 0 []
        it "Retract encodes to Constr 3 [ref]" $ do
            let ref =
                    OnChainTxOutRef
                        (BuiltinByteString "tx")
                        7
            toData' (Retract ref)
                `shouldBe` Constr
                    3
                    [Constr 0 [B "tx", I 7]]
        it "Rejected encodes to Constr 1 []"
            $ toData' Rejected
            `shouldBe` Constr 1 []
        it "OpInsert encodes to Constr 0 [B v]"
            $ toData' (OpInsert "hello")
            `shouldBe` Constr 0 [B "hello"]
        it "StateDatum wraps Constr 1" $ do
            let st =
                    OnChainTokenState
                        { stateOwner =
                            BuiltinByteString "own"
                        , stateRoot =
                            OnChainRoot "rt"
                        , stateTip = 1000
                        , stateProcessTime = 300
                        , stateRetractTime = 600
                        }
            toData' (StateDatum st)
                `shouldBe` Constr
                    1
                    [ Constr
                        0
                        [ B "own"
                        , B "rt"
                        , I 1000
                        , I 300
                        , I 600
                        ]
                    ]

    describe "Asset-name derivation" $ do
        it "produces 32 bytes"
            $ let ref =
                    OnChainTxOutRef
                        (BuiltinByteString $ BS.replicate 32 0)
                        0
              in  BS.length (deriveAssetName ref)
                    `shouldBe` 32
        it "differs for different indices" $ do
            let txid =
                    BuiltinByteString
                        $ BS.replicate 32 0xAB
                ref0 = OnChainTxOutRef txid 0
                ref1 = OnChainTxOutRef txid 1
            deriveAssetName ref0
                `shouldBe` deriveAssetName ref0
            (deriveAssetName ref0 /= deriveAssetName ref1)
                `shouldBe` True

    describe "Script hash" $ do
        it "is 28 bytes"
            $ BS.length cageScriptHash
            `shouldBe` 28

    describe "Blueprint compliance" $ do
        mPath <- runIO $ lookupEnv "MPFS_BLUEPRINT"
        case mPath of
            Nothing ->
                it
                    "skipped (MPFS_BLUEPRINT not set)"
                    (pure () :: IO ())
            Just path -> do
                ebp <- runIO $ loadBlueprint path
                case ebp of
                    Left err ->
                        it
                            ( "loads blueprint: "
                                <> err
                            )
                            ( error
                                "blueprint parse failed"
                                :: IO ()
                            )
                    Right bp -> do
                        let defs = definitions bp
                            check
                                :: (ToData a)
                                => Text
                                -> a
                                -> Property
                            check defKey x =
                                let schema =
                                        defs
                                            Map.! defKey
                                in  validateData
                                        defs
                                        schema
                                        (toData' x)
                                        === True
                        it "CageDatum"
                            $ property
                            $ \x ->
                                check
                                    "types/CageDatum"
                                    (x :: CageDatum)
                        it "MintRedeemer"
                            $ property
                            $ \x ->
                                check
                                    "types/MintRedeemer"
                                    (x :: MintRedeemer)
                        it "UpdateRedeemer"
                            $ property
                            $ \x ->
                                check
                                    "types/UpdateRedeemer"
                                    (x :: UpdateRedeemer)
                        it "Operation"
                            $ property
                            $ \x ->
                                check
                                    "types/Operation"
                                    (x :: OnChainOperation)
                        it "Request"
                            $ property
                            $ \x ->
                                check
                                    "types/Request"
                                    (x :: OnChainRequest)
                        it "TokenState"
                            $ property
                            $ \x ->
                                check
                                    "types/State"
                                    (x :: OnChainTokenState)
                        it "Mint"
                            $ property
                            $ \x ->
                                check
                                    "types/Mint"
                                    (x :: Mint)
                        it "TokenId"
                            $ property
                            $ \x ->
                                check
                                    "lib/TokenId"
                                    (x :: OnChainTokenId)
                        it "ProofStep"
                            $ property
                            $ \x ->
                                check
                                    "aiken/merkle_patricia_forestry/ProofStep"
                                    (x :: ProofStep)
                        it "Neighbor"
                            $ property
                            $ \x ->
                                check
                                    "aiken/merkle_patricia_forestry/Neighbor"
                                    (x :: Neighbor)
                        it "TxOutRef"
                            $ property
                            $ \x ->
                                check
                                    "cardano/transaction/OutputReference"
                                    (x :: OnChainTxOutRef)
                        it "script hash matches"
                            $ let mHash =
                                    extractScriptHash
                                        "cage."
                                        bp
                              in  mHash
                                    `shouldBe` Just
                                        ( toHex
                                            cageScriptHash
                                        )
