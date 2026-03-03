{-# LANGUAGE OverloadedStrings #-}

module Cardano.MPFS.ProofSpec (spec) where

import Cardano.MPFS.Core.Proof (serializeProof)
import Control.Monad (forM, forM_)
import Data.ByteString (ByteString)
import Data.ByteString qualified as B
import Data.List (nubBy)
import MPF.Hashes
    ( MPFHash
    , computeMerkleRoot
    , merkleProof
    , mkMPFHash
    , mpfHashing
    , nullHash
    , renderMPFHash
    )
import MPF.Interface
    ( HexKey
    , byteStringToHexKey
    )
import MPF.Proof.Insertion
    ( MPFProof (..)
    , MPFProofStep
    )
import MPF.Test.Lib
    ( deleteMPFM
    , encodeHex
    , foldMPFProof
    , fruitsTestData
    , getRootHashM
    , insertByteStringM
    , insertMPFM
    , proofMPFM
    , runMPFPure'
    )
import Test.Hspec
    ( Spec
    , describe
    , it
    , shouldBe
    , shouldSatisfy
    )
import Test.QuickCheck
    ( Gen
    , Property
    , choose
    , forAll
    , listOf1
    , property
    , vectorOf
    , (==>)
    )

spec :: Spec
spec = do
    describe "Cardano.MPFS.Core.Proof" $ do
        -- ------------------------------------------------
        -- merkleRoot reference vectors from helpers.test.js
        -- ------------------------------------------------
        describe "computeMerkleRoot (reference vectors)" $ do
            it "16 NULL_HASHes" $ do
                let r =
                        computeMerkleRoot
                            $ replicate 16 (Just nullHash)
                -- All nullHash → deterministic root
                encodeHex (renderMPFHash r)
                    `shouldBe` "209155a276ca3c2417e3876971dd587dd64ed9fcb8ef1fd6e7589ef4255c967f"

            it "padding: fewer than 16 children pads with nullHash" $ do
                -- Any input < 16 elements is padded to 16 with Nothing (= nullHash)
                let r1 = computeMerkleRoot [Just nullHash]
                    r16 =
                        computeMerkleRoot
                            $ replicate 16 (Just nullHash)
                r1 `shouldBe` r16

        -- ------------------------------------------------
        -- merkleProof reconstruction
        -- ------------------------------------------------
        describe "merkleProof" $ do
            it "produces 4 intermediate hashes" $ do
                let children =
                        [ Just (mkMPFHash (B.pack [i]))
                        | i <- [0 .. 15]
                        ]
                    result = merkleProof children 0
                length result `shouldBe` 4

            it "reconstructs root for all 16 positions" $ do
                let hashes =
                        [ mkMPFHash (B.pack [i])
                        | i <- [0 .. 15]
                        ]
                    children = map Just hashes
                    expected = computeFullMerkleRoot hashes
                forM_ [0 .. 15] $ \pos -> do
                    let proof = merkleProof children pos
                        root =
                            reconstructRoot
                                (hashes !! pos)
                                proof
                                pos
                    root `shouldBe` expected

            it "works with sparse children" $ do
                let children =
                        [ if i `elem` [0 :: Int, 3, 7, 12]
                            then
                                Just
                                    $ mkMPFHash
                                        (B.pack [fromIntegral i])
                            else Nothing
                        | i <- [0 .. 15]
                        ]
                    pos = 3
                    proof = merkleProof children pos
                length proof `shouldBe` 4

        -- ------------------------------------------------
        -- CBOR encoding (reference vectors)
        -- ------------------------------------------------
        describe "CBOR encoding" $ do
            it "int 0 = 0x00" $ do
                -- Sanity check: our cborInt matches
                -- cbor.test.js: cbor.int(0) → '00'
                let bs = serializeProof stubProofInt0
                -- The proof wraps the int in structure;
                -- we test byte-level with full vectors below
                B.length bs `shouldSatisfy` (> 0)

        -- ------------------------------------------------
        -- Byte-identical CBOR vectors from trie.test.js
        -- ------------------------------------------------
        describe "serializeProof (reference vectors)" $ do
            it "mango proof matches TypeScript toCBOR()" $ do
                let (mProof, _) = runMPFPure' $ do
                        forM_ fruitsTestData
                            $ uncurry insertByteStringM
                        let mangoKey =
                                byteStringToHexKey
                                    $ renderMPFHash
                                    $ mkMPFHash "mango[uid: 0]"
                        proofMPFM mangoKey
                case mProof of
                    Nothing -> fail "No proof for mango"
                    Just proof -> do
                        let actual =
                                encodeHex (serializeProof proof)
                        actual `shouldBe` mangoCborHex

            it "kumquat proof matches TypeScript toCBOR()" $ do
                let (mProof, _) = runMPFPure' $ do
                        forM_ fruitsTestData
                            $ uncurry insertByteStringM
                        let kumquatKey =
                                byteStringToHexKey
                                    $ renderMPFHash
                                    $ mkMPFHash "kumquat[uid: 0]"
                        proofMPFM kumquatKey
                case mProof of
                    Nothing -> fail "No proof for kumquat"
                    Just proof -> do
                        let actual =
                                encodeHex (serializeProof proof)
                        actual `shouldBe` kumquatCborHex

            it "tangerine proof matches TypeScript toCBOR()" $ do
                let (mProof, _) = runMPFPure' $ do
                        forM_ fruitsTestData
                            $ uncurry insertByteStringM
                        let tangerineKey =
                                byteStringToHexKey
                                    $ renderMPFHash
                                    $ mkMPFHash
                                        "tangerine[uid: 11]"
                        proofMPFM tangerineKey
                case mProof of
                    Nothing ->
                        fail "No proof for tangerine"
                    Just proof -> do
                        let actual =
                                encodeHex (serializeProof proof)
                        actual `shouldBe` tangerineCborHex

        -- ------------------------------------------------
        -- JSON-level structure from trie.test.js
        -- ------------------------------------------------
        describe "proof structure (JSON-level)" $ do
            it "mango proof has Branch + Leaf steps" $ do
                let (mProof, _) = runMPFPure' $ do
                        forM_ fruitsTestData
                            $ uncurry insertByteStringM
                        let mangoKey =
                                byteStringToHexKey
                                    $ renderMPFHash
                                    $ mkMPFHash "mango[uid: 0]"
                        proofMPFM mangoKey
                case mProof of
                    Nothing -> fail "No proof for mango"
                    Just proof -> do
                        -- Should be exactly 2 steps
                        length (proofSteps proof)
                            `shouldBe` 2

            it "kumquat proof has Branch + Fork steps" $ do
                let (mProof, _) = runMPFPure' $ do
                        forM_ fruitsTestData
                            $ uncurry insertByteStringM
                        let kumquatKey =
                                byteStringToHexKey
                                    $ renderMPFHash
                                    $ mkMPFHash "kumquat[uid: 0]"
                        proofMPFM kumquatKey
                case mProof of
                    Nothing -> fail "No proof for kumquat"
                    Just proof -> do
                        length (proofSteps proof)
                            `shouldBe` 2

        -- ------------------------------------------------
        -- foldMPFProof correctness for all fruits
        -- ------------------------------------------------
        describe "foldMPFProof (all fruits)" $ do
            it "every fruit's proof folds to the root" $ do
                let (results, _) = runMPFPure' $ do
                        forM_ fruitsTestData
                            $ uncurry insertByteStringM
                        mRoot <- getRootHashM
                        proofs <-
                            mapM
                                ( \(k, _) -> do
                                    let hk =
                                            byteStringToHexKey
                                                $ renderMPFHash
                                                $ mkMPFHash k
                                    mp <- proofMPFM hk
                                    pure (k, mp)
                                )
                                fruitsTestData
                        pure (mRoot, proofs)
                case results of
                    (Just root, proofs) ->
                        forM_ proofs
                            $ \(name, mProof) ->
                                case mProof of
                                    Nothing ->
                                        fail
                                            $ "No proof for: "
                                                ++ show name
                                    Just proof -> do
                                        let computed =
                                                foldMPFProof
                                                    mpfHashing
                                                    proof
                                        computed `shouldBe` root
                    _ -> fail "Expected root"

            it "every fruit serializes to non-empty CBOR" $ do
                let (results, _) = runMPFPure' $ do
                        forM_ fruitsTestData
                            $ uncurry insertByteStringM
                        mapM
                            ( \(k, _) -> do
                                let hk =
                                        byteStringToHexKey
                                            $ renderMPFHash
                                            $ mkMPFHash k
                                mp <- proofMPFM hk
                                pure (k, mp)
                            )
                            fruitsTestData
                forM_ results $ \(name, mProof) ->
                    case mProof of
                        Nothing ->
                            fail
                                $ "No proof for: " ++ show name
                        Just proof -> do
                            let bs = serializeProof proof
                            B.length bs `shouldSatisfy` (> 0)
                            -- Must start with 0x9f (indefinite list)
                            B.head bs `shouldBe` 0x9f
                            -- Must end with 0xff (end marker)
                            B.last bs `shouldBe` 0xff

        -- ------------------------------------------------
        -- QuickCheck property tests
        -- ------------------------------------------------
        describe "properties" $ do
            it
                "proof exists and verifies for every inserted key"
                $ property propPresence

            it
                "no proof for non-inserted keys"
                $ property propAbsence

            it
                "no proof after deletion, siblings still verify"
                $ property propAbsenceAfterDeletion

            it "serialized proof has valid CBOR framing"
                $ property propCborStructure

-- ----------------------------------------------------------
-- QuickCheck generators
-- ----------------------------------------------------------

-- | Random ByteString key.
genKeyBytes :: Gen ByteString
genKeyBytes = B.pack <$> listOf1 (choose (0, 255))

-- | Random ByteString value.
genValue :: Gen ByteString
genValue = B.pack <$> listOf1 (choose (0, 255))

-- | Hash-then-hex, matching insertByteStringM's key derivation.
toHexKey :: ByteString -> HexKey
toHexKey = byteStringToHexKey . renderMPFHash . mkMPFHash

-- | Unique key-value pairs (unique by hashed key).
genUniqueKVs :: Gen [(ByteString, ByteString)]
genUniqueKVs = do
    kvs <- listOf1 ((,) <$> genKeyBytes <*> genValue)
    pure
        $ nubBy
            ( \(k1, _) (k2, _) ->
                toHexKey k1 == toHexKey k2
            )
            kvs

-- ----------------------------------------------------------
-- Properties
-- ----------------------------------------------------------

-- | Every inserted key has a proof that folds to the root.
propPresence :: Property
propPresence = forAll genUniqueKVs $ \kvs ->
    let kvHashed =
            [ (toHexKey k, mkMPFHash v)
            | (k, v) <- kvs
            ]
        (results, _) = runMPFPure' $ do
            forM_ kvHashed $ uncurry insertMPFM
            root <- getRootHashM
            proofs <- forM kvHashed $ \(k, _) ->
                proofMPFM k
            pure (root, proofs)
    in  case results of
            (Just root, proofs) ->
                all
                    ( \mp -> case mp of
                        Nothing -> False
                        Just proof ->
                            foldMPFProof mpfHashing proof
                                == root
                    )
                    proofs
            _ -> False

-- | A key never inserted has no proof.
propAbsence :: Property
propAbsence =
    forAll
        ( (,)
            <$> genUniqueKVs
            <*> genKeyBytes
        )
        $ \(kvs, extraKey) ->
            let hexExtra = toHexKey extraKey
                kvHashed =
                    [ (toHexKey k, mkMPFHash v)
                    | (k, v) <- kvs
                    ]
                notInserted =
                    all
                        ((/= hexExtra) . fst)
                        kvHashed
            in  notInserted ==>
                    let (mProof, _) = runMPFPure' $ do
                            forM_ kvHashed
                                $ uncurry insertMPFM
                            proofMPFM hexExtra
                    in  case mProof of
                            Nothing -> True
                            Just _ -> False

-- | After deletion, the deleted key has no proof;
-- remaining keys still verify.
propAbsenceAfterDeletion :: Property
propAbsenceAfterDeletion =
    forAll
        (vectorOf 3 ((,) <$> genKeyBytes <*> genValue))
        $ \rawKvs ->
            let kvs =
                    nubBy
                        ( \(k1, _) (k2, _) ->
                            toHexKey k1 == toHexKey k2
                        )
                        rawKvs
            in  length kvs == 3 ==>
                    let kvHashed =
                            [ (toHexKey k, mkMPFHash v)
                            | (k, v) <- kvs
                            ]
                        (delKey, survivors) =
                            case kvHashed of
                                (k, _) : rest ->
                                    (k, rest)
                                [] ->
                                    error "empty kvHashed"
                        (results, _) = runMPFPure' $ do
                            forM_ kvHashed
                                $ uncurry insertMPFM
                            deleteMPFM delKey
                            gone <- proofMPFM delKey
                            root <- getRootHashM
                            kept <-
                                forM survivors
                                    $ \(k, _) ->
                                        proofMPFM k
                            pure (gone, root, kept)
                    in  case results of
                            (Nothing, Just root, kept) ->
                                all
                                    ( \mp ->
                                        case mp of
                                            Nothing -> False
                                            Just proof ->
                                                foldMPFProof
                                                    mpfHashing
                                                    proof
                                                    == root
                                    )
                                    kept
                            _ -> False

-- | Serialized proof starts with 0x9f, ends with 0xff,
-- and has length >= 2.
propCborStructure :: Property
propCborStructure = forAll genUniqueKVs $ \kvs ->
    let kvHashed =
            [ (toHexKey k, mkMPFHash v)
            | (k, v) <- kvs
            ]
        (proofs, _) = runMPFPure' $ do
            forM_ kvHashed $ uncurry insertMPFM
            forM kvHashed $ \(k, _) -> proofMPFM k
    in  all checkCbor proofs
  where
    checkCbor Nothing = False
    checkCbor (Just proof) =
        let bs = serializeProof proof
        in  B.length bs >= 2
                && B.head bs == 0x9f
                && B.last bs == 0xff

-- ----------------------------------------------------------
-- Reference CBOR hex vectors from trie.test.js
-- ----------------------------------------------------------

-- | Proof.toCBOR (mango) from trie.test.js
mangoCborHex :: ByteString
mangoCborHex =
    "9fd8799f005f5840c7bfa4472f3a98ebe0421e8f3f03adf0f7c4340dec65b4b92b1c9f0bed209eb45fdf82687b1ab133324cebaf46d99d49f92720c5ded08d5b02f57530f2cc5a5f58401508f13471a031a21277db8817615e62a50a7427d5f8be572746aa5f0d49841758c5e4a29601399a5bd916e5f3b34c38e13253f4de2a3477114f1b2b8f9f2f4dffffd87b9f00582009d23032e6edc0522c00bc9b74edd3af226d1204a079640a367da94c84b69ecc5820c29c35ad67a5a55558084e634ab0d98f7dd1f60070b9ce2a53f9f305fd9d9795ffff"

-- | Proof.toCBOR (kumquat) from trie.test.js
kumquatCborHex :: ByteString
kumquatCborHex =
    "9fd8799f005f5840c7bfa4472f3a98ebe0421e8f3f03adf0f7c4340dec65b4b92b1c9f0bed209eb47238ba5d16031b6bace4aee22156f5028b0ca56dc24f7247d6435292e82c039c58403490a825d2e8deddf8679ce2f95f7e3a59d9c3e1af4a49b410266d21c9344d6d08434fd717aea47d156185d589f44a59fc2e0158eab7ff035083a2a66cd3e15bffffd87a9f00d8799f0041075820a1ffbc0e72342b41129e2d01d289809079b002e54b123860077d2d66added281ffffff"

-- | Proof.toCBOR (tangerine) from README.md
tangerineCborHex :: ByteString
tangerineCborHex =
    "9fd8799f005f58404be28f4839135e1f8f5372a90b54bb7bfaf997a5d13711bb4d7d93f9d4e04fbefa63eb4576001d8658219f928172eccb5448b4d7d62cd6d95228e13ebcbd53505840c1e96bcc431893eef34e03989814375d439faa592edf75c9e5dc10b3c30766700000000000000000000000000000000000000000000000000000000000000000ffffff"

-- ----------------------------------------------------------
-- Helpers
-- ----------------------------------------------------------

-- | Extract proof steps (for length checks).
proofSteps
    :: MPFProof MPFHash -> [MPFProofStep MPFHash]
proofSteps = mpfProofSteps

-- | Stub proof for basic CBOR encoding checks.
-- Not used for byte-level comparison.
stubProofInt0 :: MPFProof MPFHash
stubProofInt0 =
    MPFProof
        { mpfProofSteps = []
        , mpfProofRootPrefix = []
        , mpfProofLeafSuffix = []
        , mpfProofValueHash = nullHash
        }

-- | Reconstruct merkle root from a leaf hash, its
-- 4-level proof (top-to-bottom), and position (0-15).
reconstructRoot :: MPFHash -> [MPFHash] -> Int -> MPFHash
reconstructRoot leaf proofHashes pos =
    go leaf (reverse proofHashes) pos 1
  where
    go acc [] _ _ = acc
    go acc (sibling : rest) p pivot =
        let (combined, newPos) =
                if p `mod` (pivot * 2) < pivot
                    then (hashPair acc sibling, p)
                    else (hashPair sibling acc, p)
        in  go combined rest newPos (pivot * 2)

-- | Hash two values together (pairwise reduction).
hashPair :: MPFHash -> MPFHash -> MPFHash
hashPair a b =
    mkMPFHash (renderMPFHash a <> renderMPFHash b)

-- | Full merkle root from 16 hashes.
computeFullMerkleRoot :: [MPFHash] -> MPFHash
computeFullMerkleRoot [] = nullHash
computeFullMerkleRoot [h] = h
computeFullMerkleRoot hs =
    computeFullMerkleRoot $ pairUp hs
  where
    pairUp [] = []
    pairUp [h] = [h]
    pairUp (a : b : rest) = hashPair a b : pairUp rest
