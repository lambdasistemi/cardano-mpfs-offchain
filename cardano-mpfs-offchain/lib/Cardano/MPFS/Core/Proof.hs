-- |
-- Module      : Cardano.MPFS.Core.Proof
-- Description : Aiken-compatible proof serialization
-- License     : Apache-2.0
--
-- Serializes 'MPFProof' (from @mts:mpf@)
-- to the CBOR\/PlutusData format expected by the Aiken
-- on-chain validator, and converts proofs to the on-chain
-- 'ProofStep' type used in 'UpdateRedeemer'.
--
-- The CBOR encoding uses indefinite-length lists and
-- bytestrings to match the reference TypeScript
-- implementation byte-for-byte. Steps are reversed from
-- the library's leaf-to-root order to the root-to-leaf
-- order expected on-chain.
module Cardano.MPFS.Core.Proof
    ( -- * Serialization
      serializeProof

      -- * Conversion to on-chain types
    , toProofSteps
    ) where

import Data.ByteString (ByteString)
import Data.ByteString qualified as B
import Data.ByteString.Builder
    ( Builder
    , byteString
    , toLazyByteString
    , word8
    )
import Data.ByteString.Lazy qualified as BL
import Data.Map.Strict qualified as Map
import Data.Word (Word8)
import MPF.Hashes
    ( MPFHash
    , merkleProof
    , nibbleBytes
    , packHexKey
    , renderMPFHash
    )
import MPF.Interface (HexDigit (..))
import MPF.Proof.Insertion
    ( MPFProof (..)
    , MPFProofStep (..)
    )

import Cardano.MPFS.Core.OnChain
    ( Neighbor (..)
    , ProofStep (..)
    )

-- | Serialize an 'MPFProof' to Aiken-compatible
-- PlutusData CBOR bytes.
--
-- The output is byte-identical to the TypeScript
-- reference @proof.toCBOR()@.
serializeProof
    :: MPFProof MPFHash
    -- ^ Proof produced by an insert\/delete\/update
    -> ByteString
serializeProof MPFProof{mpfProofSteps} =
    BL.toStrict
        . toLazyByteString
        $ cborBeginList
            <> mconcat
                -- Steps are stored leaf-to-root for
                -- foldl'; CBOR needs root-to-leaf.
                (map encodeStep (reverse mpfProofSteps))
            <> cborEnd

-- | Encode a single proof step as PlutusData CBOR.
encodeStep :: MPFProofStep MPFHash -> Builder
encodeStep
    ( ProofStepBranch
            { psbJump
            , psbPosition
            , psbSiblingHashes
            }
        ) =
        -- Aiken: Branch { skip, neighbors }
        -- tag 121 [ skip, indef-bytes neighbors ]
        let skip = length psbJump
            sparseChildren =
                buildSparse psbSiblingHashes
            pos =
                fromIntegral (unHexDigit psbPosition)
            neighborHashes =
                map renderMPFHash
                    $ merkleProof sparseChildren pos
            -- 4 × 32 = 128 bytes, split into two
            -- 64-byte chunks (matching TypeScript)
            allBytes = mconcat neighborHashes
            (chunk1, chunk2) = B.splitAt 64 allBytes
        in  cborTag 121
                <> cborBeginList
                <> cborInt skip
                <> cborBeginBytes
                <> cborBytes chunk1
                <> cborBytes chunk2
                <> cborEnd
                <> cborEnd
encodeStep
    ( ProofStepFork
            { psfBranchJump
            , psfNeighborPrefix
            , psfNeighborIndex
            , psfMerkleRoot
            }
        ) =
        -- Aiken: Fork { skip, neighbor }
        -- tag 122 [ skip, tag 121 [ nibble, prefix,
        --                           root ] ]
        let skip = length psfBranchJump
            nibble =
                fromIntegral
                    (unHexDigit psfNeighborIndex)
            prefix = nibbleBytes psfNeighborPrefix
            root = renderMPFHash psfMerkleRoot
        in  cborTag 122
                <> cborBeginList
                <> cborInt skip
                <> cborTag 121
                <> cborBeginList
                <> cborInt nibble
                <> cborBytes prefix
                <> cborBytes root
                <> cborEnd
                <> cborEnd
encodeStep
    ( ProofStepLeaf
            { pslBranchJump
            , pslNeighborKeyPath
            , pslNeighborValueDigest
            }
        ) =
        -- Aiken: Leaf { skip, key, value }
        -- tag 123 [ skip, key, value ]
        let skip = length pslBranchJump
            key = packHexKey pslNeighborKeyPath
            value =
                renderMPFHash pslNeighborValueDigest
        in  cborTag 123
                <> cborBeginList
                <> cborInt skip
                <> cborBytes key
                <> cborBytes value
                <> cborEnd

-- | Convert an 'MPFProof' to on-chain 'ProofStep's.
--
-- Steps are reversed from leaf-to-root storage order
-- to root-to-leaf (same as 'serializeProof').
toProofSteps
    :: MPFProof MPFHash
    -- ^ Proof produced by an insert\/delete\/update
    -> [ProofStep]
toProofSteps MPFProof{mpfProofSteps} =
    map convertStep (reverse mpfProofSteps)

-- | Convert a single 'MPFProofStep' to an on-chain
-- 'ProofStep'.
convertStep
    :: MPFProofStep MPFHash -> ProofStep
convertStep
    ProofStepBranch
        { psbJump
        , psbPosition
        , psbSiblingHashes
        } =
        let skip =
                fromIntegral (length psbJump)
            sparseChildren =
                buildSparse psbSiblingHashes
            pos =
                fromIntegral
                    (unHexDigit psbPosition)
            neighborHashes =
                map renderMPFHash
                    $ merkleProof sparseChildren pos
            neighbors = mconcat neighborHashes
        in  Branch skip neighbors
convertStep
    ProofStepFork
        { psfBranchJump
        , psfNeighborPrefix
        , psfNeighborIndex
        , psfMerkleRoot
        } =
        let skip =
                fromIntegral (length psfBranchJump)
            nibble =
                fromIntegral
                    (unHexDigit psfNeighborIndex)
            prefix =
                nibbleBytes psfNeighborPrefix
            root = renderMPFHash psfMerkleRoot
        in  Fork
                skip
                Neighbor
                    { neighborNibble = nibble
                    , neighborPrefix = prefix
                    , neighborRoot = root
                    }
convertStep
    ProofStepLeaf
        { pslBranchJump
        , pslNeighborKeyPath
        , pslNeighborValueDigest
        } =
        let skip =
                fromIntegral (length pslBranchJump)
            key =
                packHexKey pslNeighborKeyPath
            value =
                renderMPFHash pslNeighborValueDigest
        in  Leaf skip key value

-- | Build a sparse 16-element array from sibling
-- hashes for 'merkleProof'.
buildSparse
    :: [(HexDigit, MPFHash)] -> [Maybe MPFHash]
buildSparse siblings =
    let m = Map.fromList siblings
    in  [ Map.lookup (HexDigit n) m
        | n <- [0 .. 15]
        ]

-- ----------------------------------------------------------
-- Minimal CBOR encoding (PlutusData subset)
--
-- Uses indefinite-length encoding to match the
-- reference TypeScript implementation byte-for-byte.
-- ----------------------------------------------------------

-- | CBOR tag (major type 6).
cborTag :: Int -> Builder
cborTag n
    | n < 24 = word8 (0xc0 + fromIntegral n)
    | n < 256 =
        word8 0xd8 <> word8 (fromIntegral n)
    | otherwise =
        error "cborTag: value too large"

-- | Begin an indefinite-length CBOR list.
cborBeginList :: Builder
cborBeginList = word8 0x9f

-- | Begin an indefinite-length CBOR bytestring.
cborBeginBytes :: Builder
cborBeginBytes = word8 0x5f

-- | End an indefinite-length CBOR item.
cborEnd :: Builder
cborEnd = word8 0xff

-- | CBOR non-negative integer (major type 0).
cborInt :: Int -> Builder
cborInt = cborMajor 0

-- | CBOR definite-length bytestring (major type 2).
cborBytes :: ByteString -> Builder
cborBytes bs =
    cborMajor 2 (B.length bs) <> byteString bs

-- | Encode a CBOR major type + argument.
cborMajor :: Word8 -> Int -> Builder
cborMajor major n
    | n < 24 =
        word8 (major * 32 + fromIntegral n)
    | n < 256 =
        word8 (major * 32 + 24)
            <> word8 (fromIntegral n)
    | n < 65536 =
        word8 (major * 32 + 25)
            <> word8 (fromIntegral (n `div` 256))
            <> word8 (fromIntegral (n `mod` 256))
    | otherwise =
        error "cborMajor: value too large"
