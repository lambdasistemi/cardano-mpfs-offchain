-- |
-- Module      : Cardano.MPFS.Client.Verify.MPF
-- Description : MPF proof root reconstruction helpers.
module Cardano.MPFS.Client.Verify.MPF
    ( computeAikenProofRoot
    ) where

import Control.Monad (guard, when)
import Data.Bits (testBit)
import Data.ByteString (ByteString)
import Data.ByteString qualified as BS
import Data.Maybe (fromMaybe)
import Data.Word (Word8)

import MPF.Hashes
    ( MPFHash (..)
    , MPFHashing (..)
    , aikenKeyPath
    , branchHash
    , byteStringToHexKey'
    , leafHash
    , merkleRoot
    , mkMPFHash
    , mpfHashing
    , nullHash
    , renderMPFHash
    )
import MPF.Interface (HexDigit (..), HexKey)

data AikenNeighbor = AikenNeighbor
    { anNibble :: !HexDigit
    , anPrefix :: !HexKey
    , anRoot :: !MPFHash
    }

data AikenProofStep
    = AikenBranch
        { apsSkip :: !Int
        , apsNeighbors :: ![MPFHash]
        }
    | AikenFork
        { apsSkip :: !Int
        , apsNeighbor :: !AikenNeighbor
        }
    | AikenLeaf
        { apsSkip :: !Int
        , apsKey :: !HexKey
        , apsValue :: !MPFHash
        }

computeAikenProofRoot
    :: Bool
    -> ByteString
    -> Maybe ByteString
    -> ByteString
    -> Maybe ByteString
computeAikenProofRoot including key value proofBytes = do
    proofSteps <- parseExactAikenProof proofBytes
    case foldAikenProof
        including
        (aikenKeyPath key)
        (mkMPFHash <$> value)
        proofSteps of
        Just (Just computedRoot) -> Just (renderMPFHash computedRoot)
        Just Nothing -> Just (renderMPFHash nullHash)
        Nothing -> Nothing

foldAikenProof
    :: Bool
    -> HexKey
    -> Maybe MPFHash
    -> [AikenProofStep]
    -> Maybe (Maybe MPFHash)
foldAikenProof including path valueDigest =
    go 0
  where
    go cursor [] =
        if including
            then
                Just . leafHash mpfHashing (drop cursor path)
                    <$> valueDigest
            else pure Nothing
    go cursor (proofStep : rest) = do
        let skip = apsSkip proofStep
            nibbleIx = cursor + skip
            isLast = null rest
        prefix <- sliceHex cursor nibbleIx path
        acc <- go (nibbleIx + 1) rest
        case proofStep of
            AikenBranch{apsNeighbors} -> do
                ourNibble <- indexHex nibbleIx path
                let merkle =
                        rebuildMerkleRoot
                            (hexDigitToInt ourNibble)
                            acc
                            apsNeighbors
                pure
                    $ Just
                    $ branchHash mpfHashing prefix merkle
            AikenFork{apsNeighbor = AikenNeighbor{anNibble, anPrefix, anRoot}}
                | not including && isLast ->
                    pure
                        $ Just
                        $ branchHash
                            mpfHashing
                            (prefix <> [anNibble] <> anPrefix)
                            anRoot
                | otherwise -> do
                    ourNibble <- indexHex nibbleIx path
                    guard (ourNibble /= anNibble)
                    let neighborHash =
                            branchHash mpfHashing anPrefix anRoot
                        sparseChildren =
                            [ if HexDigit n == ourNibble
                                then acc
                                else
                                    if HexDigit n == anNibble
                                        then Just neighborHash
                                        else Nothing
                            | n <- [0 .. 15]
                            ]
                    pure
                        $ Just
                        $ branchHash
                            mpfHashing
                            prefix
                            (merkleRoot mpfHashing sparseChildren)
            AikenLeaf{apsKey, apsValue} -> do
                witnessPrefix <- sliceHex 0 cursor apsKey
                guard (witnessPrefix == take cursor path)
                targetNibble <- indexHex nibbleIx path
                neighborNibble <- indexHex nibbleIx apsKey
                guard (neighborNibble /= targetNibble)
                if not including && isLast
                    then
                        pure
                            $ Just
                            $ leafHash
                                mpfHashing
                                (drop cursor apsKey)
                                apsValue
                    else do
                        let neighborSuffix = drop (nibbleIx + 1) apsKey
                            neighborHash =
                                leafHash
                                    mpfHashing
                                    neighborSuffix
                                    apsValue
                            sparseChildren =
                                [ if HexDigit n == targetNibble
                                    then acc
                                    else
                                        if HexDigit n == neighborNibble
                                            then Just neighborHash
                                            else Nothing
                                | n <- [0 .. 15]
                                ]
                        pure
                            $ Just
                            $ branchHash
                                mpfHashing
                                prefix
                                (merkleRoot mpfHashing sparseChildren)

hexDigitToInt :: HexDigit -> Int
hexDigitToInt (HexDigit w) = fromIntegral w

rebuildMerkleRoot
    :: Int -> Maybe MPFHash -> [MPFHash] -> MPFHash
rebuildMerkleRoot position acc =
    foldl' step (fromMaybe nullHash acc) . zip [0 :: Int ..] . reverse
  where
    step current (depth, siblingHash)
        | testBit position depth =
            mkMPFHash (renderMPFHash siblingHash <> renderMPFHash current)
        | otherwise =
            mkMPFHash (renderMPFHash current <> renderMPFHash siblingHash)

sliceHex :: Int -> Int -> HexKey -> Maybe HexKey
sliceHex start end key
    | start < 0 || end < start = Nothing
    | otherwise =
        let prefix = take (end - start) (drop start key)
        in  if length prefix == end - start then Just prefix else Nothing

indexHex :: Int -> HexKey -> Maybe HexDigit
indexHex ix key
    | ix < 0 = Nothing
    | otherwise = case drop ix key of
        d : _ -> Just d
        [] -> Nothing

parseExactAikenProof :: ByteString -> Maybe [AikenProofStep]
parseExactAikenProof bs = case parseBytes bs of
    Just (steps, rest) | BS.null rest -> Just steps
    _ -> Nothing

type Parser a = ByteString -> Maybe (a, ByteString)

parseByte :: Parser Word8
parseByte bs = case BS.uncons bs of
    Just (w, rest) -> Just (w, rest)
    Nothing -> Nothing

expectByte :: Word8 -> Parser ()
expectByte expected bs = case parseByte bs of
    Just (w, rest) | w == expected -> Just ((), rest)
    _ -> Nothing

parseUInt :: Parser Int
parseUInt bs = case parseByte bs of
    Just (w, rest)
        | w < 24 -> Just (fromIntegral w, rest)
        | w == 0x18 -> case parseByte rest of
            Just (v, rest') -> Just (fromIntegral v, rest')
            Nothing -> Nothing
        | w == 0x19 -> do
            (bytes, rest') <- takeN 2 rest
            let hi = BS.index bytes 0
                lo = BS.index bytes 1
            Just (fromIntegral hi * 256 + fromIntegral lo, rest')
    _ -> Nothing

parseDefBytes :: Parser ByteString
parseDefBytes bs = case parseByte bs of
    Just (w, rest)
        | w >= 0x40 && w <= 0x57 ->
            takeN (fromIntegral (w - 0x40)) rest
        | w == 0x58 -> case parseByte rest of
            Just (len, rest') -> takeN (fromIntegral len) rest'
            Nothing -> Nothing
        | w == 0x59 -> do
            (bytes, rest') <- takeN 2 rest
            let hi = BS.index bytes 0
                lo = BS.index bytes 1
                len = fromIntegral hi * 256 + fromIntegral lo
            takeN len rest'
    _ -> Nothing

parseIndefBytes :: Parser ByteString
parseIndefBytes bs = case expectByte 0x5f bs of
    Just ((), rest) -> collectChunks [] rest
    Nothing -> Nothing
  where
    collectChunks acc bs' = case parseByte bs' of
        Just (0xff, rest) -> Just (BS.concat (reverse acc), rest)
        _ -> do
            (chunk, rest) <- parseDefBytes bs'
            collectChunks (chunk : acc) rest

parseCBORBytes :: Parser ByteString
parseCBORBytes bs = case parseDefBytes bs of
    Just out -> Just out
    Nothing -> parseIndefBytes bs

parseTag :: Parser Int
parseTag bs = case parseByte bs of
    Just (0xd8, rest) -> case parseByte rest of
        Just (v, rest') -> Just (fromIntegral v, rest')
        Nothing -> Nothing
    Just (0xd9, rest) -> do
        (bytes, rest') <- takeN 2 rest
        let hi = BS.index bytes 0
            lo = BS.index bytes 1
        Just (fromIntegral hi * 256 + fromIntegral lo, rest')
    _ -> Nothing

parseListBegin :: Parser ()
parseListBegin = expectByte 0x9f

parseBreak :: Parser ()
parseBreak = expectByte 0xff

takeN :: Int -> Parser ByteString
takeN n bs
    | BS.length bs >= n = Just (BS.take n bs, BS.drop n bs)
    | otherwise = Nothing

parseBranchStep :: Parser AikenProofStep
parseBranchStep bs = do
    (skip, bs1) <- parseUInt bs
    (neighborsBs, bs2) <- parseCBORBytes bs1
    ((), bs3) <- parseBreak bs2
    guard (BS.length neighborsBs == 128)
    pure (AikenBranch skip (splitHashes neighborsBs), bs3)

parseForkStep :: Parser AikenProofStep
parseForkStep bs = do
    (skip, bs1) <- parseUInt bs
    (tag, bs2) <- parseTag bs1
    when (tag /= 121) Nothing
    ((), bs3) <- parseListBegin bs2
    (nibble, bs4) <- parseUInt bs3
    (prefixBs, bs5) <- parseCBORBytes bs4
    (rootBs, bs6) <- parseCBORBytes bs5
    ((), bs7) <- parseBreak bs6
    ((), bs8) <- parseBreak bs7
    guard (nibble >= 0 && nibble < 16)
    neighborPrefix <- unpackNibblePrefix prefixBs
    neighborRoot <- MPFHash <$> takeExact 32 rootBs
    pure
        ( AikenFork
            { apsSkip = skip
            , apsNeighbor =
                AikenNeighbor
                    { anNibble = HexDigit (fromIntegral nibble)
                    , anPrefix = neighborPrefix
                    , anRoot = neighborRoot
                    }
            }
        , bs8
        )

parseLeafStep :: Parser AikenProofStep
parseLeafStep bs = do
    (skip, bs1) <- parseUInt bs
    (keyBs, bs2) <- parseCBORBytes bs1
    (valueBs, bs3) <- parseCBORBytes bs2
    ((), bs4) <- parseBreak bs3
    keyBytes <- takeExact 32 keyBs
    valueHash <- MPFHash <$> takeExact 32 valueBs
    pure
        ( AikenLeaf
            { apsSkip = skip
            , apsKey = byteStringToHexKey' keyBytes
            , apsValue = valueHash
            }
        , bs4
        )

parseStep :: Parser AikenProofStep
parseStep bs = do
    (tag, bs1) <- parseTag bs
    ((), bs2) <- parseListBegin bs1
    case tag of
        121 -> parseBranchStep bs2
        122 -> parseForkStep bs2
        123 -> parseLeafStep bs2
        _ -> Nothing

parseBytes :: Parser [AikenProofStep]
parseBytes bs = do
    ((), bs1) <- parseListBegin bs
    collectSteps [] bs1
  where
    collectSteps acc bs' = case parseByte bs' of
        Just (0xff, rest) -> Just (reverse acc, rest)
        _ -> do
            (step, rest) <- parseStep bs'
            collectSteps (step : acc) rest

splitHashes :: ByteString -> [MPFHash]
splitHashes bs
    | BS.null bs = []
    | otherwise =
        MPFHash (BS.take 32 bs) : splitHashes (BS.drop 32 bs)

takeExact :: Int -> ByteString -> Maybe ByteString
takeExact n bs
    | BS.length bs == n = Just bs
    | otherwise = Nothing

unpackNibblePrefix :: ByteString -> Maybe HexKey
unpackNibblePrefix = fmap concat . traverse unpackByte . BS.unpack
  where
    unpackByte w =
        let hi = w `div` 16
            lo = w `mod` 16
        in  if hi < 16 && lo < 16
                then Just [HexDigit hi, HexDigit lo]
                else Nothing
