{-# LANGUAGE GADTs #-}
{-# LANGUAGE LambdaCase #-}

-- |
-- Module      : Cardano.MPFS.Client.Verify.DSL
-- Description : Tutorial-shaped DSL for verifier-level assertions
--               and forgery scenarios.
--
-- The DSL has three layers, each re-exported from
-- "Cardano.MPFS.Client" so a downstream wallet test suite needs
-- a single import:
--
-- 1. __Assertions.__  'shouldAccept' and 'shouldRejectWith'
--    wrap the offline verifier in hspec 'Expectation's.  On
--    rejection 'shouldRejectWith' takes an 'ErrorMatcher' so
--    the failure message carries the expected dotted field
--    path and reason.
--
-- 2. __Error matchers.__  One smart constructor per
--    'VerifyError' case ('csmtReplayFailedAt',
--    'mpfReplayFailedAt', 'txBindingFailedAt',
--    'malformedHexAt', 'wrongHexLengthAt') plus 'withReason' to narrow the
--    match to a specific reason from the fixed vocabulary in
--    @contracts\/verify-error.md@.
--
-- 3. __Forgery DSL.__  An @operational@ free monad with two
--    program types — 'CsmtForge' for CSMT-layer tampering and
--    'TrieForge' for MPF-layer tampering on
--    'UpdateTxResponse'.  Each program is a sequence of
--    deterministic one-field tamperings; one runner per
--    response type interprets the program against a concrete
--    envelope.
--
-- = Why a free monad
--
-- A forgery is a /value/, not an ad-hoc IO mutation.  Programs
-- can be built, named, shared, and sequenced before meeting a
-- response.  The split into two instruction GADTs
-- ('CsmtForgeI', 'TrieForgeI') means the type checker rejects
-- category mistakes — you cannot accidentally run a trie
-- tampering against a 'BootTxResponse'.  Deterministic
-- byte-level effects mean every scenario is bisect-safe; no
-- @StdGen@ threading.
--
-- = Worked example — CSMT
--
-- > spec = describe "cryptographic CSMT replay at /tx/boot" $ do
-- >     it "accepts an honest response" $
-- >         honestBoot `shouldAccept` verifyBootTxResponse
-- >
-- >     it "rejects a funding proof tampered to random bytes" $
-- >         runForgeBoot (flipProof "funding[0]") honestBoot
-- >             `shouldRejectWith` verifyBootTxResponse
-- >             $ csmtReplayFailedAt
-- >                 "boot.funding[0].utxo_proof"
--
-- The @"funding[0]"@ path is a substring of the dotted field
-- path the verifier reports on — so the forgery site and the
-- expected rejection site line up by eye.  Multi-step programs
-- compose with do-notation:
--
-- > twoSpots :: CsmtForge ()
-- > twoSpots = do
-- >     flipTxOut "state"
-- >     flipProof "requests[0]"
--
-- = Worked example — MPF on an update response
--
-- > it "rejects a trie_read value tampered by one byte" $
-- >     runForgeUpdateTrie (flipTrieValue 0) honestUpdate
-- >         `shouldRejectWith` verifyUpdateTxResponse
-- >         $ mpfReplayFailedAt
-- >             "update.trie_read[0].mpf_proof"
--
-- An 'UpdateTxResponse' is the only envelope that carries an
-- MPF trie; 'runForgeUpdate' handles CSMT tampering, and
-- 'runForgeUpdateTrie' handles MPF tampering — the two layers
-- are distinct program types and do not commute through the
-- same runner.
--
-- = Path grammar
--
-- 'FlipProof' and 'FlipTxOut' take a role path.  The supported
-- shapes differ by endpoint:
--
-- [@BootTxResponse@, @RequestTxResponse@]
--   @"funding[<i>]"@
-- [@RetractTxResponse@]
--   @"request_in"@, @"state_ref"@, @"funding[<i>]"@
-- [@RejectTxResponse@]
--   @"state"@, @"request_ins[<i>]"@, @"funding[<i>]"@
-- [@EndTxResponse@]
--   @"state"@, @"funding[<i>]"@
-- [@UpdateTxResponse@]
--   @"state"@, @"requests[<i>]"@, @"funding[<i>]"@
--
-- 'FlipTrieValue' and 'DropToExclusion' take an index into
-- @UpdateProof.trie_read@; 'FlipTrieRoot' takes no argument.
--
-- Invalid or out-of-range paths are a test-author bug: the
-- runner fails immediately via 'error' rather than silently
-- passing through.
module Cardano.MPFS.Client.Verify.DSL
    ( -- * Assertions
      shouldAccept
    , shouldRejectWith

      -- * Error matchers
    , ErrorMatcher
    , csmtReplayFailedAt
    , mpfReplayFailedAt
    , txBindingFailedAt
    , trustedRootMismatchAt
    , malformedHexAt
    , wrongHexLengthAt
    , withReason

      -- * Forgery helpers
    , flipByteInHex
    , swapHexTo
    , forgeWitnessedUtxoProof
    , forgeWitnessedUtxoTxOut
    , forgeTrieFactValue
    , dropTrieFactToExclusion
    , promoteTrieFactToInclusion

      -- * Forgery DSL (operational free-monad)
    , CsmtForge
    , TrieForge
    , flipProof
    , flipTxOut
    , flipSnapshotRoot
    , flipTrieValue
    , dropToExclusion
    , flipTrieRoot

      -- * DSL runners (per-endpoint)
    , runForgeBoot
    , runForgeRequest
    , runForgeRetract
    , runForgeReject
    , runForgeEnd
    , runForgeUpdate
    , runForgeUpdateTrie
    ) where

import Control.Monad.Operational
    ( Program
    , ProgramViewT (..)
    , singleton
    , view
    )
import Data.Bits (xor)
import Data.ByteString (ByteString)
import Data.ByteString qualified as BS
import Data.ByteString.Base16 qualified as Base16
import Data.Text (Text)
import Data.Text qualified as T
import Data.Text.Encoding qualified as T
import GHC.Stack (HasCallStack)
import Test.Hspec (Expectation, expectationFailure)

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
    , UpdateProof (..)
    , UpdateTxResponse (..)
    , WitnessedUtxo (..)
    )
import Cardano.MPFS.Client.Snapshot
    ( Hex (..)
    , VerificationSnapshot (..)
    )
import Cardano.MPFS.Client.Verify.Replay (VerifyError (..))

-- | A predicate over 'VerifyError' plus a human-readable description
-- used when the assertion fails. Use the smart constructors
-- ('csmtReplayFailedAt' etc.) rather than building values directly;
-- chain with 'withReason' to narrow the match to a specific reason
-- from the fixed vocabulary (see @contracts\/verify-error.md@).
data ErrorMatcher = ErrorMatcher
    { matcherMatches :: VerifyError -> Bool
    , matcherDescribes :: Text
    }

-- | @response \`shouldAccept\` verifier@: assert the verifier
-- returns @Right ()@. On failure, reports the structured
-- 'VerifyError' and a 'show' of the response so the scenario
-- author does not need a debugger.
--
-- Mirrors the Lean preservation theorems — a replayed envelope
-- whose cryptographic checks pass is the Haskell witness of the
-- corresponding @replayWitness@ / @replayTrieFact@ transition.
shouldAccept
    :: (HasCallStack, Show a)
    => a
    -> (a -> Either VerifyError ())
    -> Expectation
shouldAccept response verifier =
    case verifier response of
        Right () -> pure ()
        Left err ->
            expectationFailure
                $ "expected shouldAccept but got Left "
                    <> show err
                    <> "\nresponse: "
                    <> show response

-- | @response \`shouldRejectWith\` verifier $ matcher@: assert the
-- verifier returns @Left err@ where @matcher err@ holds.
-- On failure, renders the expected matcher description and the
-- actual error so the diff reads like:
--
-- > expected  : CsmtReplayFailed "boot.funding[0].utxo_proof" "root mismatch"
-- > but got   : CsmtReplayFailed "boot.funding[0].utxo_proof" "value binding mismatch"
shouldRejectWith
    :: (HasCallStack, Show a)
    => a
    -> (a -> Either VerifyError ())
    -> ErrorMatcher
    -> Expectation
shouldRejectWith response verifier matcher =
    case verifier response of
        Right () ->
            expectationFailure
                $ "expected shouldRejectWith "
                    <> T.unpack (matcherDescribes matcher)
                    <> " but got Right ()"
                    <> "\nresponse: "
                    <> show response
        Left err
            | matcherMatches matcher err -> pure ()
            | otherwise ->
                expectationFailure
                    $ "expected : "
                        <> T.unpack (matcherDescribes matcher)
                        <> "\nbut got  : "
                        <> show err

-- | Match 'CsmtReplayFailed' at the given dotted field path. The
-- reason field is unconstrained — compose with 'withReason' to
-- pin it to a specific value.
csmtReplayFailedAt :: Text -> ErrorMatcher
csmtReplayFailedAt path =
    ErrorMatcher
        { matcherMatches = \case
            CsmtReplayFailed p _ -> p == path
            _ -> False
        , matcherDescribes =
            "CsmtReplayFailed " <> quote path <> " <any reason>"
        }

-- | Match 'MpfReplayFailed' at the given dotted field path.
mpfReplayFailedAt :: Text -> ErrorMatcher
mpfReplayFailedAt path =
    ErrorMatcher
        { matcherMatches = \case
            MpfReplayFailed p _ -> p == path
            _ -> False
        , matcherDescribes =
            "MpfReplayFailed " <> quote path <> " <any reason>"
        }

-- | Match 'TrustedRootMismatch' at the given dotted field
-- path. Emitted by 'verifyUnsignedTxResponse' (and the
-- read-side verifiers, when they land) when
-- @snapshot.utxo_root@ disagrees with the externally-supplied
-- 'TrustedRoot'.
trustedRootMismatchAt :: Text -> ErrorMatcher
trustedRootMismatchAt path =
    ErrorMatcher
        { matcherMatches = \case
            TrustedRootMismatch p -> p == path
            _ -> False
        , matcherDescribes =
            "TrustedRootMismatch " <> quote path
        }

-- | Match 'MalformedHex' at the given dotted field path.
malformedHexAt :: Text -> ErrorMatcher
malformedHexAt path =
    ErrorMatcher
        { matcherMatches = \case
            MalformedHex p _ -> p == path
            _ -> False
        , matcherDescribes =
            "MalformedHex " <> quote path <> " <any value>"
        }

-- | Match 'WrongHexLength' at the given dotted field path.
wrongHexLengthAt :: Text -> ErrorMatcher
wrongHexLengthAt path =
    ErrorMatcher
        { matcherMatches = \case
            WrongHexLength p _ _ -> p == path
            _ -> False
        , matcherDescribes =
            "WrongHexLength " <> quote path <> " <any length>"
        }

-- | Match 'TxBindingFailed' at the given dotted field path.
txBindingFailedAt :: Text -> ErrorMatcher
txBindingFailedAt path =
    ErrorMatcher
        { matcherMatches = \case
            TxBindingFailed p _ -> p == path
            _ -> False
        , matcherDescribes =
            "TxBindingFailed " <> quote path <> " <any reason>"
        }

-- | Narrow an 'ErrorMatcher' to also require a specific reason
-- string from the fixed vocabulary in @contracts\/verify-error.md@
-- (@"root mismatch"@, @"key binding mismatch"@,
-- @"value binding mismatch"@, @"malformed proof CBOR"@,
-- @"inclusion proof for absence claim"@,
-- @"exclusion proof for inclusion claim"@).
--
-- > csmtReplayFailedAt "retract.state_ref.utxo_proof"
-- >   `withReason` "root mismatch"
withReason :: ErrorMatcher -> Text -> ErrorMatcher
withReason base reason =
    ErrorMatcher
        { matcherMatches = \err ->
            matcherMatches base err && reasonOf err == Just reason
        , matcherDescribes =
            matcherDescribes base
                <> " with reason "
                <> quote reason
        }
  where
    reasonOf (CsmtReplayFailed _ r) = Just r
    reasonOf (MpfReplayFailed _ r) = Just r
    reasonOf (TxBindingFailed _ r) = Just r
    reasonOf _ = Nothing

quote :: Text -> Text
quote t = "\"" <> t <> "\""

-- ---------------------------------------------------------------
-- Forgery helpers
-- ---------------------------------------------------------------

-- | Flip the last byte of a 'Hex' value, producing a different
-- but still structurally-valid hex string. Deterministic: given
-- the same input, always produces the same output.
flipByteInHex :: Hex -> Hex
flipByteInHex (Hex txt) =
    case Base16.decode (T.encodeUtf8 txt) of
        Right bs -> Hex (encodeHex (flipTail bs))
        Left _ -> Hex txt
  where
    flipTail bs
        | BS.null bs = BS.singleton 0xff
        | otherwise =
            let (front, back) = BS.splitAt (BS.length bs - 1) bs
                lastByte = BS.head back
                flipped = lastByte `xor` 0xff
            in  front <> BS.singleton flipped

    encodeHex :: ByteString -> Text
    encodeHex = T.decodeUtf8 . Base16.encode

-- | Replace a 'Hex' value with a caller-supplied byte string
-- (re-encoded as hex). Useful for \"wrong root\" scenarios.
swapHexTo :: ByteString -> Hex -> Hex
swapHexTo newBytes _ =
    Hex (T.decodeUtf8 (Base16.encode newBytes))

-- | Flip the last byte of a 'WitnessedUtxo'\'s @utxo_proof@,
-- leaving every other field intact. Models the
-- \"forgingRandomUtxoProofAt\" scenario from
-- @contracts\/dsl.md@.
forgeWitnessedUtxoProof :: WitnessedUtxo -> WitnessedUtxo
forgeWitnessedUtxoProof w =
    w{utxoProof = flipByteInHex (utxoProof w)}

-- | Flip the last byte of a 'WitnessedUtxo'\'s @tx_out@ — the
-- \"tamperingTxOutAt\" scenario. The resulting hash will no
-- longer match @proofValue@, so the replay surfaces
-- 'CsmtReplayFailed' with @"value binding mismatch"@.
forgeWitnessedUtxoTxOut :: WitnessedUtxo -> WitnessedUtxo
forgeWitnessedUtxoTxOut w =
    w{txOut = flipByteInHex (txOut w)}

-- | Flip the last byte of a 'TrieFact'\'s @value@ (when
-- present). No-op for exclusion facts.
forgeTrieFactValue :: TrieFact -> TrieFact
forgeTrieFactValue f = case value f of
    Just v -> f{value = Just (flipByteInHex v)}
    Nothing -> f

-- | Set a 'TrieFact'\'s @value@ to 'Nothing' while leaving the
-- inclusion proof bytes intact. Models
-- \"dropToExclusionAt\": the envelope now claims absence but
-- the proof witnesses presence.
dropTrieFactToExclusion :: TrieFact -> TrieFact
dropTrieFactToExclusion f = f{value = Nothing}

-- | Replace a 'TrieFact'\'s @value@ with 'Just' the supplied
-- bytes while leaving an exclusion-shaped proof intact.
-- Models \"promoteToInclusionAt\": the envelope claims
-- inclusion but the proof witnesses absence.
promoteTrieFactToInclusion :: Hex -> TrieFact -> TrieFact
promoteTrieFactToInclusion v f = f{value = Just v}

-- ---------------------------------------------------------------
-- Forgery DSL (operational free-monad)
-- ---------------------------------------------------------------

-- | CSMT-layer forgery instructions. Each constructor names
-- one deterministic, one-field tampering. A 'CsmtForge' is
-- a 'Program' over these instructions, which the per-endpoint
-- runners ('runForgeBoot' etc.) interpret against a concrete
-- response envelope.
--
-- The 'Text' path on 'FlipProof' \/ 'FlipTxOut' follows the
-- dotted field grammar that 'VerifyError' already reports
-- on:
--
-- > "funding[<i>]"    -- any list-of-funding endpoint
-- > "state"           -- end, reject, update
-- > "state_ref"       -- retract
-- > "request_in"      -- retract
-- > "request_ins[<i>]"-- reject
-- > "requests[<i>]"   -- update
--
-- Invalid or out-of-range paths are a test-author bug: the
-- runner fails immediately via 'error'.
data CsmtForgeI a where
    -- | Flip the last byte of @<path>.utxo_proof@.
    FlipProof :: Text -> CsmtForgeI ()
    -- | Flip the last byte of @<path>.tx_out@.
    FlipTxOut :: Text -> CsmtForgeI ()
    -- | Swap the advertised @snapshot.utxo_root@ with a
    -- deterministic 32-byte \"wrong root\". Every witness
    -- in the response now replays against the wrong root.
    FlipSnapshotRoot :: CsmtForgeI ()

-- | A program of CSMT-layer forgeries.
type CsmtForge = Program CsmtForgeI

-- | MPF-layer forgery instructions, valid only against an
-- 'UpdateTxResponse'. A 'TrieForge' is a 'Program' over
-- these instructions.
data TrieForgeI a where
    -- | Flip the last byte of @trie_read[i].value@.
    FlipTrieValue :: Int -> TrieForgeI ()
    -- | Drop @trie_read[i].value@ to 'Nothing' while
    -- leaving the inclusion proof bytes intact.
    DropToExclusion :: Int -> TrieForgeI ()
    -- | Flip the last byte of the advertised @trie_root@.
    FlipTrieRoot :: TrieForgeI ()

type TrieForge = Program TrieForgeI

-- | CSMT smart constructors — each lifts a single
-- instruction into the 'CsmtForge' monad.
flipProof :: Text -> CsmtForge ()
flipProof = singleton . FlipProof

flipTxOut :: Text -> CsmtForge ()
flipTxOut = singleton . FlipTxOut

flipSnapshotRoot :: CsmtForge ()
flipSnapshotRoot = singleton FlipSnapshotRoot

-- | Trie smart constructors.
flipTrieValue :: Int -> TrieForge ()
flipTrieValue = singleton . FlipTrieValue

dropToExclusion :: Int -> TrieForge ()
dropToExclusion = singleton . DropToExclusion

flipTrieRoot :: TrieForge ()
flipTrieRoot = singleton FlipTrieRoot

-- ---------------------------------------------------------------
-- Path parsing
-- ---------------------------------------------------------------

-- | Parse a role with an optional index: @"funding[3]"@ →
-- @(\"funding\", Just 3)@, @"state_ref"@ →
-- @(\"state_ref\", Nothing)@.
parseIndexedRole :: Text -> (Text, Maybe Int)
parseIndexedRole raw = case T.breakOn "[" raw of
    (role, "") -> (role, Nothing)
    (role, idxBits) ->
        let body = T.drop 1 (T.dropEnd 1 idxBits)
        in  (role, readMaybeInt body)

readMaybeInt :: Text -> Maybe Int
readMaybeInt t = case reads (T.unpack t) of
    [(n, "")] -> Just n
    _ -> Nothing

-- | Apply a forgery to the @i@-th element of a list. Errors
-- out if @i@ is out of range — a test fixture that names a
-- non-existent index is a test bug.
tamperListAt :: (HasCallStack) => Int -> (a -> a) -> [a] -> [a]
tamperListAt i f = go (0 :: Int)
  where
    go _ [] =
        error
            $ "tamperListAt: index "
                <> show i
                <> " out of range"
    go k (x : xs)
        | k == i = f x : xs
        | otherwise = x : go (k + 1) xs

-- | Wrong-root bytes injected by 'FlipSnapshotRoot'.
wrongRootBytes :: ByteString
wrongRootBytes = BS.replicate 32 0xff

-- | Replace a snapshot's @utxo_root@ with 'wrongRootBytes'.
swapSnapRoot :: VerificationSnapshot -> VerificationSnapshot
swapSnapRoot s = s{utxoRoot = swapHexTo wrongRootBytes (utxoRoot s)}

-- ---------------------------------------------------------------
-- Per-endpoint CSMT runners
-- ---------------------------------------------------------------

-- | Interpret a 'CsmtForge' program against a 'BootTxResponse'.
runForgeBoot :: CsmtForge () -> BootTxResponse -> BootTxResponse
runForgeBoot prog r = case view prog of
    Return () -> r
    FlipSnapshotRoot :>>= k ->
        let BootTxResponse tx sn p = r
        in  runForgeBoot (k ()) (BootTxResponse tx (swapSnapRoot sn) p)
    FlipProof path :>>= k ->
        runForgeBoot (k ()) (applyToBootFunding path flipUtxoProof r)
    FlipTxOut path :>>= k ->
        runForgeBoot (k ()) (applyToBootFunding path flipTxOutField r)

applyToBootFunding
    :: Text
    -> (WitnessedUtxo -> WitnessedUtxo)
    -> BootTxResponse
    -> BootTxResponse
applyToBootFunding path f (BootTxResponse tx sn (BootProof fs)) =
    BootTxResponse tx sn (BootProof (tamperFundingList path f fs))

-- | Interpret a 'CsmtForge' against a 'RequestTxResponse'.
runForgeRequest
    :: CsmtForge () -> RequestTxResponse -> RequestTxResponse
runForgeRequest prog r = case view prog of
    Return () -> r
    FlipSnapshotRoot :>>= k ->
        let RequestTxResponse tx sn p = r
        in  runForgeRequest
                (k ())
                (RequestTxResponse tx (swapSnapRoot sn) p)
    FlipProof path :>>= k ->
        runForgeRequest
            (k ())
            (applyToRequestFunding path flipUtxoProof r)
    FlipTxOut path :>>= k ->
        runForgeRequest
            (k ())
            (applyToRequestFunding path flipTxOutField r)

applyToRequestFunding
    :: Text
    -> (WitnessedUtxo -> WitnessedUtxo)
    -> RequestTxResponse
    -> RequestTxResponse
applyToRequestFunding path f (RequestTxResponse tx sn (RequestProof fs)) =
    RequestTxResponse
        tx
        sn
        (RequestProof (tamperFundingList path f fs))

-- | Interpret a 'CsmtForge' against a 'RetractTxResponse'.
runForgeRetract
    :: CsmtForge () -> RetractTxResponse -> RetractTxResponse
runForgeRetract prog r = case view prog of
    Return () -> r
    FlipSnapshotRoot :>>= k ->
        let RetractTxResponse tx sn p = r
        in  runForgeRetract
                (k ())
                (RetractTxResponse tx (swapSnapRoot sn) p)
    FlipProof path :>>= k ->
        runForgeRetract (k ()) (tamperRetract path flipUtxoProof r)
    FlipTxOut path :>>= k ->
        runForgeRetract (k ()) (tamperRetract path flipTxOutField r)

tamperRetract
    :: (HasCallStack)
    => Text
    -> (WitnessedUtxo -> WitnessedUtxo)
    -> RetractTxResponse
    -> RetractTxResponse
tamperRetract path f (RetractTxResponse tx sn (RetractProof ri sr fs)) =
    case parseIndexedRole path of
        ("request_in", Nothing) ->
            RetractTxResponse tx sn (RetractProof (f ri) sr fs)
        ("state_ref", Nothing) ->
            RetractTxResponse tx sn (RetractProof ri (f sr) fs)
        ("funding", Just i) ->
            RetractTxResponse
                tx
                sn
                (RetractProof ri sr (tamperListAt i f fs))
        _ ->
            error $ "runForgeRetract: bad path " <> show path

-- | Interpret a 'CsmtForge' against a 'RejectTxResponse'.
runForgeReject
    :: CsmtForge () -> RejectTxResponse -> RejectTxResponse
runForgeReject prog r = case view prog of
    Return () -> r
    FlipSnapshotRoot :>>= k ->
        let RejectTxResponse tx sn p = r
        in  runForgeReject
                (k ())
                (RejectTxResponse tx (swapSnapRoot sn) p)
    FlipProof path :>>= k ->
        runForgeReject (k ()) (tamperReject path flipUtxoProof r)
    FlipTxOut path :>>= k ->
        runForgeReject (k ()) (tamperReject path flipTxOutField r)

tamperReject
    :: (HasCallStack)
    => Text
    -> (WitnessedUtxo -> WitnessedUtxo)
    -> RejectTxResponse
    -> RejectTxResponse
tamperReject path f (RejectTxResponse tx sn (RejectProof st ris fs)) =
    case parseIndexedRole path of
        ("state", Nothing) ->
            RejectTxResponse tx sn (RejectProof (f st) ris fs)
        ("request_ins", Just i) ->
            RejectTxResponse
                tx
                sn
                (RejectProof st (tamperListAt i f ris) fs)
        ("funding", Just i) ->
            RejectTxResponse
                tx
                sn
                (RejectProof st ris (tamperListAt i f fs))
        _ ->
            error $ "runForgeReject: bad path " <> show path

-- | Interpret a 'CsmtForge' against an 'EndTxResponse'.
runForgeEnd :: CsmtForge () -> EndTxResponse -> EndTxResponse
runForgeEnd prog r = case view prog of
    Return () -> r
    FlipSnapshotRoot :>>= k ->
        let EndTxResponse tx sn p = r
        in  runForgeEnd
                (k ())
                (EndTxResponse tx (swapSnapRoot sn) p)
    FlipProof path :>>= k ->
        runForgeEnd (k ()) (tamperEnd path flipUtxoProof r)
    FlipTxOut path :>>= k ->
        runForgeEnd (k ()) (tamperEnd path flipTxOutField r)

tamperEnd
    :: (HasCallStack)
    => Text
    -> (WitnessedUtxo -> WitnessedUtxo)
    -> EndTxResponse
    -> EndTxResponse
tamperEnd path f (EndTxResponse tx sn (EndProof st fs)) =
    case parseIndexedRole path of
        ("state", Nothing) ->
            EndTxResponse tx sn (EndProof (f st) fs)
        ("funding", Just i) ->
            EndTxResponse
                tx
                sn
                (EndProof st (tamperListAt i f fs))
        _ ->
            error $ "runForgeEnd: bad path " <> show path

-- | Interpret a 'CsmtForge' against an 'UpdateTxResponse'.
runForgeUpdate
    :: CsmtForge () -> UpdateTxResponse -> UpdateTxResponse
runForgeUpdate prog r = case view prog of
    Return () -> r
    FlipSnapshotRoot :>>= k ->
        let UpdateTxResponse tx sn p = r
        in  runForgeUpdate
                (k ())
                (UpdateTxResponse tx (swapSnapRoot sn) p)
    FlipProof path :>>= k ->
        runForgeUpdate (k ()) (tamperUpdate path flipUtxoProof r)
    FlipTxOut path :>>= k ->
        runForgeUpdate (k ()) (tamperUpdate path flipTxOutField r)

tamperUpdate
    :: (HasCallStack)
    => Text
    -> (WitnessedUtxo -> WitnessedUtxo)
    -> UpdateTxResponse
    -> UpdateTxResponse
tamperUpdate
    path
    f
    (UpdateTxResponse tx sn (UpdateProof st rs fs tr tread)) =
        case parseIndexedRole path of
            ("state", Nothing) ->
                UpdateTxResponse
                    tx
                    sn
                    (UpdateProof (f st) rs fs tr tread)
            ("requests", Just i) ->
                UpdateTxResponse
                    tx
                    sn
                    ( UpdateProof
                        st
                        (tamperListAt i f rs)
                        fs
                        tr
                        tread
                    )
            ("funding", Just i) ->
                UpdateTxResponse
                    tx
                    sn
                    ( UpdateProof
                        st
                        rs
                        (tamperListAt i f fs)
                        tr
                        tread
                    )
            _ ->
                error $ "runForgeUpdate: bad path " <> show path

-- | Interpret a 'TrieForge' against an 'UpdateTxResponse'.
runForgeUpdateTrie
    :: TrieForge () -> UpdateTxResponse -> UpdateTxResponse
runForgeUpdateTrie prog r = case view prog of
    Return () -> r
    FlipTrieValue i :>>= k ->
        runForgeUpdateTrie
            (k ())
            (tamperTrie i forgeTrieFactValue r)
    DropToExclusion i :>>= k ->
        runForgeUpdateTrie
            (k ())
            (tamperTrie i dropTrieFactToExclusion r)
    FlipTrieRoot :>>= k ->
        let UpdateTxResponse tx sn (UpdateProof st rs fs tr tread) = r
        in  runForgeUpdateTrie
                (k ())
                ( UpdateTxResponse
                    tx
                    sn
                    ( UpdateProof
                        st
                        rs
                        fs
                        (flipByteInHex tr)
                        tread
                    )
                )

tamperTrie
    :: Int
    -> (TrieFact -> TrieFact)
    -> UpdateTxResponse
    -> UpdateTxResponse
tamperTrie
    i
    f
    (UpdateTxResponse tx sn (UpdateProof st rs fs tr tread)) =
        UpdateTxResponse
            tx
            sn
            (UpdateProof st rs fs tr (tamperListAt i f tread))

-- ---------------------------------------------------------------
-- Shared CSMT field helpers
-- ---------------------------------------------------------------

-- | Apply a 'WitnessedUtxo' tamper to the @i@-th entry of a
-- funding list, where @path@ is @"funding[<i>]"@.
tamperFundingList
    :: (HasCallStack)
    => Text
    -> (WitnessedUtxo -> WitnessedUtxo)
    -> [WitnessedUtxo]
    -> [WitnessedUtxo]
tamperFundingList path f fs = case parseIndexedRole path of
    ("funding", Just i) -> tamperListAt i f fs
    _ ->
        error
            $ "tamperFundingList: bad path "
                <> show path

-- | Alias for 'forgeWitnessedUtxoProof' scoped to the DSL.
flipUtxoProof :: WitnessedUtxo -> WitnessedUtxo
flipUtxoProof = forgeWitnessedUtxoProof

-- | Alias for 'forgeWitnessedUtxoTxOut' scoped to the DSL.
flipTxOutField :: WitnessedUtxo -> WitnessedUtxo
flipTxOutField = forgeWitnessedUtxoTxOut
