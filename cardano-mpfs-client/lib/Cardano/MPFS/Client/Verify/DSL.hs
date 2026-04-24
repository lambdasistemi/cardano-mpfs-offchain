{-# LANGUAGE LambdaCase #-}

-- |
-- Module      : Cardano.MPFS.Client.Verify.DSL
-- Description : Tutorial-shaped DSL for verifier-level assertions.
--
-- Re-exported from "Cardano.MPFS.Client" so a single import exposes
-- both the verifier and the test DSL. The DSL pairs positive
-- ('shouldAccept') and negative ('shouldRejectWith') scenarios with
-- structured 'ErrorMatcher's so the E2E spec reads as tutorial prose
-- — every scenario names the endpoint, the expected outcome, and
-- (on the negative side) the exact 'VerifyError' constructor and
-- dotted field path that must fire.
--
-- The DSL is deliberately small: the two assertions plus an
-- 'ErrorMatcher' builder per 'VerifyError' constructor plus a
-- 'withReason' combinator for matching on the reason text.
--
-- __Scenario template__ (see @specs\/178-crypto-proof-replay\/contracts\/dsl.md@):
--
-- > spec :: Spec
-- > spec = describe "cryptographic CSMT replay at /tx/boot" $ do
-- >     it "accepts an honest response" $ do
-- >         response <- server `postsBoot` ownerAddress
-- >         response `shouldAccept` verifyBootTxResponse
-- >
-- >     it "rejects a funding proof tampered to random bytes" $ do
-- >         response <- server `postsBoot` ownerAddress
-- >         forged   <- response
-- >                       `forgingRandomUtxoProofAt` "boot.funding[0]"
-- >         forged `shouldRejectWith` verifyBootTxResponse
-- >             $ csmtReplayFailedAt "boot.funding[0].utxo_proof"
module Cardano.MPFS.Client.Verify.DSL
    ( -- * Assertions
      shouldAccept
    , shouldRejectWith

      -- * Error matchers
    , ErrorMatcher
    , csmtReplayFailedAt
    , mpfReplayFailedAt
    , malformedHexAt
    , wrongHexLengthAt
    , withReason
    ) where

import Data.Text (Text)
import Data.Text qualified as T
import GHC.Stack (HasCallStack)
import Test.Hspec (Expectation, expectationFailure)

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
    reasonOf _ = Nothing

quote :: Text -> Text
quote t = "\"" <> t <> "\""
