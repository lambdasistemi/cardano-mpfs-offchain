-- | Unit test entry point. The SPA's real assurances are the type checker
-- | and the manual browser smoke; this suite exists so `spago test` (and
-- | therefore `just ci`) has a target and guards the build wiring.
module Test.Main where

import Prelude

import Data.Maybe (Maybe(..))
import Effect (Effect)
import Effect.Aff (launchAff_)
import MpfsSpa.Http (PendingRequest)
import MpfsSpa.Tab.Facts (isProcessable)
import MpfsSpa.Types (Key(..), RequestId(..), TokenId(..))
import Test.Spec (describe, it)
import Test.Spec.Assertions (shouldEqual)
import Test.Spec.Reporter.Console (consoleReporter)
import Test.Spec.Runner (runSpec)

main :: Effect Unit
main = launchAff_ $ runSpec [ consoleReporter ] do
  describe "mpfs-spa" do
    it "builds and runs its test harness" do
      (1 + 1) `shouldEqual` 2

    it "marks requests processable only within the process window" do
      isProcessable 1000.0 1800.0 (pendingAt 0.0) `shouldEqual` true
      isProcessable 1800.0 1800.0 (pendingAt 0.0) `shouldEqual` true
      isProcessable 2000.0 1800.0 (pendingAt 0.0) `shouldEqual` false

pendingAt :: Number -> PendingRequest
pendingAt submittedAt =
  { token: TokenId "token"
  , owner: "owner"
  , key: Key "key"
  , value: Nothing
  , operation: "insert"
  , fee: 0.0
  , submittedAt
  , requestId: RequestId "tx#0"
  }
