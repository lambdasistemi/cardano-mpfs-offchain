-- | Unit test entry point. The SPA's real assurances are the type checker
-- | and the manual browser smoke; this suite exists so `spago test` (and
-- | therefore `just ci`) has a target and guards the build wiring.
module Test.Main where

import Prelude

import Effect (Effect)
import Effect.Aff (launchAff_)
import Test.Spec (describe, it)
import Test.Spec.Assertions (shouldEqual)
import Test.Spec.Reporter.Console (consoleReporter)
import Test.Spec.Runner (runSpec)

main :: Effect Unit
main = launchAff_ $ runSpec [ consoleReporter ] do
  describe "mpfs-spa" do
    it "builds and runs its test harness" do
      (1 + 1) `shouldEqual` 2
