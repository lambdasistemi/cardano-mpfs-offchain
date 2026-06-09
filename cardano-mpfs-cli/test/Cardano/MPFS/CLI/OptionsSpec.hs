-- |
-- Module      : Cardano.MPFS.CLI.OptionsSpec
-- Description : Command-line parser tests.
module Cardano.MPFS.CLI.OptionsSpec
    ( spec
    ) where

import Cardano.MPFS.CLI.Hex (HexArg (..))
import Cardano.MPFS.CLI.Options
    ( App (..)
    , Command (..)
    , OutputFormat (..)
    , commandInfo
    )
import Options.Applicative
    ( ParserResult (..)
    , defaultPrefs
    , execParserPure
    , renderFailure
    )
import Test.Hspec

spec :: Spec
spec =
    describe "commandInfo" $ do
        it "parses token process with selected request ids and local --json"
            $ case parseArgs
                [ "token"
                , "process"
                , "cafe"
                , "--request-id"
                , "abc#0"
                , "--json"
                ] of
                Right
                    App
                        { appOutput = OutputJson
                        , appCommand =
                            TokenProcess
                                { token = parsedToken
                                , requestIds = parsedRequests
                                }
                        } -> do
                        parsedToken `shouldBe` "cafe"
                        parsedRequests `shouldBe` ["abc#0"]
                other ->
                    expectationFailure
                        ("unexpected parse result: " <> show other)
        it "parses register-token devnet timing overrides"
            $ case parseArgs
                [ "register-token"
                , "--process-time-ms"
                , "5000"
                , "--retract-time-ms"
                , "5000"
                ] of
                Right
                    App
                        { appCommand =
                            RegisterToken
                                { processTimeMs = parsedProcess
                                , retractTimeMs = parsedRetract
                                }
                        } -> do
                        parsedProcess `shouldBe` Just 5000
                        parsedRetract `shouldBe` Just 5000
                other ->
                    expectationFailure
                        ("unexpected parse result: " <> show other)
        it "parses global --json before a read command"
            $ case parseArgs ["--json", "requests", "list", "cafe"] of
                Right
                    App
                        { appOutput = OutputJson
                        , appCommand = RequestsList{token = parsedToken}
                        } ->
                        parsedToken `shouldBe` "cafe"
                other ->
                    expectationFailure
                        ("unexpected parse result: " <> show other)
        it "parses positional fact get"
            $ case parseArgs ["fact", "get", "cafe", "00"] of
                Right
                    App
                        { appCommand =
                            FactGet
                                { token = parsedToken
                                , key = HexArg parsedKey
                                }
                        } -> do
                        parsedToken `shouldBe` "cafe"
                        parsedKey `shouldBe` "\NUL"
                other ->
                    expectationFailure
                        ("unexpected parse result: " <> show other)
        it "keeps legacy --token/--key fact get working"
            $ case parseArgs ["fact", "get", "--token", "cafe", "--key", "00"] of
                Right
                    App
                        { appCommand =
                            FactGet
                                { token = parsedToken
                                , key = HexArg parsedKey
                                }
                        } -> do
                        parsedToken `shouldBe` "cafe"
                        parsedKey `shouldBe` "\NUL"
                other ->
                    expectationFailure
                        ("unexpected parse result: " <> show other)

parseArgs :: [String] -> Either String App
parseArgs args =
    case execParserPure defaultPrefs commandInfo args of
        Success app -> Right app
        Failure failure ->
            Left (fst (renderFailure failure "mpfs-cli"))
        CompletionInvoked _ ->
            Left "completion invoked"
