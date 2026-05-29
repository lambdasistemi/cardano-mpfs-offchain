-- |
-- Module      : Cardano.MPFS.CLI.Run
-- Description : Subcommand handlers.
--
-- Each handler turns a parsed 'Command' into an action. Until
-- cardano-mpfs-workflows (#289) publishes its function surface, the
-- write handlers are stubs that emit, as JSON, the workflow call they
-- *would* make. Read-only handlers are likewise stubbed here and wired
-- to the real read endpoints in a later slice (S3). The stdout/stderr
-- contract from "Cardano.MPFS.CLI.Output" already holds.
module Cardano.MPFS.CLI.Run
    ( run
    ) where

import Cardano.MPFS.API.Encoding (Hex (..))
import Cardano.MPFS.API.Types (TokenIdJSON (..))
import Cardano.MPFS.CLI.Hex (decodeHexText, hexArgText, hexBytes)
import Cardano.MPFS.CLI.Options (Command (..))
import Cardano.MPFS.CLI.Output (die, emitJson, emitResult, logErr)
import Cardano.MPFS.CLI.Submit (getFact, listTokens, mkServerEnv)
import Data.Aeson (ToJSON, Value, object, (.=))
import Data.Aeson.Types (Pair)
import Servant.Client (ClientEnv, ClientError)

-- | Dispatch a parsed command.
run :: Command -> IO ()
run cmd = case cmd of
    RegisterToken{..} ->
        writeStub
            "register-token"
            "Workflows.registerToken"
            [ "server" .= server
            , "ownerKey" .= ownerKey
            , "cageConfig" .= cageConfig
            ]
    FactInsert{..} ->
        writeStub
            "fact insert"
            "Workflows.insertFact"
            [ "server" .= server
            , "token" .= token
            , "key" .= hexArgText key
            , "value" .= hexArgText value
            , "ownerKey" .= ownerKey
            ]
    FactUpdate{..} ->
        writeStub
            "fact update"
            "Workflows.updateFact"
            [ "server" .= server
            , "token" .= token
            , "key" .= hexArgText key
            , "oldValue" .= hexArgText oldValue
            , "newValue" .= hexArgText newValue
            , "ownerKey" .= ownerKey
            ]
    FactDelete{..} ->
        writeStub
            "fact delete"
            "Workflows.deleteFact"
            [ "server" .= server
            , "token" .= token
            , "key" .= hexArgText key
            , "ownerKey" .= ownerKey
            ]
    FactRetract{..} ->
        writeStub
            "fact retract"
            "Workflows.retractRequest"
            [ "server" .= server
            , "token" .= token
            , "requestId" .= requestId
            , "ownerKey" .= ownerKey
            ]
    FactReject{..} ->
        writeStub
            "fact reject"
            "Workflows.rejectExpired"
            [ "server" .= server
            , "token" .= token
            , "ownerKey" .= ownerKey
            ]
    TokenEnd{..} ->
        writeStub
            "token end"
            "Workflows.endCage"
            [ "server" .= server
            , "token" .= token
            , "ownerKey" .= ownerKey
            ]
    FactGet{..} ->
        withEnv server $ \env ->
            case decodeHexText token of
                Left e -> die ("invalid --token hex: " <> e)
                Right tokenBytes -> do
                    res <-
                        getFact
                            env
                            (TokenIdJSON tokenBytes)
                            (Hex (hexBytes key))
                    emitOrDie "fact get" res
    TokenList{..} ->
        withEnv server $ \env -> do
            res <- listTokens env
            emitOrDie "token list" res

-- | Emit the stub envelope for a write subcommand and log to stderr.
writeStub :: String -> String -> [Pair] -> IO ()
writeStub name workflow args = do
    logErr
        $ name
            <> ": would call "
            <> workflow
            <> " (cardano-mpfs-workflows #289 not yet wired)"
    emitJson (stubEnvelope name (Just workflow) args)

-- | Resolve a server env or exit, then run the action against it.
withEnv :: String -> (ClientEnv -> IO ()) -> IO ()
withEnv server k = do
    eEnv <- mkServerEnv server
    case eEnv of
        Left err -> die err
        Right env -> k env

-- | Emit a successful client result as JSON, or exit on a client error.
emitOrDie :: ToJSON a => String -> Either ClientError a -> IO ()
emitOrDie name =
    either (\e -> die (name <> " failed: " <> show e)) emitResult

stubEnvelope :: String -> Maybe String -> [Pair] -> Value
stubEnvelope name mWorkflow args =
    object
        [ "command" .= name
        , "status" .= ("stub" :: String)
        , "workflow" .= mWorkflow
        , "args" .= object args
        ]
