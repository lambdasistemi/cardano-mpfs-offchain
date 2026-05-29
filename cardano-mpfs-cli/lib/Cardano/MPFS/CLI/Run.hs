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

import Cardano.MPFS.CLI.Hex (hexArgText)
import Cardano.MPFS.CLI.Options (Command (..))
import Cardano.MPFS.CLI.Output (emitJson, logErr)
import Data.Aeson (Value, object, (.=))
import Data.Aeson.Types (Pair)

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
        readStub
            "fact get"
            "GET /tokens/:id/facts/:key"
            [ "server" .= server
            , "token" .= token
            , "key" .= hexArgText key
            ]
    TokenList{..} ->
        readStub
            "token list"
            "GET /tokens"
            ["server" .= server]

-- | Emit the stub envelope for a write subcommand and log to stderr.
writeStub :: String -> String -> [Pair] -> IO ()
writeStub name workflow args = do
    logErr
        $ name
            <> ": would call "
            <> workflow
            <> " (cardano-mpfs-workflows #289 not yet wired)"
    emitJson (stubEnvelope name (Just workflow) args)

-- | Emit the stub envelope for a read-only subcommand and log to stderr.
readStub :: String -> String -> [Pair] -> IO ()
readStub name endpoint args = do
    logErr
        $ name
            <> ": would call "
            <> endpoint
            <> " (read endpoint wiring lands in slice S3)"
    emitJson (stubEnvelope name Nothing (("endpoint" .= endpoint) : args))

stubEnvelope :: String -> Maybe String -> [Pair] -> Value
stubEnvelope name mWorkflow args =
    object
        [ "command" .= name
        , "status" .= ("stub" :: String)
        , "workflow" .= mWorkflow
        , "args" .= object args
        ]
