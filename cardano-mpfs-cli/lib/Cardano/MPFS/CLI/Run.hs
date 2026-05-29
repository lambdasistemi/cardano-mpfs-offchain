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
import Cardano.MPFS.CLI.Cage (ownerAddressBytes, resolveCageConfig)
import Cardano.MPFS.CLI.Hex
    ( decodeHexText
    , encodeHexText
    , hexArgText
    , hexBytes
    )
import Cardano.MPFS.CLI.Key (loadSigningKey)
import Cardano.MPFS.CLI.Options (Command (..))
import Cardano.MPFS.CLI.Output (die, emitJson, emitResult, logErr)
import Cardano.MPFS.CLI.Sign (signTx)
import Cardano.MPFS.CLI.Submit
    ( awaitTx
    , getFact
    , listTokens
    , mkServerEnv
    , mkServerParts
    , submitSignedTx
    )
import Cardano.MPFS.CLI.Workflow
    ( defaultWalletPolicy
    , mkHttpClient
    , resolveTrustedRoot
    )
import Cardano.MPFS.Client.Cage.Config (network)
import Cardano.MPFS.Workflows
    ( BootRequest (..)
    , UnsignedTx (..)
    , WorkflowsConfig (..)
    , registerToken
    )
import Data.Aeson (ToJSON, Value, object, (.=))
import Data.Aeson.Types (Pair)
import Data.Word (Word64)
import Servant.Client (ClientEnv, ClientError, mkClientEnv)

-- | Dispatch a parsed command.
run :: Command -> IO ()
run cmd = case cmd of
    RegisterToken{..} -> do
        (manager, base) <- mkServerParts server >>= orDie "server"
        sk <- loadSigningKey ownerKey >>= orDieShow "owner-key"
        cage <- resolveCageConfig cageConfig >>= orDie "cage-config"
        troot <-
            resolveTrustedRoot manager base (hexBytes <$> trustedRoot)
                >>= orDie "trusted-root"
        let config =
                WorkflowsConfig
                    { wcCage = cage
                    , wcPolicy = defaultWalletPolicy
                    , wcTrustedRoot = troot
                    }
            httpClient = mkHttpClient manager base
            request =
                BootRequest (Hex (ownerAddressBytes (network cage) sk))
        UnsignedTx cbor <-
            registerToken httpClient config request >>= orDieShow "workflow"
        signed <- orDieShow "sign" (signTx sk cbor)
        let env = mkClientEnv manager base
        txId <- submitSignedTx env signed >>= orDieShow "submit"
        _ <- awaitTx env txId defaultAwaitTimeout >>= orDieShow "await"
        emitJson
            ( object
                [ "command" .= ("register-token" :: String)
                , "status" .= ("submitted" :: String)
                , "txId" .= encodeHexText txId
                ]
            )
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

-- | Await timeout (seconds) for a submitted tx to be indexed.
defaultAwaitTimeout :: Word64
defaultAwaitTimeout = 60

-- | Unwrap an 'Either String' or exit with a contextual error.
orDie :: String -> Either String a -> IO a
orDie ctx = either (\e -> die (ctx <> ": " <> e)) pure

-- | Unwrap an 'Either' whose error only has a 'Show', or exit.
orDieShow :: Show e => String -> Either e a -> IO a
orDieShow ctx = either (\e -> die (ctx <> ": " <> show e)) pure

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
