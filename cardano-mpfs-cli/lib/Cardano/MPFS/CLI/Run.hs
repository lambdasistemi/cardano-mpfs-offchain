-- |
-- Module      : Cardano.MPFS.CLI.Run
-- Description : Subcommand handlers.
--
-- Each handler turns a parsed 'Command' into an action. Write commands
-- run the matching @cardano-mpfs-workflows@ function (fetch facts →
-- verify → build the unsigned tx), sign locally with the Bech32 key,
-- POST to @\/submit@, await the tx, and print a JSON result. Read-only
-- commands hit the read endpoints directly. The stdout/stderr contract
-- lives in "Cardano.MPFS.CLI.Output": JSON on stdout, logs on stderr.
module Cardano.MPFS.CLI.Run
    ( run
    ) where

import Cardano.MPFS.API.Encoding (Hex (..))
import Cardano.MPFS.API.Types (TokenIdJSON (..))
import Cardano.MPFS.CLI.Cage (ownerAddressBytes, resolveCageConfig)
import Cardano.MPFS.CLI.Hex
    ( HexArg
    , decodeHexText
    , encodeHexText
    , hexBytes
    )
import Cardano.MPFS.CLI.Key (loadSigningKey)
import Cardano.MPFS.CLI.Options (Command (..))
import Cardano.MPFS.CLI.Output (die, emitJson, emitResult)
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
    , resolveEvalContext
    , resolveTrustedRoot
    )
import Cardano.MPFS.Client.Cage.Config (network)
import Cardano.MPFS.Workflows
    ( BootRequest (..)
    , DeleteRequest (..)
    , EndRequest (..)
    , HttpClient
    , InsertRequest (..)
    , RejectRequest (..)
    , RetractRequest (..)
    , UnsignedTx (..)
    , UpdateValueRequest (..)
    , WorkflowError
    , WorkflowsConfig (..)
    , deleteFact
    , endCage
    , insertFact
    , registerToken
    , rejectExpired
    , retractRequest
    , updateFact
    )
import Data.Aeson (ToJSON, object, (.=))
import Data.Text (Text)
import Data.Word (Word64)
import Servant.Client (ClientEnv, ClientError, mkClientEnv)

-- | Dispatch a parsed command.
run :: Command -> IO ()
run cmd = case cmd of
    RegisterToken{..} ->
        runWrite
            "register-token"
            server
            ownerKey
            cageConfig
            trustedRoot
            BootRequest
            registerToken
    FactInsert{..} -> do
        tid <- decodeToken token
        runWrite
            "fact insert"
            server
            ownerKey
            cageConfig
            trustedRoot
            (InsertRequest tid (Hex (hexBytes key)) (Hex (hexBytes value)))
            insertFact
    FactUpdate{..} -> do
        tid <- decodeToken token
        runWrite
            "fact update"
            server
            ownerKey
            cageConfig
            trustedRoot
            ( UpdateValueRequest
                tid
                (Hex (hexBytes key))
                (Hex (hexBytes oldValue))
                (Hex (hexBytes newValue))
            )
            updateFact
    FactDelete{..} -> do
        tid <- decodeToken token
        runWrite
            "fact delete"
            server
            ownerKey
            cageConfig
            trustedRoot
            (DeleteRequest tid (Hex (hexBytes key)) (Hex (hexBytes value)))
            deleteFact
    FactRetract{..} ->
        runWrite
            "fact retract"
            server
            ownerKey
            cageConfig
            trustedRoot
            (RetractRequest requestId)
            retractRequest
    FactReject{..} -> do
        tid <- decodeToken token
        runWrite
            "fact reject"
            server
            ownerKey
            cageConfig
            trustedRoot
            ( \addr ->
                RejectRequest
                    { rejToken = tid
                    , rejAddr = addr
                    , rejRequests = []
                    }
            )
            rejectExpired
    TokenEnd{..} -> do
        tid <- decodeToken token
        runWrite
            "token end"
            server
            ownerKey
            cageConfig
            trustedRoot
            (EndRequest tid)
            endCage
    FactGet{..} ->
        withEnv server $ \env -> do
            tid <- decodeToken token
            res <- getFact env tid (Hex (hexBytes key))
            emitOrDie "fact get" res
    TokenList{..} ->
        withEnv server $ \env -> do
            res <- listTokens env
            emitOrDie "token list" res

-- | The full write pipeline shared by every write subcommand: resolve
-- the connection, key, cage config, and trusted root; run the workflow
-- to get an unsigned tx; sign locally; submit; await; print the txId.
runWrite
    :: String
    -- ^ Command name (for output + error context).
    -> String
    -- ^ Server base URL.
    -> FilePath
    -- ^ Bech32 owner key path.
    -> Maybe FilePath
    -- ^ Cage blueprint path (else @MPFS_BLUEPRINT@).
    -> Maybe HexArg
    -- ^ Trusted-root override (else server @\/status@).
    -> (Hex -> req)
    -- ^ Build the request from the owner address.
    -> ( HttpClient
         -> WorkflowsConfig
         -> req
         -> IO (Either WorkflowError UnsignedTx)
       )
    -- ^ The workflow to run.
    -> IO ()
runWrite name server ownerKey cageConfig trustedRoot mkReq workflow = do
    (manager, base) <- mkServerParts server >>= orDie "server"
    sk <- loadSigningKey ownerKey >>= orDieShow "owner-key"
    cage <- resolveCageConfig cageConfig >>= orDie "cage-config"
    troot <-
        resolveTrustedRoot manager base (hexBytes <$> trustedRoot)
            >>= orDie "trusted-root"
    evalCtx <- resolveEvalContext manager base >>= orDie "eval-context"
    let config =
            WorkflowsConfig
                { wcCage = cage
                , wcPolicy = defaultWalletPolicy
                , wcTrustedRoot = troot
                , wcEvalContext = evalCtx
                }
        httpClient = mkHttpClient manager base
        request = mkReq (Hex (ownerAddressBytes (network cage) sk))
    UnsignedTx cbor <-
        workflow httpClient config request >>= orDieShow "workflow"
    signed <- orDieShow "sign" (signTx sk cbor)
    let env = mkClientEnv manager base
    txId <- submitSignedTx env signed >>= orDieShow "submit"
    _ <- awaitTx env txId defaultAwaitTimeout >>= orDieShow "await"
    emitJson
        ( object
            [ "command" .= name
            , "status" .= ("submitted" :: String)
            , "txId" .= encodeHexText txId
            ]
        )

-- | Decode a @--token@ hex argument to a 'TokenIdJSON' or exit.
decodeToken :: Text -> IO TokenIdJSON
decodeToken = fmap TokenIdJSON . orDie "token" . decodeHexText

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

-- | Await timeout (seconds) for a submitted tx to be indexed.
defaultAwaitTimeout :: Word64
defaultAwaitTimeout = 60

-- | Unwrap an 'Either String' or exit with a contextual error.
orDie :: String -> Either String a -> IO a
orDie ctx = either (\e -> die (ctx <> ": " <> e)) pure

-- | Unwrap an 'Either' whose error only has a 'Show', or exit.
orDieShow :: Show e => String -> Either e a -> IO a
orDieShow ctx = either (\e -> die (ctx <> ": " <> show e)) pure
