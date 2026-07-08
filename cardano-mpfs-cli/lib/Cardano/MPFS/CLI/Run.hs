-- |
-- Module      : Cardano.MPFS.CLI.Run
-- Description : Subcommand handlers.
--
-- Write commands fetch proof-bearing facts, verify them through
-- @cardano-mpfs-workflows@, build an unsigned tx, sign locally, submit,
-- await indexing, and then report the tx id. Read commands fetch
-- proof-bearing responses, verify them locally, and only then render
-- either human text or a structured JSON envelope.
module Cardano.MPFS.CLI.Run
    ( run
    ) where

import Cardano.MPFS.API.Encoding (Hex (..))
import Cardano.MPFS.API.Types
    ( FactEntry (..)
    , FactResponse (..)
    , FactsResponse (..)
    , ProofResponse
    , RequestsResponse (..)
    , TokenIdJSON (..)
    , TokenResponse (..)
    , TokenSetWitness (..)
    , TokenUtxoEntry (..)
    , TokensResponse (..)
    , TxInJSON (..)
    , UtxoRef (..)
    , WitnessedRequest (..)
    , WitnessedTokenState (..)
    , WitnessedUtxo (..)
    )
import Cardano.MPFS.CLI.Cage (ownerAddressBytes, resolveCageConfig)
import Cardano.MPFS.CLI.Hex
    ( HexArg
    , decodeHexText
    , encodeHexText
    , hexBytes
    )
import Cardano.MPFS.CLI.Key (loadSigningKey)
import Cardano.MPFS.CLI.Options
    ( App (..)
    , Command (..)
    , OutputFormat
    )
import Cardano.MPFS.CLI.Output (die, emit)
import Cardano.MPFS.CLI.Sign (signTx)
import Cardano.MPFS.CLI.Submit
    ( awaitTx
    , getFact
    , getFactProof
    , getToken
    , listFacts
    , listRequests
    , listTokens
    , mkServerParts
    , submitSignedTx
    )
import Cardano.MPFS.CLI.Workflow
    ( defaultWalletPolicy
    , mkHttpClient
    , resolveEvalContext
    , resolveTrustedRoot
    )
import Cardano.MPFS.Client.Cage.Config (CageConfig (..), network)
import Cardano.MPFS.Client.Facts
    ( FactAbsentFacts (..)
    , FactPresentFacts (..)
    , verifyFactAbsentFacts
    , verifyFactPresentFacts
    )
import Cardano.MPFS.Client.TrustedRoot (TrustedRoot)
import Cardano.MPFS.Client.Verify.Read
    ( verifiedTokenFacts
    , verifiedTokenRequests
    , verifiedTokenState
    , verifiedTokens
    , verifyTokenFacts
    , verifyTokenRequests
    , verifyTokenState
    , verifyTokens
    )
import Cardano.MPFS.Workflows
    ( BootRequest (..)
    , DeleteRequest (..)
    , EndRequest (..)
    , HttpClient
    , HttpError (..)
    , InsertRequest (..)
    , RejectRequest (..)
    , RetractRequest (..)
    , UnsignedTx (..)
    , UpdateRequest (..)
    , UpdateValueRequest (..)
    , WorkflowError (..)
    , WorkflowsConfig (..)
    , applyRequests
    , deleteFact
    , endCage
    , insertFact
    , registerToken
    , rejectExpired
    , retractRequest
    , updateFact
    )
import Data.Aeson (ToJSON, Value, object, (.=))
import Data.ByteString qualified as BS
import Data.ByteString.Char8 qualified as BSC
import Data.ByteString.Lazy qualified as BSL
import Data.List (intercalate)
import Data.Maybe (fromMaybe)
import Data.Text (Text)
import Data.Text qualified as T
import Data.Text.Encoding qualified as TE
import Data.Text.Encoding.Error (lenientDecode)
import Data.Word (Word64)
import Network.HTTP.Client (Manager)
import Network.HTTP.Types.Status (statusCode, statusMessage)
import Servant.Client
    ( BaseUrl
    , ClientEnv
    , ClientError
    , mkClientEnv
    )
import Servant.Client qualified as Servant
import Servant.Client.Core.Response
    ( responseBody
    , responseStatusCode
    )
import System.Environment (lookupEnv)

-- | Dispatch a parsed invocation.
run :: App -> IO ()
run App{appOutput, appCommand} = case appCommand of
    RegisterToken{..} ->
        runWriteWithCage
            appOutput
            "register-token"
            server
            ownerKey
            cageConfig
            trustedRoot
            (applyBootTiming processTimeMs retractTimeMs)
            (BootRequest . pure)
            registerToken
    FactInsert{..} -> do
        tid <- decodeToken token
        runWrite
            appOutput
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
            appOutput
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
            appOutput
            "fact delete"
            server
            ownerKey
            cageConfig
            trustedRoot
            (DeleteRequest tid (Hex (hexBytes key)) (Hex (hexBytes value)))
            deleteFact
    FactRetract{..} ->
        runWrite
            appOutput
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
            appOutput
            "fact reject"
            server
            ownerKey
            cageConfig
            trustedRoot
            ( \addr ->
                RejectRequest
                    { rejToken = tid
                    , rejAddr = addr
                    , rejRequests = requestIds
                    }
            )
            rejectExpired
    FactList{..} ->
        runFactList appOutput server token trustedRoot
    FactGet{..} ->
        runFactGet appOutput server token key trustedRoot
    TokenProcess{..} -> do
        tid <- decodeToken token
        runWrite
            appOutput
            "token process"
            server
            ownerKey
            cageConfig
            trustedRoot
            ( \addr ->
                UpdateRequest
                    { urToken = tid
                    , urAddr = addr
                    , urRequests = requestIds
                    }
            )
            applyRequests
    TokenEnd{..} -> do
        tid <- decodeToken token
        runWrite
            appOutput
            "token end"
            server
            ownerKey
            cageConfig
            trustedRoot
            (EndRequest tid)
            endCage
    TokenList{..} ->
        runTokenList appOutput server cageConfig trustedRoot
    TokenGet{..} ->
        runTokenGet appOutput server token trustedRoot
    RequestsList{..} ->
        runRequestsList appOutput server token cageConfig trustedRoot

-- | The full write pipeline shared by every write subcommand.
runWrite
    :: OutputFormat
    -> String
    -- ^ Command name (for output + error context).
    -> Maybe String
    -- ^ Server base URL (else @MPFS_SERVER@).
    -> Maybe FilePath
    -- ^ Bech32 signing-key path (else @MPFS_SIGNER_WALLET@).
    -> Maybe FilePath
    -- ^ Cage blueprint path (else @MPFS_BLUEPRINT@).
    -> Maybe HexArg
    -- ^ Trusted-root override (else server @\/status@).
    -> (Hex -> req)
    -- ^ Build the request from the signer address.
    -> ( HttpClient
         -> WorkflowsConfig
         -> req
         -> IO (Either WorkflowError UnsignedTx)
       )
    -- ^ The workflow to run.
    -> IO ()
runWrite out name mServer mOwnerKey cageConfig trustedRoot =
    runWriteWithCage out name mServer mOwnerKey cageConfig trustedRoot id

runWriteWithCage
    :: OutputFormat
    -> String
    -- ^ Command name (for output + error context).
    -> Maybe String
    -- ^ Server base URL (else @MPFS_SERVER@).
    -> Maybe FilePath
    -- ^ Bech32 signing-key path (else @MPFS_SIGNER_WALLET@).
    -> Maybe FilePath
    -- ^ Cage blueprint path (else @MPFS_BLUEPRINT@).
    -> Maybe HexArg
    -- ^ Trusted-root override (else server @\/status@).
    -> (CageConfig -> CageConfig)
    -- ^ Boot-only timing/config adjustment.
    -> (Hex -> req)
    -- ^ Build the request from the signer address.
    -> ( HttpClient
         -> WorkflowsConfig
         -> req
         -> IO (Either WorkflowError UnsignedTx)
       )
    -- ^ The workflow to run.
    -> IO ()
runWriteWithCage
    out
    name
    mServer
    mOwnerKey
    cageConfig
    trustedRoot
    adjustCage
    mkReq
    workflow = do
        (manager, base) <- resolveServerParts mServer
        ownerKeyPath <- resolveSignerWallet mOwnerKey
        sk <- loadSigningKey ownerKeyPath >>= orDieShow "signer wallet"
        cage <- adjustCage <$> resolveCage cageConfig
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
            workflow httpClient config request >>= orDieWorkflow "workflow"
        signed <- orDieShow "sign" (signTx sk cbor)
        txId <-
            submitSignedTx (mkClientEnv manager base) signed
                >>= orDieClient "submit"
        _ <-
            awaitTx (mkClientEnv manager base) txId defaultAwaitTimeout
                >>= orDieClient "await"
        emit
            out
            ( object
                [ "command" .= name
                , "status" .= ("submitted" :: String)
                , "txId" .= encodeHexText txId
                ]
            )
            ( name
                <> " submitted\ntransaction: "
                <> T.unpack (encodeHexText txId)
            )

runTokenList
    :: OutputFormat
    -> Maybe String
    -> Maybe FilePath
    -> Maybe HexArg
    -> IO ()
runTokenList out mServer cageConfig trustedRoot =
    withReadRoot mServer trustedRoot $ \_ _ env troot -> do
        cage <- resolveCage cageConfig
        resp <- listTokens env >>= orDieClient "token list"
        verified <- orDieVerify "token list" (verifyTokens cage troot resp)
        let verifiedResp = verifiedTokens verified
        emit
            out
            (queryEnvelope "token list" (tokenListResult verifiedResp) verifiedResp)
            (formatTokenList verifiedResp)

runTokenGet
    :: OutputFormat
    -> Maybe String
    -> Text
    -> Maybe HexArg
    -> IO ()
runTokenGet out mServer tokenText trustedRoot =
    withReadRoot mServer trustedRoot $ \_ _ env troot -> do
        tid <- decodeToken tokenText
        resp <- getToken env tid >>= orDieClient "token get"
        verified <- orDieVerify "token get" (verifyTokenState troot resp)
        let verifiedResp = verifiedTokenState verified
        emit
            out
            ( queryEnvelope
                "token get"
                (tokenStateResult tid verifiedResp)
                verifiedResp
            )
            (formatTokenState tid verifiedResp)

runFactList
    :: OutputFormat
    -> Maybe String
    -> Text
    -> Maybe HexArg
    -> IO ()
runFactList out mServer tokenText trustedRoot =
    withReadRoot mServer trustedRoot $ \_ _ env troot -> do
        tid <- decodeToken tokenText
        resp <- listFacts env tid >>= orDieClient "fact list"
        verified <- orDieVerify "fact list" (verifyTokenFacts troot resp)
        let verifiedResp = verifiedTokenFacts verified
        emit
            out
            ( queryEnvelope
                "fact list"
                (factListResult tid verifiedResp)
                verifiedResp
            )
            (formatFactList tid verifiedResp)

runFactGet
    :: OutputFormat
    -> Maybe String
    -> Text
    -> HexArg
    -> Maybe HexArg
    -> IO ()
runFactGet out mServer tokenText key trustedRoot =
    withReadRoot mServer trustedRoot $ \_ _ env troot -> do
        tid <- decodeToken tokenText
        let keyHex = Hex (hexBytes key)
        present <- getFact env tid keyHex
        case present of
            Right resp -> do
                _ <-
                    orDieVerify
                        "fact get"
                        ( verifyFactPresentFacts
                            troot
                            FactPresentFacts
                                { fpfKey = keyHex
                                , fpfResponse = resp
                                }
                        )
                emit
                    out
                    (factGetPresentEnvelope tid keyHex resp)
                    (formatFactPresent tid keyHex resp)
            Left err
                | isNotFound err -> do
                    proof <-
                        getFactProof env tid keyHex
                            >>= orDieClient "fact get absence proof"
                    _ <-
                        orDieVerify
                            "fact get"
                            ( verifyFactAbsentFacts
                                troot
                                FactAbsentFacts
                                    { fafKey = keyHex
                                    , fafResponse = proof
                                    }
                            )
                    emit
                        out
                        (factGetAbsentEnvelope tid keyHex proof)
                        (formatFactAbsent tid keyHex)
                | otherwise -> die ("fact get: " <> renderClientError err)

runRequestsList
    :: OutputFormat
    -> Maybe String
    -> Text
    -> Maybe FilePath
    -> Maybe HexArg
    -> IO ()
runRequestsList out mServer tokenText cageConfig trustedRoot =
    withReadRoot mServer trustedRoot $ \_ _ env troot -> do
        cage <- resolveCage cageConfig
        tid <- decodeToken tokenText
        reqResp <- listRequests env tid >>= orDieClient "requests list"
        reqVerified <-
            orDieVerify
                "requests list"
                (verifyTokenRequests cage tid troot reqResp)
        let verifiedResp = verifiedTokenRequests reqVerified
        emit
            out
            (requestsEnvelope tid verifiedResp)
            (formatRequests tid verifiedResp)

-- | Decode a @TOKEN@ hex argument to a 'TokenIdJSON' or exit.
decodeToken :: Text -> IO TokenIdJSON
decodeToken = fmap TokenIdJSON . orDie "token" . decodeHexText

withReadRoot
    :: Maybe String
    -> Maybe HexArg
    -> (Manager -> BaseUrl -> ClientEnv -> TrustedRoot -> IO ())
    -> IO ()
withReadRoot mServer trustedRoot k = do
    (manager, base) <- resolveServerParts mServer
    troot <-
        resolveTrustedRoot manager base (hexBytes <$> trustedRoot)
            >>= orDie "trusted-root"
    k manager base (mkClientEnv manager base) troot

resolveServerParts :: Maybe String -> IO (Manager, BaseUrl)
resolveServerParts mServer = do
    serverUrl <- resolveServer mServer
    mkServerParts serverUrl >>= orDie "server"

resolveServer :: Maybe String -> IO String
resolveServer (Just serverUrl) = pure serverUrl
resolveServer Nothing = do
    mServer <- lookupEnv "MPFS_SERVER"
    case mServer of
        Just serverUrl -> pure serverUrl
        Nothing ->
            die
                "server: no --server URL supplied and MPFS_SERVER is unset"

resolveSignerWallet :: Maybe FilePath -> IO FilePath
resolveSignerWallet (Just path) = pure path
resolveSignerWallet Nothing = do
    mPath <- lookupEnv "MPFS_SIGNER_WALLET"
    case mPath of
        Just path -> pure path
        Nothing ->
            die
                "signer wallet: no --owner-key KEYFILE supplied and \
                \MPFS_SIGNER_WALLET is unset"

resolveCage :: Maybe FilePath -> IO CageConfig
resolveCage cageConfig = resolveCageConfig cageConfig >>= orDie "cage-config"

applyBootTiming
    :: Maybe Integer
    -> Maybe Integer
    -> CageConfig
    -> CageConfig
applyBootTiming processTimeOverride retractTimeOverride cage =
    cage
        { defaultProcessTime =
            fromMaybe (defaultProcessTime cage) processTimeOverride
        , defaultRetractTime =
            fromMaybe (defaultRetractTime cage) retractTimeOverride
        }

queryEnvelope
    :: ToJSON response => String -> Value -> response -> Value
queryEnvelope name result response =
    object
        [ "command" .= name
        , "verified" .= True
        , "result" .= result
        , "response" .= response
        ]

tokenListResult :: TokensResponse -> Value
tokenListResult TokensResponse{trsTokens = TokenSetWitness{..}} =
    object
        [ "tokens" .= map tokenEntryResult tswEntries
        ]

-- | A token entry is rendered as its provable witness fields. The
-- token id is the state token's asset name inside @txout_cbor@;
-- consumers that need it decode the witnessed @TxOut@ locally.
tokenEntryResult :: TokenUtxoEntry -> Value
tokenEntryResult TokenUtxoEntry{..} =
    object
        [ "ref" .= utxoRefText tueRef
        , "txout_cbor" .= hexText tueTxOutCbor
        ]

-- | The token-state response is rendered as its provable state-UTxO
-- witness. The decoded on-chain state lives in the inline datum of
-- @tx_out@; consumers decode it locally rather than trusting a
-- server-side projection.
tokenStateResult :: TokenIdJSON -> TokenResponse -> Value
tokenStateResult tid resp =
    let wu = responseStateUtxo resp
    in  object
            [ "token" .= tokenIdText tid
            , "stateRef" .= txInText (wuTxIn wu)
            , "txout_cbor" .= hexText (wuTxOut wu)
            ]

factListResult :: TokenIdJSON -> FactsResponse -> Value
factListResult tid FactsResponse{..} =
    object
        [ "token" .= tokenIdText tid
        , "facts" .= map factEntryResult frsFacts
        ]

factEntryResult :: FactEntry -> Value
factEntryResult FactEntry{..} =
    object
        [ "key" .= hexText feKey
        , "value" .= hexText feValue
        ]

factGetPresentEnvelope :: TokenIdJSON -> Hex -> FactResponse -> Value
factGetPresentEnvelope tid keyHex resp =
    queryEnvelope
        "fact get"
        ( object
            [ "token" .= tokenIdText tid
            , "key" .= hexText keyHex
            , "present" .= True
            , "value" .= factResponseValue resp
            ]
        )
        resp

factGetAbsentEnvelope :: TokenIdJSON -> Hex -> ProofResponse -> Value
factGetAbsentEnvelope tid keyHex =
    queryEnvelope
        "fact get"
        ( object
            [ "token" .= tokenIdText tid
            , "key" .= hexText keyHex
            , "present" .= False
            ]
        )

requestsEnvelope
    :: TokenIdJSON
    -> RequestsResponse
    -> Value
requestsEnvelope tid resp@RequestsResponse{..} =
    queryEnvelope
        "requests list"
        ( object
            [ "token" .= tokenIdText tid
            , "requests" .= map requestResult rrRequests
            ]
        )
        resp

-- | A pending request is rendered as its provable witness fields. The
-- request payload (operation, key, value, owner, fee, submitted-at)
-- lives in the inline datum of @tx_out@; consumers decode it locally
-- rather than trusting a server-side projection.
requestResult :: WitnessedRequest -> Value
requestResult WitnessedRequest{..} =
    object
        [ "id" .= txInText (wuTxIn wrUtxo)
        , "txout_cbor" .= hexText (wuTxOut wrUtxo)
        ]

formatTokenList :: TokensResponse -> String
formatTokenList TokensResponse{trsTokens = TokenSetWitness{..}} =
    case tswEntries of
        [] -> "No verified tokens."
        entries ->
            intercalate
                "\n"
                ("Verified tokens:" : map formatTokenEntry entries)

formatTokenEntry :: TokenUtxoEntry -> String
formatTokenEntry TokenUtxoEntry{..} =
    "- " <> T.unpack (utxoRefText tueRef)

formatTokenState :: TokenIdJSON -> TokenResponse -> String
formatTokenState tid resp =
    let wu = responseStateUtxo resp
    in  intercalate
            "\n"
            [ "Verified token " <> T.unpack (tokenIdText tid)
            , "state_utxo: " <> T.unpack (txInText (wuTxIn wu))
            ]

formatFactList :: TokenIdJSON -> FactsResponse -> String
formatFactList tid FactsResponse{..} =
    case frsFacts of
        [] ->
            "No verified facts for token " <> T.unpack (tokenIdText tid) <> "."
        facts ->
            intercalate
                "\n"
                ( ("Verified facts for token " <> T.unpack (tokenIdText tid) <> ":")
                    : map formatFactEntry facts
                )

formatFactEntry :: FactEntry -> String
formatFactEntry FactEntry{..} =
    "- "
        <> T.unpack (hexText feKey)
        <> " = "
        <> T.unpack (hexText feValue)

formatFactPresent :: TokenIdJSON -> Hex -> FactResponse -> String
formatFactPresent tid keyHex resp =
    intercalate
        "\n"
        [ "Verified fact is present"
        , "token: " <> T.unpack (tokenIdText tid)
        , "key: " <> T.unpack (hexText keyHex)
        , "value: " <> T.unpack (factResponseValue resp)
        ]

formatFactAbsent :: TokenIdJSON -> Hex -> String
formatFactAbsent tid keyHex =
    intercalate
        "\n"
        [ "Verified fact is absent"
        , "token: " <> T.unpack (tokenIdText tid)
        , "key: " <> T.unpack (hexText keyHex)
        ]

formatRequests
    :: TokenIdJSON -> RequestsResponse -> String
formatRequests tid RequestsResponse{..} =
    case rrRequests of
        [] ->
            "No verified pending requests for token "
                <> T.unpack (tokenIdText tid)
                <> "."
        reqs ->
            intercalate
                "\n"
                ( ( "Verified pending requests for token "
                        <> T.unpack (tokenIdText tid)
                        <> ":"
                  )
                    : map formatRequest reqs
                )

formatRequest :: WitnessedRequest -> String
formatRequest WitnessedRequest{..} =
    "- " <> T.unpack (txInText (wuTxIn wrUtxo))

responseStateUtxo :: TokenResponse -> WitnessedUtxo
responseStateUtxo TokenResponse{trState = WitnessedTokenState{..}} =
    wtsUtxo

factResponseValue :: FactResponse -> Text
factResponseValue FactResponse{..} = hexText frValue

hexText :: Hex -> Text
hexText (Hex bytes) = encodeHexText bytes

tokenIdText :: TokenIdJSON -> Text
tokenIdText (TokenIdJSON bytes) = encodeHexText bytes

txInText :: TxInJSON -> Text
txInText TxInJSON{..} =
    hexText tjTxId <> "#" <> T.pack (show tjTxIx)

utxoRefText :: UtxoRef -> Text
utxoRefText UtxoRef{..} =
    hexText urTxId <> "#" <> T.pack (show urTxIx)

-- | Await timeout (seconds) for a submitted tx to be indexed.
defaultAwaitTimeout :: Word64
defaultAwaitTimeout = 60

-- | Unwrap an 'Either String' or exit with a contextual error.
orDie :: String -> Either String a -> IO a
orDie ctx = either (\e -> die (ctx <> ": " <> e)) pure

-- | Unwrap an 'Either' whose error only has a 'Show', or exit.
orDieShow :: Show e => String -> Either e a -> IO a
orDieShow ctx = either (\e -> die (ctx <> ": " <> show e)) pure

orDieWorkflow :: String -> Either WorkflowError a -> IO a
orDieWorkflow ctx =
    either (\e -> die (ctx <> ": " <> renderWorkflowError e)) pure

orDieClient :: String -> Either ClientError a -> IO a
orDieClient ctx =
    either (\e -> die (ctx <> ": " <> renderClientError e)) pure

orDieVerify :: Show e => String -> Either e a -> IO a
orDieVerify ctx =
    either (\e -> die (ctx <> ": verification failed: " <> show e)) pure

renderWorkflowError :: WorkflowError -> String
renderWorkflowError err =
    case err of
        WorkflowHttpError (HttpStatus code body) ->
            "server returned HTTP "
                <> show code
                <> bodySuffix (BSL.fromStrict body)
        WorkflowHttpError (HttpTransport msg) ->
            "transport failure: " <> T.unpack msg
        WorkflowDecodeError msg ->
            "failed to decode server response: " <> msg
        WorkflowVerifyError verifyErr ->
            "verification failed: " <> show verifyErr
        WorkflowBuildError buildErr ->
            "transaction build failed: " <> show buildErr

renderClientError :: ClientError -> String
renderClientError err =
    case err of
        Servant.FailureResponse _ response ->
            let status = responseStatusCode response
                code = statusCode status
                message = BSC.unpack (statusMessage status)
            in  "server returned HTTP "
                    <> show code
                    <> " "
                    <> message
                    <> bodySuffix (responseBody response)
        Servant.DecodeFailure msg _ ->
            "failed to decode server response: " <> T.unpack msg
        Servant.UnsupportedContentType mediaType _ ->
            "unsupported response content type: " <> show mediaType
        Servant.InvalidContentTypeHeader _ ->
            "invalid response Content-Type header"
        Servant.ConnectionError connErr ->
            "connection failed: " <> show connErr

isNotFound :: ClientError -> Bool
isNotFound err =
    case err of
        Servant.FailureResponse _ response ->
            statusCode (responseStatusCode response) == 404
        _ -> False

bodySuffix :: BSL.ByteString -> String
bodySuffix body =
    let strict = BS.take 4096 (BSL.toStrict body)
    in  if BS.null strict
            then ""
            else ": " <> T.unpack (TE.decodeUtf8With lenientDecode strict)
