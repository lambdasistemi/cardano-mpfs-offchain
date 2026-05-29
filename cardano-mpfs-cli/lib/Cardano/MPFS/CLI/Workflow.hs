{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

-- |
-- Module      : Cardano.MPFS.CLI.Workflow
-- Description : Shared plumbing for wiring cardano-mpfs-workflows.
--
-- The transport adapter, trusted-root resolution, and policy defaults
-- every write subcommand shares when calling a @cardano-mpfs-workflows@
-- function. The CLI is a pure consumer: it supplies the 'HttpClient'
-- transport and 'WorkflowsConfig' inputs, never reimplements protocol
-- logic.
module Cardano.MPFS.CLI.Workflow
    ( mkHttpClient
    , resolveTrustedRoot
    , defaultWalletPolicy
    ) where

import Cardano.Ledger.Coin (Coin (..))
import Cardano.Ledger.Plutus.ExUnits (Prices (..))
import Cardano.Slotting.Slot (SlotNo (..))
import Control.Exception (SomeException, try)
import Data.ByteString (ByteString)
import Data.ByteString.Lazy qualified as BSL
import Data.Proxy (Proxy (..))
import Data.Text qualified as T
import Network.HTTP.Client
    ( Manager
    , RequestBody (RequestBodyBS)
    , httpLbs
    , method
    , parseRequest
    , requestBody
    , requestHeaders
    , responseBody
    , responseStatus
    )
import Network.HTTP.Types.Status (statusCode)
import Servant.Client
    ( BaseUrl
    , client
    , mkClientEnv
    , runClientM
    , showBaseUrl
    )

import Cardano.MPFS.API (StatusAPI)
import Cardano.MPFS.API.Encoding (Hex (..))
import Cardano.MPFS.API.Types (StatusResponse (..))
import Cardano.MPFS.Client.Cage.Policy (WalletPolicy (..))
import Cardano.MPFS.Client.TrustedRoot (TrustedRoot (..))
import Cardano.MPFS.Workflows (HttpClient (..), HttpError (..))

-- | Adapt http-client to the workflows 'HttpClient' transport: POST a
-- JSON body to @baseUrl <> path@ and return the response body, mapping
-- non-2xx and transport failures to 'HttpError'.
mkHttpClient :: Manager -> BaseUrl -> HttpClient
mkHttpClient manager base =
    HttpClient $ \path body -> do
        initReq <- parseRequest (showBaseUrl base <> T.unpack path)
        let req =
                initReq
                    { method = "POST"
                    , requestBody = RequestBodyBS body
                    , requestHeaders =
                        [("Content-Type", "application/json")]
                    }
        result <- try (httpLbs req manager)
        pure $ case result of
            Left (e :: SomeException) ->
                Left (HttpTransport (T.pack (show e)))
            Right resp ->
                let code = statusCode (responseStatus resp)
                    respBody = BSL.toStrict (responseBody resp)
                in  if code >= 200 && code < 300
                        then Right respBody
                        else Left (HttpStatus code respBody)

-- | Resolve the trusted UTxO-CSMT root for a write workflow.
--
-- With an explicit override (raw root bytes, e.g. from
-- @--trusted-root@) the CLI verifies facts against that
-- independently-obtained anchor. Without one, it fetches the root from
-- the server's @\/status@ — appropriate when running against your own
-- server, which is the trust anchor by topology.
resolveTrustedRoot
    :: Manager
    -> BaseUrl
    -> Maybe ByteString
    -> IO (Either String TrustedRoot)
resolveTrustedRoot _ _ (Just rootBytes) =
    pure (Right (TrustedRoot (Hex rootBytes)))
resolveTrustedRoot manager base Nothing = do
    result <- runClientM statusClient (mkClientEnv manager base)
    pure $ case result of
        Left err ->
            Left ("failed to fetch /status: " <> show err)
        Right status ->
            case currentUtxoRoot status of
                Just root -> Right (TrustedRoot root)
                Nothing ->
                    Left "server /status has no utxo_root yet"
  where
    statusClient = client (Proxy @StatusAPI)

-- | Permissive wallet policy caps for CLI-built transactions. The CLI
-- builds and submits txs the operator already controls, so the caps are
-- generous; tightening them is a future flag.
defaultWalletPolicy :: WalletPolicy
defaultWalletPolicy =
    WalletPolicy
        { wpMaxFee = Coin 10_000_000
        , wpMaxExUnitPrices = Prices maxBound maxBound
        , wpMaxMinUtxoCoinPerByte = Coin 10_000
        , wpMaxValidityWindow = SlotNo maxBound
        }
