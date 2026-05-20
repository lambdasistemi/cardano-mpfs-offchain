{-# LANGUAGE DataKinds #-}
{-# LANGUAGE TypeOperators #-}

-- |
-- Module      : Cardano.MPFS.HTTP.API
-- Description : Servant API type for the MPFS HTTP service.
-- License     : Apache-2.0
module Cardano.MPFS.HTTP.API
    ( -- * Full API
      API

      -- * Proxy
    , api

      -- * Metrics endpoints
    , MetricsAPI
    , MetricsPrometheusAPI

      -- * Query endpoints
    , StatusAPI
    , TokensAPI
    , TokenAPI
    , TokenRootAPI
    , TokenFactAPI
    , TokenProofAPI
    , TokenRequestsAPI

      -- * UTxO CSMT endpoints
    , UtxoResolveAPI
    , UtxoProofAPI
    , UtxoRootAPI

      -- * Confirmation
    , TxAwaitAPI

      -- * Facts endpoints
    , FactsBootAPI
    , FactsRequestInsertAPI
    , FactsRequestDeleteAPI
    , FactsRetractAPI
    , FactsEndAPI

      -- * Transaction endpoints
    , TxRequestUpdateAPI
    , TxRejectAPI
    , TxUpdateAPI
    , TxRetractAPI
    , TxSweepAPI
    , TxWriteAPI
    , TxSubmitAPI
    ) where

import Data.Proxy (Proxy (..))
import Data.Text (Text)
import Servant.API
    ( Get
    , JSON
    , PlainText
    , (:<|>)
    , (:>)
    )

import Cardano.MPFS.API
    ( FactsBootAPI
    , FactsEndAPI
    , FactsRequestDeleteAPI
    , FactsRequestInsertAPI
    , FactsRetractAPI
    , StatusAPI
    , TokenAPI
    , TokenFactAPI
    , TokenProofAPI
    , TokenRequestsAPI
    , TokenRootAPI
    , TokensAPI
    , TxAwaitAPI
    , TxRejectAPI
    , TxRequestUpdateAPI
    , TxRetractAPI
    , TxSubmitAPI
    , TxSweepAPI
    , TxUpdateAPI
    , TxWriteAPI
    , UtxoProofAPI
    , UtxoResolveAPI
    , UtxoRootAPI
    )
import Cardano.MPFS.API qualified as Shared
import Cardano.UTxOCSMT.Application.Metrics (Metrics)

-- | @GET \/metrics\/prometheus@ — Prometheus exposition text format.
type MetricsPrometheusAPI =
    "metrics"
        :> "prometheus"
        :> Get '[PlainText] Text

-- | @GET \/metrics@ — JSON metrics snapshot.
type MetricsAPI =
    "metrics" :> Get '[JSON] Metrics

-- | Complete offchain HTTP API, including server-local metrics.
type API =
    MetricsPrometheusAPI
        :<|> MetricsAPI
        :<|> Shared.API

-- | Proxy for the complete offchain API.
api :: Proxy API
api = Proxy
