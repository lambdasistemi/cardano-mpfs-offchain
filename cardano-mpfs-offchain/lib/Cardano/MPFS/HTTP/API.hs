{-# LANGUAGE DataKinds #-}
{-# LANGUAGE TypeOperators #-}

-- |
-- Module      : Cardano.MPFS.HTTP.API
-- Description : Servant API type for the MPFS HTTP service
-- License     : Apache-2.0
--
-- Defines the Servant API type combining all endpoint
-- groups. Each endpoint group is defined as a separate
-- type alias and composed via '(:<|>)'.
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

      -- * Transaction endpoints
    , TxBootAPI
    , TxInsertAPI
    , TxDeleteAPI
    , TxRequestUpdateAPI
    , TxRejectAPI
    , TxUpdateAPI
    , TxRetractAPI
    , TxEndAPI
    , TxSubmitAPI
    ) where

import Data.Proxy (Proxy (..))
import Data.Text (Text)
import Data.Word (Word64)
import Servant.API
    ( Capture
    , Get
    , JSON
    , NoContent
    , PlainText
    , Post
    , QueryParam
    , ReqBody
    , (:<|>)
    , (:>)
    )

import Cardano.MPFS.HTTP.Encoding (Hex)
import Cardano.MPFS.HTTP.Types
    ( BootRequest
    , DeleteRequest
    , EndRequest
    , FactResponse
    , InsertRequest
    , ProofResponse
    , RejectRequest
    , RequestsResponse
    , RetractRequest
    , StatusResponse
    , SubmitRequest
    , TokenIdJSON
    , TokenResponse
    , UpdateRequest
    , UpdateValueRequest
    )
import Cardano.UTxOCSMT.Application.Metrics (Metrics)

-- | @GET \/metrics\/prometheus@ — Prometheus exposition
-- text format.
type MetricsPrometheusAPI =
    "metrics"
        :> "prometheus"
        :> Get '[PlainText] Text

-- | @GET \/metrics@ — JSON metrics snapshot.
type MetricsAPI =
    "metrics" :> Get '[JSON] Metrics

-- | @GET \/status@ — indexer chain tip and checkpoint.
type StatusAPI = "status" :> Get '[JSON] StatusResponse

-- | @GET \/tokens@ — list all known token IDs.
type TokensAPI = "tokens" :> Get '[JSON] [TokenIdJSON]

-- | @GET \/tokens\/:id@ — get a token's state
-- together with its UTxO witness and the
-- verification snapshot the bundled proof targets.
type TokenAPI =
    "tokens"
        :> Capture "id" TokenIdJSON
        :> Get '[JSON] TokenResponse

-- | @GET \/tokens\/:id\/root@ — get trie root hash.
type TokenRootAPI =
    "tokens"
        :> Capture "id" TokenIdJSON
        :> "root"
        :> Get '[JSON] Hex

-- | @GET \/tokens\/:id\/facts\/:key@ — look up a
-- value by key, returning the value, the state
-- witness it belongs to, and an MPF inclusion proof
-- against that state's trie root.
type TokenFactAPI =
    "tokens"
        :> Capture "id" TokenIdJSON
        :> "facts"
        :> Capture "key" Hex
        :> Get '[JSON] FactResponse

-- | @GET \/tokens\/:id\/proofs\/:key@ — produce an
-- MPF inclusion proof for a key together with the
-- state witness it targets and the verification
-- snapshot.
type TokenProofAPI =
    "tokens"
        :> Capture "id" TokenIdJSON
        :> "proofs"
        :> Capture "key" Hex
        :> Get '[JSON] ProofResponse

-- | @GET \/tokens\/:id\/requests@ — list pending
-- requests for a token together with their UTxO
-- witnesses and the verification snapshot the
-- bundled proofs target.
type TokenRequestsAPI =
    "tokens"
        :> Capture "id" TokenIdJSON
        :> "requests"
        :> Get '[JSON] RequestsResponse

-- | @GET \/utxo\/:txId\/:txIx@ — resolve a TxIn to
-- its CBOR-encoded TxOut (hex).
type UtxoResolveAPI =
    "utxo"
        :> Capture "txId" Hex
        :> Capture "txIx" Word64
        :> Get '[JSON] Hex

-- | @GET \/utxo\/:txId\/:txIx\/proof@ — CSMT
-- inclusion proof for a UTxO (hex bytes).
type UtxoProofAPI =
    "utxo"
        :> Capture "txId" Hex
        :> Capture "txIx" Word64
        :> "proof"
        :> Get '[JSON] Hex

-- | @GET \/utxo\/root@ — current CSMT Merkle root.
type UtxoRootAPI =
    "utxo"
        :> "root"
        :> Get '[JSON] Hex

-- | @GET \/tx\/:txId?timeout=30@ — block until the
-- transaction's first output (TxIn(txId, 0)) appears
-- in the indexed UTxO set. Returns 204 on success,
-- 408 on timeout. Default timeout is 30 seconds.
type TxAwaitAPI =
    "tx"
        :> Capture "txId" Hex
        :> QueryParam "timeout" Word64
        :> Get '[JSON] NoContent

-- | @POST \/tx\/boot@ — build a boot transaction.
type TxBootAPI =
    "tx"
        :> "boot"
        :> ReqBody '[JSON] BootRequest
        :> Post '[JSON] Hex

-- | @POST \/tx\/request\/insert@ — build an insert
-- request transaction.
type TxInsertAPI =
    "tx"
        :> "request"
        :> "insert"
        :> ReqBody '[JSON] InsertRequest
        :> Post '[JSON] Hex

-- | @POST \/tx\/request\/delete@ — build a delete
-- request transaction.
type TxDeleteAPI =
    "tx"
        :> "request"
        :> "delete"
        :> ReqBody '[JSON] DeleteRequest
        :> Post '[JSON] Hex

-- | @POST \/tx\/request\/update@ — build an
-- update-value request transaction.
type TxRequestUpdateAPI =
    "tx"
        :> "request"
        :> "update"
        :> ReqBody '[JSON] UpdateValueRequest
        :> Post '[JSON] Hex

-- | @POST \/tx\/reject@ — build a reject
-- transaction for Phase 3 requests.
type TxRejectAPI =
    "tx"
        :> "reject"
        :> ReqBody '[JSON] RejectRequest
        :> Post '[JSON] Hex

-- | @POST \/tx\/update@ — build an update
-- transaction.
type TxUpdateAPI =
    "tx"
        :> "update"
        :> ReqBody '[JSON] UpdateRequest
        :> Post '[JSON] Hex

-- | @POST \/tx\/retract@ — build a retract
-- transaction.
type TxRetractAPI =
    "tx"
        :> "retract"
        :> ReqBody '[JSON] RetractRequest
        :> Post '[JSON] Hex

-- | @POST \/tx\/end@ — build an end transaction.
type TxEndAPI =
    "tx"
        :> "end"
        :> ReqBody '[JSON] EndRequest
        :> Post '[JSON] Hex

-- | @POST \/tx\/submit@ — submit a signed
-- transaction.
type TxSubmitAPI =
    "tx"
        :> "submit"
        :> ReqBody '[JSON] SubmitRequest
        :> Post '[JSON] Hex

-- | Proxy for the complete API.
api :: Proxy API
api = Proxy

-- | Complete MPFS HTTP API.
type API =
    MetricsPrometheusAPI
        :<|> MetricsAPI
        :<|> StatusAPI
        :<|> TokensAPI
        :<|> TokenAPI
        :<|> TokenRootAPI
        :<|> TokenFactAPI
        :<|> TokenProofAPI
        :<|> TokenRequestsAPI
        :<|> UtxoResolveAPI
        :<|> UtxoProofAPI
        :<|> UtxoRootAPI
        :<|> TxAwaitAPI
        :<|> TxBootAPI
        :<|> TxInsertAPI
        :<|> TxDeleteAPI
        :<|> TxRequestUpdateAPI
        :<|> TxRejectAPI
        :<|> TxUpdateAPI
        :<|> TxRetractAPI
        :<|> TxEndAPI
        :<|> TxSubmitAPI
