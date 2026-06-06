{-# LANGUAGE DataKinds #-}
{-# LANGUAGE TypeOperators #-}

-- |
-- Module      : Cardano.MPFS.API
-- Description : Servant API type for the MPFS HTTP service
-- License     : Apache-2.0
--
-- Defines the Servant API type combining all endpoint
-- groups. Each endpoint group is defined as a separate
-- type alias and composed via '(:<|>)'.
module Cardano.MPFS.API
    ( -- * Full API
      API

      -- * Proxy
    , api

      -- * Query endpoints
    , StatusAPI
    , EvalContextAPI
    , TokensAPI
    , TokenAPI
    , TokenRootAPI
    , TokensFactsAPI
    , TokenFactAPI
    , TokenProofAPI
    , TokenRequestsAPI

      -- * Response types
    , FactEntry (..)
    , FactsResponse (..)
    , TokensResponse (..)

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
    , FactsRequestUpdateAPI
    , FactsUpdateAPI
    , FactsRetractAPI
    , FactsRejectAPI
    , FactsEndAPI

      -- * Transaction endpoints
    , TxSweepAPI
    , TxWriteAPI
    , TxSubmitAPI
    ) where

import Data.Proxy (Proxy (..))
import Data.Word (Word64)
import Servant.API
    ( Capture
    , Get
    , JSON
    , NoContent
    , Post
    , QueryParam
    , ReqBody
    , (:<|>)
    , (:>)
    )

import Cardano.MPFS.API.Encoding (Hex)
import Cardano.MPFS.API.Types
    ( BootRequest
    , DeleteRequest
    , EndRequest
    , EvalContext (..)
    , FactEntry (..)
    , FactResponse
    , FactsResponse (..)
    , InsertRequest
    , ProofResponse
    , RejectRequest
    , RequestsResponse
    , RetractRequest
    , StatusResponse
    , SubmitRequest
    , SubmitResponse
    , SweepRequest
    , SweepTxResponse
    , TokenResponse
    , TokensResponse (..)
    , UpdateRequest
    , UpdateValueRequest
    )
import Cardano.MPFS.API.Types.Common (TokenIdJSON)
import Cardano.MPFS.API.Types.Facts
    ( BootFacts
    , EndFacts
    , RejectFacts
    , RequestDeleteFacts
    , RequestInsertFacts
    , RequestUpdateFacts
    , RetractFacts
    , UpdateFacts
    )

-- | @GET \/status@ — indexer chain tip and checkpoint.
type StatusAPI = "status" :> Get '[JSON] StatusResponse

-- | @GET \/eval-context@ — trusted-not-proven ledger
-- context required for wallet-side ex-unit evaluation.
type EvalContextAPI =
    "eval-context" :> Get '[JSON] EvalContext

-- | @GET \/tokens@ — get the complete token-state
-- UTxO set with a prefix-completeness witness.
type TokensAPI = "tokens" :> Get '[JSON] TokensResponse

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

-- | @GET \/tokens\/:id\/facts@ — enumerate every
-- fact key and value for a token, returning the
-- state witness carrying the trie root and the
-- verification snapshot the bundled UTxO proof
-- targets.
type TokensFactsAPI =
    "tokens"
        :> Capture "id" TokenIdJSON
        :> "facts"
        :> Get '[JSON] FactsResponse

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

-- | @POST \/facts\/boot@ — return indexed facts
-- required by wallet-side boot transaction
-- construction.
type FactsBootAPI =
    "facts"
        :> "boot"
        :> ReqBody '[JSON] BootRequest
        :> Post '[JSON] BootFacts

-- | @POST \/facts\/request\/insert@ — return indexed facts
-- required by wallet-side request-insert transaction
-- construction.
type FactsRequestInsertAPI =
    "facts"
        :> "request"
        :> "insert"
        :> ReqBody '[JSON] InsertRequest
        :> Post '[JSON] RequestInsertFacts

-- | @POST \/facts\/request\/delete@ — return indexed facts
-- required by wallet-side request-delete transaction
-- construction.
type FactsRequestDeleteAPI =
    "facts"
        :> "request"
        :> "delete"
        :> ReqBody '[JSON] DeleteRequest
        :> Post '[JSON] RequestDeleteFacts

-- | @POST \/facts\/request\/update@ — return indexed facts
-- required by wallet-side request-update transaction
-- construction.
type FactsRequestUpdateAPI =
    "facts"
        :> "request"
        :> "update"
        :> ReqBody '[JSON] UpdateValueRequest
        :> Post '[JSON] RequestUpdateFacts

-- | @POST \/facts\/update@ — return indexed facts
-- required by wallet-side update transaction construction.
type FactsUpdateAPI =
    "facts"
        :> "update"
        :> ReqBody '[JSON] UpdateRequest
        :> Post '[JSON] UpdateFacts

-- | @POST \/facts\/retract@ — return indexed facts
-- required by wallet-side retract transaction
-- construction.
type FactsRetractAPI =
    "facts"
        :> "retract"
        :> ReqBody '[JSON] RetractRequest
        :> Post '[JSON] RetractFacts

-- | @POST \/facts\/reject@ — return indexed facts
-- required by wallet-side Phase 3 reject transaction
-- construction.
type FactsRejectAPI =
    "facts"
        :> "reject"
        :> ReqBody '[JSON] RejectRequest
        :> Post '[JSON] RejectFacts

-- | @POST \/facts\/end@ — return indexed facts
-- required by wallet-side end transaction
-- construction.
type FactsEndAPI =
    "facts"
        :> "end"
        :> ReqBody '[JSON] EndRequest
        :> Post '[JSON] EndFacts

-- | @POST \/tx\/sweep@ — build an owner-only sweep
-- transaction that spends a non-legitimate UTxO at
-- the per-cage request address (PR #50).
type TxSweepAPI =
    "tx"
        :> "sweep"
        :> ReqBody '[JSON] SweepRequest
        :> Post '[JSON] SweepTxResponse

-- | MPFS write endpoints that construct unsigned
-- transactions. This subset is used by native clients
-- deriving a Servant client without depending on the
-- server package.
type TxWriteAPI = TxSweepAPI

-- | @POST \/submit@ — submit a signed
-- transaction.
type TxSubmitAPI =
    "submit"
        :> ReqBody '[JSON] SubmitRequest
        :> Post '[JSON] SubmitResponse

-- | Proxy for the complete API.
api :: Proxy API
api = Proxy

-- | Complete MPFS HTTP API.
type API =
    StatusAPI
        :<|> EvalContextAPI
        :<|> TokensAPI
        :<|> TokenAPI
        :<|> TokenRootAPI
        :<|> TokensFactsAPI
        :<|> TokenFactAPI
        :<|> TokenProofAPI
        :<|> TokenRequestsAPI
        :<|> UtxoResolveAPI
        :<|> UtxoProofAPI
        :<|> UtxoRootAPI
        :<|> TxAwaitAPI
        :<|> FactsBootAPI
        :<|> FactsRequestInsertAPI
        :<|> FactsRequestDeleteAPI
        :<|> FactsRequestUpdateAPI
        :<|> FactsUpdateAPI
        :<|> FactsRetractAPI
        :<|> FactsRejectAPI
        :<|> FactsEndAPI
        :<|> TxSweepAPI
        :<|> TxSubmitAPI
