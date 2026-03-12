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

      -- * Query endpoints
    , StatusAPI
    , TokensAPI
    , TokenAPI
    , TokenRootAPI
    , TokenFactAPI
    , TokenProofAPI
    , TokenRequestsAPI

      -- * Transaction endpoints
    , TxBootAPI
    , TxInsertAPI
    , TxDeleteAPI
    , TxUpdateAPI
    , TxRetractAPI
    , TxEndAPI
    , TxSubmitAPI
    ) where

import Servant.API
    ( Capture
    , Get
    , JSON
    , Post
    , ReqBody
    , (:<|>)
    , (:>)
    )

import Cardano.MPFS.HTTP.Encoding (Hex)
import Cardano.MPFS.HTTP.Types
    ( BootRequest
    , DeleteRequest
    , EndRequest
    , InsertRequest
    , RequestJSON
    , RetractRequest
    , StatusResponse
    , SubmitRequest
    , TokenIdJSON
    , TokenStateJSON
    , UpdateRequest
    )

-- | @GET \/status@ — indexer chain tip and checkpoint.
type StatusAPI = "status" :> Get '[JSON] StatusResponse

-- | @GET \/tokens@ — list all known token IDs.
type TokensAPI = "tokens" :> Get '[JSON] [TokenIdJSON]

-- | @GET \/tokens\/:id@ — get a token's state.
type TokenAPI =
    "tokens"
        :> Capture "id" TokenIdJSON
        :> Get '[JSON] TokenStateJSON

-- | @GET \/tokens\/:id\/root@ — get trie root hash.
type TokenRootAPI =
    "tokens"
        :> Capture "id" TokenIdJSON
        :> "root"
        :> Get '[JSON] Hex

-- | @GET \/tokens\/:id\/facts\/:key@ — look up a
-- value by key.
type TokenFactAPI =
    "tokens"
        :> Capture "id" TokenIdJSON
        :> "facts"
        :> Capture "key" Hex
        :> Get '[JSON] Hex

-- | @GET \/tokens\/:id\/proofs\/:key@ — generate a
-- Merkle proof for a key.
type TokenProofAPI =
    "tokens"
        :> Capture "id" TokenIdJSON
        :> "proofs"
        :> Capture "key" Hex
        :> Get '[JSON] Hex

-- | @GET \/tokens\/:id\/requests@ — list pending
-- requests for a token.
type TokenRequestsAPI =
    "tokens"
        :> Capture "id" TokenIdJSON
        :> "requests"
        :> Get '[JSON] [RequestJSON]

-- | @POST \/tx\/boot@ — build a boot transaction.
type TxBootAPI =
    "tx" :> "boot"
        :> ReqBody '[JSON] BootRequest
        :> Post '[JSON] Hex

-- | @POST \/tx\/request\/insert@ — build an insert
-- request transaction.
type TxInsertAPI =
    "tx" :> "request" :> "insert"
        :> ReqBody '[JSON] InsertRequest
        :> Post '[JSON] Hex

-- | @POST \/tx\/request\/delete@ — build a delete
-- request transaction.
type TxDeleteAPI =
    "tx" :> "request" :> "delete"
        :> ReqBody '[JSON] DeleteRequest
        :> Post '[JSON] Hex

-- | @POST \/tx\/update@ — build an update
-- transaction.
type TxUpdateAPI =
    "tx" :> "update"
        :> ReqBody '[JSON] UpdateRequest
        :> Post '[JSON] Hex

-- | @POST \/tx\/retract@ — build a retract
-- transaction.
type TxRetractAPI =
    "tx" :> "retract"
        :> ReqBody '[JSON] RetractRequest
        :> Post '[JSON] Hex

-- | @POST \/tx\/end@ — build an end transaction.
type TxEndAPI =
    "tx" :> "end"
        :> ReqBody '[JSON] EndRequest
        :> Post '[JSON] Hex

-- | @POST \/tx\/submit@ — submit a signed
-- transaction.
type TxSubmitAPI =
    "tx" :> "submit"
        :> ReqBody '[JSON] SubmitRequest
        :> Post '[JSON] Hex

-- | Complete MPFS HTTP API.
type API =
    StatusAPI
        :<|> TokensAPI
        :<|> TokenAPI
        :<|> TokenRootAPI
        :<|> TokenFactAPI
        :<|> TokenProofAPI
        :<|> TokenRequestsAPI
        :<|> TxBootAPI
        :<|> TxInsertAPI
        :<|> TxDeleteAPI
        :<|> TxUpdateAPI
        :<|> TxRetractAPI
        :<|> TxEndAPI
        :<|> TxSubmitAPI
