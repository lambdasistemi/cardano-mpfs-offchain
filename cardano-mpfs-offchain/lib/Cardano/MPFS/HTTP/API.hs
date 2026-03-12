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
    , StatusAPI
    , TokensAPI
    , TokenAPI
    , TokenRootAPI
    , TokenFactAPI
    , TokenProofAPI
    , TokenRequestsAPI
    ) where

import Servant.API
    ( Capture
    , Get
    , JSON
    , (:<|>)
    , (:>)
    )

import Cardano.MPFS.HTTP.Encoding (Hex)
import Cardano.MPFS.HTTP.Types
    ( RequestJSON
    , StatusResponse
    , TokenIdJSON
    , TokenStateJSON
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

-- | Complete MPFS HTTP API.
type API =
    StatusAPI
        :<|> TokensAPI
        :<|> TokenAPI
        :<|> TokenRootAPI
        :<|> TokenFactAPI
        :<|> TokenProofAPI
        :<|> TokenRequestsAPI
