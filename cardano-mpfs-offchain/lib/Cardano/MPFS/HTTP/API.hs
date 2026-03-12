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
    ) where

import Servant.API
    ( Capture
    , Get
    , JSON
    , (:<|>)
    , (:>)
    )

import Cardano.MPFS.HTTP.Types
    ( StatusResponse
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

-- | Complete MPFS HTTP API.
type API = StatusAPI :<|> TokensAPI :<|> TokenAPI
