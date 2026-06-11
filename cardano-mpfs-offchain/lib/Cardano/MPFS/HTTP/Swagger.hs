{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedStrings #-}

-- |
-- Module      : Cardano.MPFS.HTTP.Swagger
-- Description : OpenAPI/Swagger documentation for MPFS
-- License     : Apache-2.0
--
-- Generates Swagger 2.0 documentation from the
-- Servant API type and serves it via SwaggerSchemaUI.
module Cardano.MPFS.HTTP.Swagger
    ( -- * Swagger document
      swaggerDoc

      -- * JSON rendering
    , renderSwaggerJSON

      -- * Servant plumbing
    , SwaggerAPI
    , swaggerServer
    ) where

import Cardano.MPFS.HTTP.API (api)
import Control.Lens ((&), (.~), (?~))
import Data.Aeson.Encode.Pretty (encodePretty)
import Data.ByteString.Lazy (LazyByteString)
import Data.Swagger
    ( Swagger
    , description
    , info
    , license
    , title
    , version
    )
import Servant (Server)
import Servant.Swagger (toSwagger)
import Servant.Swagger.UI (SwaggerSchemaUI)
import Servant.Swagger.UI qualified as SwaggerUI

-- | Generate Swagger documentation for the API.
swaggerDoc :: Swagger
swaggerDoc =
    toSwagger api
        & info . title .~ "MPFS HTTP API"
        & info . version .~ "0.0.0"
        & info . description
            ?~ "HTTP service for the Cardano \
               \Merkle Patricia Forestry offchain \
               \node. Provides token lifecycle, \
               \trie queries, and transaction \
               \building endpoints.\n\n\
               \Contract: verified read endpoints \
               \provide only provable on-chain data \
               \(witnessed UTxOs: TxIn + full TxOut \
               \+ CSMT proof) plus the proofs that \
               \bind it to the snapshot's utxo_root, \
               \and genuinely server-only data the \
               \client cannot derive (snapshot / \
               \chain tip / utxo_root). They never \
               \ship server-side projections - \
               \parsed convenience JSON of data \
               \already present in a witnessed \
               \TxOut. Clients reconstruct \
               \everything else (token id, token \
               \state, request payload) by decoding \
               \the inline datum in the witnessed \
               \TxOut."
        & info . license ?~ "Apache 2.0"

-- | Type alias for Swagger UI endpoint.
type SwaggerAPI =
    SwaggerSchemaUI "swagger-ui" "swagger.json"

-- | Servant server for Swagger UI.
swaggerServer :: Server SwaggerAPI
swaggerServer =
    SwaggerUI.swaggerSchemaUIServer swaggerDoc

-- | Render Swagger documentation as pretty JSON.
renderSwaggerJSON :: LazyByteString
renderSwaggerJSON = encodePretty swaggerDoc
