-- |
-- Module      : Cardano.MPFS.HTTP.Server
-- Description : Servant server wiring for MPFS HTTP API
-- License     : Apache-2.0
--
-- Wires the Servant handlers to the 'Context' record
-- of functions. Each handler extracts the relevant
-- interface from 'Context' and delegates to it.
module Cardano.MPFS.HTTP.Server
    ( -- * Application
      mkApp

      -- * Handlers
    , statusHandler
    , tokensHandler
    , tokenHandler
    , tokenRootHandler
    , tokenFactHandler
    , tokenProofHandler
    , tokenRequestsHandler
    ) where

import Control.Monad.IO.Class (liftIO)
import Data.Proxy (Proxy (..))
import Servant
    ( Application
    , Handler
    , err404
    , serve
    , throwError
    , (:<|>) (..)
    )

import Cardano.MPFS.Context (Context (..))
import Cardano.MPFS.Core.Types
    ( BlockId (..)
    , Root (..)
    , SlotNo (..)
    )
import Cardano.MPFS.HTTP.API (API)
import Cardano.MPFS.HTTP.Encoding (Hex (..))
import Cardano.MPFS.HTTP.Types
    ( RequestJSON
    , StatusResponse (..)
    , TokenIdJSON (..)
    , TokenStateJSON
    , requestToJSON
    , tokenStateToJSON
    )
import Cardano.MPFS.Indexer qualified as Indexer
import Cardano.MPFS.State qualified as St
import Cardano.MPFS.Trie qualified as Trie

-- | Build a WAI 'Application' from a 'Context IO'.
mkApp :: Context IO -> Application
mkApp ctx =
    serve (Proxy @API)
        $ statusHandler ctx
            :<|> tokensHandler ctx
            :<|> tokenHandler ctx
            :<|> tokenRootHandler ctx
            :<|> tokenFactHandler ctx
            :<|> tokenProofHandler ctx
            :<|> tokenRequestsHandler ctx

-- | Handler for @GET \/status@.
statusHandler :: Context IO -> Handler StatusResponse
statusHandler ctx = do
    tip <- liftIO $ Indexer.getTip (indexer ctx)
    mcp <-
        liftIO
            $ St.getCheckpoint
                (St.checkpoints (state ctx))
    pure
        StatusResponse
            { tipSlot =
                unSlotNo (Indexer.tipSlot tip)
            , tipBlockId =
                Hex
                    ( unBlockId
                        (Indexer.tipBlockId tip)
                    )
            , checkpointSlot =
                fmap (unSlotNo . fst) mcp
            , checkpointBlockId =
                fmap (Hex . unBlockId . snd) mcp
            }

-- | Handler for @GET \/tokens@.
tokensHandler
    :: Context IO -> Handler [TokenIdJSON]
tokensHandler ctx = do
    tids <-
        liftIO
            $ St.listTokens (St.tokens (state ctx))
    pure (map TokenIdJSON tids)

-- | Handler for @GET \/tokens\/:id@.
tokenHandler
    :: Context IO
    -> TokenIdJSON
    -> Handler TokenStateJSON
tokenHandler ctx (TokenIdJSON tid) = do
    mts <-
        liftIO
            $ St.getToken (St.tokens (state ctx)) tid
    case mts of
        Nothing -> throwError err404
        Just ts -> pure (tokenStateToJSON ts)

-- | Handler for @GET \/tokens\/:id\/root@.
tokenRootHandler
    :: Context IO
    -> TokenIdJSON
    -> Handler Hex
tokenRootHandler ctx (TokenIdJSON tid) =
    liftIO
        $ Trie.withTrie (trieManager ctx) tid
        $ \trie -> do
            Root r <- Trie.getRoot trie
            pure (Hex r)

-- | Handler for @GET \/tokens\/:id\/facts\/:key@.
tokenFactHandler
    :: Context IO
    -> TokenIdJSON
    -> Hex
    -> Handler Hex
tokenFactHandler ctx (TokenIdJSON tid) (Hex k) = do
    mv <-
        liftIO
            $ Trie.withTrie (trieManager ctx) tid
            $ \trie -> Trie.lookup trie k
    case mv of
        Nothing -> throwError err404
        Just v -> pure (Hex v)

-- | Handler for @GET \/tokens\/:id\/proofs\/:key@.
tokenProofHandler
    :: Context IO
    -> TokenIdJSON
    -> Hex
    -> Handler Hex
tokenProofHandler ctx (TokenIdJSON tid) (Hex k) = do
    mp <-
        liftIO
            $ Trie.withTrie (trieManager ctx) tid
            $ \trie -> Trie.getProof trie k
    case mp of
        Nothing -> throwError err404
        Just p -> pure (Hex (Trie.unProof p))

-- | Handler for @GET \/tokens\/:id\/requests@.
tokenRequestsHandler
    :: Context IO
    -> TokenIdJSON
    -> Handler [RequestJSON]
tokenRequestsHandler ctx (TokenIdJSON tid) = do
    reqs <-
        liftIO
            $ St.requestsByToken
                (St.requests (state ctx))
                tid
    pure (map requestToJSON reqs)
