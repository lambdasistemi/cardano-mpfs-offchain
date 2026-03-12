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
    ) where

import Control.Monad.IO.Class (liftIO)
import Data.Proxy (Proxy (..))
import Servant (Application, Handler, serve)

import Cardano.MPFS.Context (Context (..))
import Cardano.MPFS.Core.Types
    ( BlockId (..)
    , SlotNo (..)
    )
import Cardano.MPFS.HTTP.API (API)
import Cardano.MPFS.HTTP.Encoding (Hex (..))
import Cardano.MPFS.HTTP.Types (StatusResponse (..))
import Cardano.MPFS.Indexer qualified as Indexer
import Cardano.MPFS.State qualified as St

-- | Build a WAI 'Application' from a 'Context IO'.
mkApp :: Context IO -> Application
mkApp ctx =
    serve (Proxy @API) (statusHandler ctx)

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
