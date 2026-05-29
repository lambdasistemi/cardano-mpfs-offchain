-- |
-- Module      : Cardano.MPFS.Workflows
-- Description : Public facts-to-transaction workflow surface.
--
-- Each workflow is a one-liner over
-- 'Cardano.MPFS.Workflows.Internal.runFactsWorkflow': it pairs a
-- facts endpoint with its verifier and cage builder, returning a
-- submission-ready 'UnsignedTx'. The transport is injected as an
-- 'HttpClient' so the same workflows run in a CLI or a browser SPA.
-- This slice ships 'registerToken'; later slices add the remaining
-- workflows and their request types to this surface.
module Cardano.MPFS.Workflows
    ( -- * Transport and configuration
      HttpClient (..)
    , HttpError (..)
    , UnsignedTx (..)
    , WorkflowsConfig (..)
    , WorkflowError (..)

      -- * Workflows
    , registerToken

      -- * Re-exported request types
    , BootRequest (..)
    ) where

import Cardano.MPFS.API.Types (BootRequest (..))
import Cardano.MPFS.Client.Cage.Boot (bootCageTx)
import Cardano.MPFS.Client.Cage.Serialize (serializeCageTx)
import Cardano.MPFS.Client.Verify (verifyBootFacts)
import Cardano.MPFS.Workflows.Internal
    ( HttpClient (..)
    , HttpError (..)
    , UnsignedTx (..)
    , WorkflowError (..)
    , WorkflowsConfig (..)
    , runFactsWorkflow
    )

-- | Register a new cage token (the @boot@ operation): POST the
-- requester address to @\/facts\/boot@, verify the returned boot
-- facts against the trusted root, and build the boot transaction
-- locally.
registerToken
    :: HttpClient
    -> WorkflowsConfig
    -> BootRequest
    -> IO (Either WorkflowError UnsignedTx)
registerToken http WorkflowsConfig{..} req =
    runFactsWorkflow
        http
        "/facts/boot"
        req
        (verifyBootFacts wcTrustedRoot)
        (fmap serializeCageTx . bootCageTx wcCage wcPolicy)
