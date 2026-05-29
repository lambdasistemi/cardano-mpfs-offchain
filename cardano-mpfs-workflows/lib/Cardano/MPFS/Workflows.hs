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
    , insertFact
    , updateFact
    , deleteFact
    , applyRequests
    , retractRequest
    , rejectExpired

      -- * Re-exported request types
    , BootRequest (..)
    , InsertRequest (..)
    , UpdateValueRequest (..)
    , DeleteRequest (..)
    , UpdateRequest (..)
    , RetractRequest (..)
    , RejectRequest (..)
    ) where

import Cardano.MPFS.API.Types
    ( BootRequest (..)
    , DeleteRequest (..)
    , InsertRequest (..)
    , RejectRequest (..)
    , RetractRequest (..)
    , UpdateRequest (..)
    , UpdateValueRequest (..)
    )
import Cardano.MPFS.Client.Cage.Boot (bootCageTx)
import Cardano.MPFS.Client.Cage.Reject (rejectCageTx)
import Cardano.MPFS.Client.Cage.Request
    ( requestDeleteCageTx
    , requestInsertCageTx
    , requestUpdateCageTx
    )
import Cardano.MPFS.Client.Cage.Retract (retractCageTx)
import Cardano.MPFS.Client.Cage.Serialize (serializeCageTx)
import Cardano.MPFS.Client.Cage.Update (updateCageTx)
import Cardano.MPFS.Client.Verify
    ( verifyBootFacts
    , verifyRejectFacts
    , verifyRequestDeleteFacts
    , verifyRequestInsertFacts
    , verifyRequestUpdateFacts
    , verifyRetractFacts
    , verifyUpdateFacts
    )
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

-- | Request a key insertion (the @request\/insert@ operation): POST
-- the insert request to @\/facts\/request\/insert@, verify the
-- returned facts against the trusted root, and build the request
-- transaction locally for the requester to sign.
insertFact
    :: HttpClient
    -> WorkflowsConfig
    -> InsertRequest
    -> IO (Either WorkflowError UnsignedTx)
insertFact http WorkflowsConfig{..} req =
    runFactsWorkflow
        http
        "/facts/request/insert"
        req
        (verifyRequestInsertFacts wcTrustedRoot)
        (fmap serializeCageTx . requestInsertCageTx wcCage wcPolicy)

-- | Request a key update (the @request\/update@ operation): POST the
-- update request to @\/facts\/request\/update@, verify the returned
-- facts against the trusted root, and build the request transaction
-- locally for the requester to sign.
updateFact
    :: HttpClient
    -> WorkflowsConfig
    -> UpdateValueRequest
    -> IO (Either WorkflowError UnsignedTx)
updateFact http WorkflowsConfig{..} req =
    runFactsWorkflow
        http
        "/facts/request/update"
        req
        (verifyRequestUpdateFacts wcTrustedRoot)
        (fmap serializeCageTx . requestUpdateCageTx wcCage wcPolicy)

-- | Request a key deletion (the @request\/delete@ operation): POST
-- the delete request to @\/facts\/request\/delete@, verify the
-- returned facts against the trusted root, and build the request
-- transaction locally for the requester to sign.
deleteFact
    :: HttpClient
    -> WorkflowsConfig
    -> DeleteRequest
    -> IO (Either WorkflowError UnsignedTx)
deleteFact http WorkflowsConfig{..} req =
    runFactsWorkflow
        http
        "/facts/request/delete"
        req
        (verifyRequestDeleteFacts wcTrustedRoot)
        (fmap serializeCageTx . requestDeleteCageTx wcCage wcPolicy)

-- | Apply pending requests (the oracle @update@ operation): POST the
-- token to @\/facts\/update@, verify the returned update facts
-- against the trusted root, and build the batch update transaction
-- locally for the cage owner to sign. This advances the trie root by
-- folding the pending requests' MPF proofs.
applyRequests
    :: HttpClient
    -> WorkflowsConfig
    -> UpdateRequest
    -> IO (Either WorkflowError UnsignedTx)
applyRequests http WorkflowsConfig{..} req =
    runFactsWorkflow
        http
        "/facts/update"
        req
        (verifyUpdateFacts wcTrustedRoot)
        (fmap serializeCageTx . updateCageTx wcCage wcPolicy)

-- | Retract a pending request (the @retract@ operation): POST the
-- request UTxO reference to @\/facts\/retract@, verify the returned
-- facts against the trusted root, and build the retract transaction
-- locally for the original requester to sign.
retractRequest
    :: HttpClient
    -> WorkflowsConfig
    -> RetractRequest
    -> IO (Either WorkflowError UnsignedTx)
retractRequest http WorkflowsConfig{..} req =
    runFactsWorkflow
        http
        "/facts/retract"
        req
        (verifyRetractFacts wcTrustedRoot)
        (fmap serializeCageTx . retractCageTx wcCage wcPolicy)

-- | Reject expired pending requests (the @reject@ operation): POST
-- the token to @\/facts\/reject@, verify the returned facts against
-- the trusted root, and build the reject transaction locally for the
-- cage owner to sign.
rejectExpired
    :: HttpClient
    -> WorkflowsConfig
    -> RejectRequest
    -> IO (Either WorkflowError UnsignedTx)
rejectExpired http WorkflowsConfig{..} req =
    runFactsWorkflow
        http
        "/facts/reject"
        req
        (verifyRejectFacts wcTrustedRoot)
        (fmap serializeCageTx . rejectCageTx wcCage wcPolicy)
