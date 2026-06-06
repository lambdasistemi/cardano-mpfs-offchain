-- |
-- Module      : Cardano.MPFS.Workflows.Internal
-- Description : Shared core for facts-to-transaction workflows.
--
-- The transport abstraction and the single helper every workflow
-- shares. 'runFactsWorkflow' POSTs a request to a facts endpoint,
-- decodes the proof-bearing response, hands it to a caller-supplied
-- verifier, and then to a caller-supplied builder that returns
-- submission-ready CBOR. The builder is pre-composed with
-- serialization at the call site, so this signature never names a
-- ledger transaction type and this module pulls in no
-- @Cardano.Ledger.*@ dependency.
module Cardano.MPFS.Workflows.Internal
    ( HttpClient (..)
    , HttpError (..)
    , UnsignedTx (..)
    , WorkflowsConfig (..)
    , WorkflowError (..)
    , runFactsWorkflow
    ) where

import Data.Aeson
    ( FromJSON
    , ToJSON
    , eitherDecodeStrict'
    , encode
    )
import Data.Bifunctor (first)
import Data.ByteString (ByteString)
import Data.ByteString.Lazy qualified as BSL
import Data.Text (Text)

import Cardano.MPFS.Client.Cage.BuildError (BuildError)
import Cardano.MPFS.Client.Cage.Config (CageConfig)
import Cardano.MPFS.Client.Cage.Eval (DecodedEvalContext)
import Cardano.MPFS.Client.Cage.Policy (WalletPolicy)
import Cardano.MPFS.Client.TrustedRoot (TrustedRoot)
import Cardano.MPFS.Client.Verify (VerifyError)

-- | A transport-level failure from a facts request: a non-2xx HTTP
-- status with the response body, or a transport error.
data HttpError
    = HttpStatus !Int !ByteString
    | HttpTransport !Text
    deriving stock (Eq, Show)

-- | The HTTP transport, as a record of functions rather than a
-- typeclass, so the CLI and the browser SPA can each supply their
-- own implementation. The function takes a relative path and a
-- strict JSON request body and returns either a transport error or
-- the strict JSON response body.
newtype HttpClient = HttpClient
    { runHttpPost
        :: Text -> ByteString -> IO (Either HttpError ByteString)
    }

-- | Submission-ready Conway transaction CBOR built locally from
-- verified facts.
newtype UnsignedTx = UnsignedTx
    { unsignedTxCbor :: ByteString
    }
    deriving stock (Eq, Show)

-- | Static configuration shared by every workflow: the client-owned
-- cage configuration, the wallet policy caps enforced on the built
-- transaction, and the trusted UTxO-CSMT root verified against.
data WorkflowsConfig = WorkflowsConfig
    { wcCage :: CageConfig
    -- ^ Client-owned cage configuration.
    , wcPolicy :: WalletPolicy
    -- ^ Wallet-side policy caps.
    , wcTrustedRoot :: TrustedRoot
    -- ^ Trusted UTxO-CSMT root.
    , wcEvalContext :: DecodedEvalContext
    -- ^ Trusted ledger evaluation context used to compute real ex-units.
    }

-- | Every way a facts workflow can fail, tagged by the stage that
-- produced it.
data WorkflowError
    = WorkflowHttpError !HttpError
    | WorkflowDecodeError !String
    | WorkflowVerifyError !VerifyError
    | WorkflowBuildError !BuildError
    deriving stock (Show)

-- | Run one facts workflow end to end: POST the request, decode the
-- response, verify it, and build submission-ready CBOR. Each stage's
-- failure is tagged with the matching 'WorkflowError' constructor.
runFactsWorkflow
    :: (ToJSON req, FromJSON facts)
    => HttpClient
    -> Text
    -> req
    -> (facts -> Either VerifyError verified)
    -> (verified -> Either BuildError ByteString)
    -> IO (Either WorkflowError UnsignedTx)
runFactsWorkflow http path req verify build = do
    resp <- runHttpPost http path (BSL.toStrict (encode req))
    pure $ case resp of
        Left e -> Left (WorkflowHttpError e)
        Right bs -> do
            facts <-
                first WorkflowDecodeError (eitherDecodeStrict' bs)
            verified <- first WorkflowVerifyError (verify facts)
            cbor <- first WorkflowBuildError (build verified)
            Right (UnsignedTx cbor)
