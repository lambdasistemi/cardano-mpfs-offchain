-- |
-- Module      : Cardano.MPFS.Client
-- Description : Public surface of the MPFS verification client.
--
-- Re-exports the snapshot JSON contract, the per-endpoint response
-- envelopes, the offline verifiers, and the tutorial-shaped test
-- DSL for consumers that want one import. Downstream wallet
-- integrators can reuse the same DSL combinators in their own
-- hspec suites to pair 'shouldAccept' / 'shouldRejectWith'
-- scenarios per endpoint.
module Cardano.MPFS.Client
    ( -- * Snapshot
      VerificationSnapshot (..)
    , ChainPoint (..)
    , Hex (..)
    , statusSnapshot

      -- * HTTP transport
    , BaseUrl (..)
    , Scheme (..)
    , VerifierMode (..)
    , MpfsHttp (..)
    , ClientError (..)
    , BootTxParams (..)
    , RequestInsertParams (..)
    , RequestDeleteParams (..)
    , RequestUpdateParams (..)
    , RetractParams (..)
    , RejectParams (..)
    , UpdateParams (..)
    , EndParams (..)
    , bootTx
    , requestInsertTx
    , requestDeleteTx
    , requestUpdateTx
    , retractTx
    , rejectTx
    , updateTx
    , endTx

      -- * Response envelopes
    , TxIn (..)
    , WitnessedUtxo (..)
    , TrieFact (..)
    , BootProof (..)
    , RequestProof (..)
    , RetractProof (..)
    , RejectProof (..)
    , EndProof (..)
    , UpdateProof (..)
    , BootTxResponse (..)
    , RequestTxResponse (..)
    , RetractTxResponse (..)
    , RejectTxResponse (..)
    , EndTxResponse (..)
    , UpdateTxResponse (..)

      -- * Read response envelopes
    , TokenState (..)
    , Request (..)
    , WitnessedTokenState (..)
    , WitnessedRequest (..)
    , FactWitness (..)
    , TokenResponse (..)
    , FactResponse (..)
    , ProofResponse (..)
    , RequestsResponse (..)

      -- * Verification
    , VerifyError (..)
    , verifyVerificationSnapshot
    , verifyBootTxResponse
    , verifyRequestTxResponse
    , verifyRetractTxResponse
    , verifyRejectTxResponse
    , verifyEndTxResponse
    , verifyUpdateTxResponse
    , verifyTokenResponse
    , verifyFactResponse
    , verifyProofResponse
    , verifyRequestsResponse

      -- * Test DSL (re-exported from "Cardano.MPFS.Client.Verify.DSL")
    , shouldAccept
    , shouldRejectWith
    , ErrorMatcher
    , csmtReplayFailedAt
    , mpfReplayFailedAt
    , txBindingFailedAt
    , malformedHexAt
    , wrongHexLengthAt
    , withReason

      -- * Forgery helpers
    , flipByteInHex
    , swapHexTo
    , forgeWitnessedUtxoProof
    , forgeWitnessedUtxoTxOut
    , forgeTrieFactValue
    , dropTrieFactToExclusion
    , promoteTrieFactToInclusion

      -- * Forgery DSL (operational free-monad)
    , CsmtForge
    , TrieForge
    , flipProof
    , flipTxOut
    , flipSnapshotRoot
    , flipTrieValue
    , dropToExclusion
    , flipTrieRoot
    , runForgeBoot
    , runForgeRequest
    , runForgeRetract
    , runForgeReject
    , runForgeEnd
    , runForgeUpdate
    , runForgeUpdateTrie
    ) where

import Cardano.MPFS.Client.Bundle
    ( BootProof (..)
    , BootTxResponse (..)
    , EndProof (..)
    , EndTxResponse (..)
    , RejectProof (..)
    , RejectTxResponse (..)
    , RequestProof (..)
    , RequestTxResponse (..)
    , RetractProof (..)
    , RetractTxResponse (..)
    , TrieFact (..)
    , TxIn (..)
    , UpdateProof (..)
    , UpdateTxResponse (..)
    , WitnessedUtxo (..)
    )
import Cardano.MPFS.Client.Http
    ( BaseUrl (..)
    , BootTxParams (..)
    , ClientError (..)
    , EndParams (..)
    , MpfsHttp (..)
    , RejectParams (..)
    , RequestDeleteParams (..)
    , RequestInsertParams (..)
    , RequestUpdateParams (..)
    , RetractParams (..)
    , Scheme (..)
    , UpdateParams (..)
    , VerifierMode (..)
    , bootTx
    , endTx
    , rejectTx
    , requestDeleteTx
    , requestInsertTx
    , requestUpdateTx
    , retractTx
    , updateTx
    )
import Cardano.MPFS.Client.Read
    ( FactResponse (..)
    , FactWitness (..)
    , ProofResponse (..)
    , Request (..)
    , RequestsResponse (..)
    , TokenResponse (..)
    , TokenState (..)
    , WitnessedRequest (..)
    , WitnessedTokenState (..)
    )
import Cardano.MPFS.Client.Snapshot
    ( ChainPoint (..)
    , Hex (..)
    , VerificationSnapshot (..)
    , statusSnapshot
    )
import Cardano.MPFS.Client.Verify
    ( VerifyError (..)
    , verifyBootTxResponse
    , verifyEndTxResponse
    , verifyFactResponse
    , verifyProofResponse
    , verifyRejectTxResponse
    , verifyRequestTxResponse
    , verifyRequestsResponse
    , verifyRetractTxResponse
    , verifyTokenResponse
    , verifyUpdateTxResponse
    , verifyVerificationSnapshot
    )
import Cardano.MPFS.Client.Verify.DSL
    ( CsmtForge
    , ErrorMatcher
    , TrieForge
    , csmtReplayFailedAt
    , dropToExclusion
    , dropTrieFactToExclusion
    , flipByteInHex
    , flipProof
    , flipSnapshotRoot
    , flipTrieRoot
    , flipTrieValue
    , flipTxOut
    , forgeTrieFactValue
    , forgeWitnessedUtxoProof
    , forgeWitnessedUtxoTxOut
    , malformedHexAt
    , mpfReplayFailedAt
    , promoteTrieFactToInclusion
    , runForgeBoot
    , runForgeEnd
    , runForgeReject
    , runForgeRequest
    , runForgeRetract
    , runForgeUpdate
    , runForgeUpdateTrie
    , shouldAccept
    , shouldRejectWith
    , swapHexTo
    , txBindingFailedAt
    , withReason
    , wrongHexLengthAt
    )
