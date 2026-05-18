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
    , BootFactsParams (..)
    , RequestInsertParams (..)
    , RequestDeleteParams (..)
    , RequestUpdateParams (..)
    , RetractParams (..)
    , RejectParams (..)
    , UpdateParams (..)
    , EndParams (..)
    , bootFacts
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
    , BootFacts (..)
    , EndFacts (..)
    , UnverifiedPParams (..)
    , VerifiedBootFacts
    , VerifiedEndFacts
    , verifiedBootFacts
    , verifiedEndFacts
    , RequestTxResponse (..)
    , RetractTxResponse (..)
    , RejectTxResponse (..)
    , EndTxResponse (..)
    , UpdateTxResponse (..)

      -- * Verification
    , VerifyError (..)
    , verifyVerificationSnapshot
    , verifyBootFacts
    , verifyEndFacts
    , verifyBootTxResponse
    , verifyRequestTxResponse
    , verifyRetractTxResponse
    , verifyRejectTxResponse
    , verifyEndTxResponse
    , verifyUpdateTxResponse

      -- * Test DSL (re-exported from "Cardano.MPFS.Client.Verify.DSL")
    , shouldAccept
    , shouldRejectWith
    , ErrorMatcher
    , csmtReplayFailedAt
    , mpfReplayFailedAt
    , txBindingFailedAt
    , trustedRootMismatchAt
    , malformedHexAt
    , wrongHexLengthAt
    , withReason

      -- * Forgery helpers
    , flipByteInHex
    , flipApiHexMidByte
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
import Cardano.MPFS.Client.Facts
    ( BootFacts (..)
    , EndFacts (..)
    , UnverifiedPParams (..)
    , VerifiedBootFacts
    , VerifiedEndFacts
    , verifiedBootFacts
    , verifiedEndFacts
    )
import Cardano.MPFS.Client.Http
    ( BaseUrl (..)
    , BootFactsParams (..)
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
    , bootFacts
    , endTx
    , rejectTx
    , requestDeleteTx
    , requestInsertTx
    , requestUpdateTx
    , retractTx
    , updateTx
    )
import Cardano.MPFS.Client.Snapshot
    ( ChainPoint (..)
    , Hex (..)
    , VerificationSnapshot (..)
    , statusSnapshot
    )
import Cardano.MPFS.Client.Verify
    ( VerifyError (..)
    , verifyBootFacts
    , verifyBootTxResponse
    , verifyEndFacts
    , verifyEndTxResponse
    , verifyRejectTxResponse
    , verifyRequestTxResponse
    , verifyRetractTxResponse
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
    , flipApiHexMidByte
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
    , trustedRootMismatchAt
    , txBindingFailedAt
    , withReason
    , wrongHexLengthAt
    )
