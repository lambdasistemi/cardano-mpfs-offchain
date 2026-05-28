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
    , RejectParams (..)
    , UpdateFactsParams (..)
    , bootFacts
    , requestInsertFacts
    , requestDeleteFacts
    , requestUpdateFacts
    , updateFacts
    , rejectTx

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
    , RequestInsertFacts (..)
    , RequestDeleteFacts (..)
    , RequestUpdateFacts (..)
    , RejectFacts (..)
    , EndFacts (..)
    , UpdateFacts (..)
    , FactPresentFacts (..)
    , FactAbsentFacts (..)
    , UnverifiedPParams (..)
    , VerifiedBootFacts
    , VerifiedRequestInsertFacts
    , VerifiedRequestDeleteFacts
    , VerifiedRequestUpdateFacts
    , VerifiedUpdateFacts
    , VerifiedEndFacts
    , VerifiedFactPresentFacts
    , VerifiedFactAbsentFacts
    , verifiedBootFacts
    , verifiedRequestInsertFacts
    , verifiedRequestDeleteFacts
    , verifiedRequestUpdateFacts
    , verifiedUpdateFacts
    , verifiedEndFacts
    , verifiedFactPresentFacts
    , verifiedFactAbsentFacts
    , RequestTxResponse (..)
    , RetractTxResponse (..)
    , RejectTxResponse (..)
    , EndTxResponse (..)
    , UpdateTxResponse (..)

      -- * Verification
    , VerifyError (..)
    , verifyVerificationSnapshot
    , verifyBootFacts
    , verifyRequestInsertFacts
    , verifyRequestDeleteFacts
    , verifyRequestUpdateFacts
    , verifyUpdateFacts
    , verifyEndFacts
    , verifyFactPresentFacts
    , verifyFactAbsentFacts
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
    , forgeEntryProof
    , forgeEntryTxOut
    , forgeFactsTrieValue
    , forgeFactsTrieProof

      -- * Forgery DSL (operational free-monad)
    , CsmtForge
    , TrieForge
    , flipProof
    , flipTxOut
    , flipSnapshotRoot
    , flipTrieValue
    , dropToExclusion
    , flipTrieRoot
    , flipTrieProof
    , runForgeBoot
    , runForgeRequest
    , runForgeRetract
    , runForgeReject
    , runForgeEnd
    , runForgeUpdate
    , runForgeUpdateTrie
    , runForgeUpdateFacts
    , runForgeUpdateFactsTrie

      -- * Local cage builders
    , CageConfig (..)
    , WalletPolicy (..)
    , PolicyViolationDetail (..)
    , BuildError (..)
    , requestInsertCageTx
    , requestDeleteCageTx
    , requestUpdateCageTx
    , retractCageTx
    , updateCageTx
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
import Cardano.MPFS.Client.Cage.BuildError
    ( BuildError (..)
    )
import Cardano.MPFS.Client.Cage.Config
    ( CageConfig (..)
    )
import Cardano.MPFS.Client.Cage.Policy
    ( PolicyViolationDetail (..)
    , WalletPolicy (..)
    )
import Cardano.MPFS.Client.Cage.Request
    ( requestDeleteCageTx
    , requestInsertCageTx
    , requestUpdateCageTx
    )
import Cardano.MPFS.Client.Cage.Retract
    ( retractCageTx
    )
import Cardano.MPFS.Client.Cage.Update
    ( updateCageTx
    )
import Cardano.MPFS.Client.Facts
    ( BootFacts (..)
    , EndFacts (..)
    , FactAbsentFacts (..)
    , FactPresentFacts (..)
    , RejectFacts (..)
    , RequestDeleteFacts (..)
    , RequestInsertFacts (..)
    , RequestUpdateFacts (..)
    , UnverifiedPParams (..)
    , UpdateFacts (..)
    , VerifiedBootFacts
    , VerifiedEndFacts
    , VerifiedFactAbsentFacts
    , VerifiedFactPresentFacts
    , VerifiedRequestDeleteFacts
    , VerifiedRequestInsertFacts
    , VerifiedRequestUpdateFacts
    , VerifiedUpdateFacts
    , verifiedBootFacts
    , verifiedEndFacts
    , verifiedFactAbsentFacts
    , verifiedFactPresentFacts
    , verifiedRequestDeleteFacts
    , verifiedRequestInsertFacts
    , verifiedRequestUpdateFacts
    , verifiedUpdateFacts
    , verifyFactAbsentFacts
    , verifyFactPresentFacts
    )
import Cardano.MPFS.Client.Http
    ( BaseUrl (..)
    , BootFactsParams (..)
    , ClientError (..)
    , MpfsHttp (..)
    , RejectParams (..)
    , RequestDeleteParams (..)
    , RequestInsertParams (..)
    , RequestUpdateParams (..)
    , Scheme (..)
    , UpdateFactsParams (..)
    , VerifierMode (..)
    , bootFacts
    , rejectTx
    , requestDeleteFacts
    , requestInsertFacts
    , requestUpdateFacts
    , updateFacts
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
    , verifyRequestDeleteFacts
    , verifyRequestInsertFacts
    , verifyRequestTxResponse
    , verifyRequestUpdateFacts
    , verifyRetractTxResponse
    , verifyUpdateFacts
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
    , flipTrieProof
    , flipTrieRoot
    , flipTrieValue
    , flipTxOut
    , forgeEntryProof
    , forgeEntryTxOut
    , forgeFactsTrieProof
    , forgeFactsTrieValue
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
    , runForgeUpdateFacts
    , runForgeUpdateFactsTrie
    , runForgeUpdateTrie
    , shouldAccept
    , shouldRejectWith
    , swapHexTo
    , trustedRootMismatchAt
    , txBindingFailedAt
    , withReason
    , wrongHexLengthAt
    )
