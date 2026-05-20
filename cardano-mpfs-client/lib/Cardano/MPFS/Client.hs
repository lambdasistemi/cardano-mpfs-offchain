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
    , UpdateParams (..)
    , bootFacts
    , requestInsertFacts
    , requestDeleteFacts
    , requestUpdateTx
    , rejectTx
    , updateTx

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
    , EndFacts (..)
    , UnverifiedPParams (..)
    , VerifiedBootFacts
    , VerifiedRequestInsertFacts
    , VerifiedRequestDeleteFacts
    , VerifiedEndFacts
    , verifiedBootFacts
    , verifiedRequestInsertFacts
    , verifiedRequestDeleteFacts
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
    , verifyRequestInsertFacts
    , verifyRequestDeleteFacts
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

      -- * Local cage builders
    , CageConfig (..)
    , WalletPolicy (..)
    , PolicyViolationDetail (..)
    , BuildError (..)
    , requestInsertCageTx
    , requestDeleteCageTx
    , retractCageTx
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
    )
import Cardano.MPFS.Client.Cage.Retract
    ( retractCageTx
    )
import Cardano.MPFS.Client.Facts
    ( BootFacts (..)
    , EndFacts (..)
    , RequestDeleteFacts (..)
    , RequestInsertFacts (..)
    , UnverifiedPParams (..)
    , VerifiedBootFacts
    , VerifiedEndFacts
    , VerifiedRequestDeleteFacts
    , VerifiedRequestInsertFacts
    , verifiedBootFacts
    , verifiedEndFacts
    , verifiedRequestDeleteFacts
    , verifiedRequestInsertFacts
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
    , UpdateParams (..)
    , VerifierMode (..)
    , bootFacts
    , rejectTx
    , requestDeleteFacts
    , requestInsertFacts
    , requestUpdateTx
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
    , verifyRequestDeleteFacts
    , verifyRequestInsertFacts
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
