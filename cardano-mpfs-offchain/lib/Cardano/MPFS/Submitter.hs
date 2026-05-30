-- |
-- Module      : Cardano.MPFS.Submitter
-- Description : Transaction submission interface
-- License     : Apache-2.0
--
-- Record-of-functions interface for submitting signed
-- transactions. The real implementation uses node-to-client
-- LocalTxSubmission ("Cardano.MPFS.Submitter.N2C"); the
-- mock records submitted transactions in memory.
module Cardano.MPFS.Submitter
    ( -- * Submitter interface
      Submitter (..)

      -- * Result type
    , SubmitResult (..)
    ) where

import Data.ByteString (ByteString)

import Cardano.Tx.Ledger (ConwayTx)

import Cardano.MPFS.Core.Types (TxId)

-- | Result of submitting a transaction.
data SubmitResult
    = -- | Transaction accepted into the mempool
      Submitted !TxId
    | -- | Transaction was rejected
      Rejected
        !ByteString
        -- ^ Rejection reason (UTF-8 encoded)
    deriving stock (Show)

-- | Interface for submitting transactions to the
-- blockchain.
newtype Submitter m = Submitter
    { submitTx
        :: ConwayTx -> m SubmitResult
    -- ^ Submit a signed transaction
    }
