{-# LANGUAGE DataKinds #-}

-- |
-- Module      : Cardano.MPFS.CLI.Sign
-- Description : Local witnessing of an unsigned Conway transaction.
--
-- Takes the unsigned transaction CBOR produced by a workflow and an
-- Ed25519 signing key, adds a verification-key witness, and re-serializes
-- to submission-ready CBOR — the same witness shape the @POST /submit@
-- e2e test uses. This is a general Cardano signing primitive, not MPFS
-- protocol logic.
module Cardano.MPFS.CLI.Sign
    ( SignError (..)
    , ConwayTx
    , signTx
    ) where

import Cardano.Crypto.DSIGN
    ( Ed25519DSIGN
    , SignKeyDSIGN
    , deriveVerKeyDSIGN
    )
import Cardano.Ledger.Api.Tx (Tx, addrTxWitsL, txIdTx, witsTxL)
import Cardano.Ledger.Api.Tx.In (TxId (..))
import Cardano.Ledger.Binary
    ( DecoderError
    , decodeFull'
    , natVersion
    , serialize'
    )
import Cardano.Ledger.Conway (ConwayEra)
import Cardano.Ledger.Core (extractHash)
import Cardano.Ledger.Keys
    ( VKey (..)
    , WitVKey (..)
    , asWitness
    , signedDSIGN
    )
import Data.Bifunctor (first)
import Data.ByteString (ByteString)
import Data.Set qualified as Set
import Lens.Micro ((%~), (&))

-- | A Conway-era transaction.
type ConwayTx = Tx ConwayEra

-- | Why an unsigned transaction could not be signed.
newtype SignError
    = -- | The unsigned CBOR did not decode as a Conway transaction.
      TxDecodeError DecoderError
    deriving stock (Show)

-- | Witness an unsigned Conway transaction CBOR with the given key and
-- return submission-ready CBOR.
signTx
    :: SignKeyDSIGN Ed25519DSIGN
    -> ByteString
    -> Either SignError ByteString
signTx sk unsignedCbor = do
    tx <- first TxDecodeError (decodeFull' (natVersion @11) unsignedCbor)
    pure (serialize' (natVersion @11) (addKeyWitness sk tx))

-- | Add a single vkey witness for the key to the transaction.
addKeyWitness :: SignKeyDSIGN Ed25519DSIGN -> ConwayTx -> ConwayTx
addKeyWitness sk tx =
    tx & witsTxL . addrTxWitsL %~ Set.insert wit
  where
    vk = VKey (deriveVerKeyDSIGN sk)
    wit = case txIdTx tx of
        TxId h -> WitVKey (asWitness vk) (signedDSIGN sk (extractHash h))
