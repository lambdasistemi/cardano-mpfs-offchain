{-# LANGUAGE DataKinds #-}

-- |
-- Module      : Cardano.MPFS.HTTP.Types.Facts
-- Description : Server-side conversion helpers for facts responses.
-- License     : Apache-2.0
--
-- Keeps facts-only response assembly out of the legacy
-- @Cardano.MPFS.HTTP.Types@ compatibility surface.
module Cardano.MPFS.HTTP.Types.Facts
    ( mkEndFacts
    ) where

import Data.ByteString (ByteString)

import Cardano.Crypto.Hash.Class qualified as Crypto
import Cardano.Ledger.BaseTypes (TxIx (..))
import Cardano.Ledger.Binary (natVersion, serialize')
import Cardano.Ledger.Hashes (extractHash)
import Cardano.Ledger.TxIn
    ( TxId (..)
    , TxIn (..)
    )

import Cardano.MPFS.API.Encoding (Hex (..))
import Cardano.MPFS.API.Types.Common
    ( UnverifiedPParams (..)
    , UtxoEntryRefOnly (..)
    , UtxoRef (..)
    , UtxoSetWitness (..)
    )
import Cardano.MPFS.API.Types.Facts (EndFacts (..))
import Cardano.MPFS.Core.Types
    ( ConwayEra
    , PParams
    , TokenId
    )
import Cardano.MPFS.HTTP.Types
    ( bundleSnapshotToJSON
    , resolvedWalletInputToUtxoEntry
    , tokenIdToJSON
    )
import Cardano.MPFS.Indexer.Reads (ResolvedUtxoSet)
import Cardano.MPFS.TxBuilder
    ( BundleSnapshot
    , ResolvedWalletInput
    )

-- | Build the facts-only end response from one atomic
-- indexer snapshot, the state UTxO, owner funding UTxOs,
-- the request-set completeness witness, and node protocol
-- parameters.
mkEndFacts
    :: BundleSnapshot
    -> TokenId
    -> ResolvedWalletInput
    -> [ResolvedWalletInput]
    -> ResolvedUtxoSet
    -> PParams ConwayEra
    -> EndFacts
mkEndFacts snap tid stateUtxo walletUtxos requestSet pparams =
    EndFacts
        { efSnapshot = bundleSnapshotToJSON snap
        , efToken = tokenIdToJSON tid
        , efStateUtxo =
            resolvedWalletInputToUtxoEntry stateUtxo
        , efWalletUtxos =
            map resolvedWalletInputToUtxoEntry walletUtxos
        , efRequestSet = requestSetToJSON requestSet
        , efProtocolParameters =
            UnverifiedPParams
                { uppVerified = False
                , uppCbor =
                    Hex
                        ( serialize'
                            (natVersion @11)
                            pparams
                        )
                }
        }

requestSetToJSON :: ResolvedUtxoSet -> UtxoSetWitness
requestSetToJSON (entries, proofBytes) =
    UtxoSetWitness
        { uswEntries = map requestSetEntryToJSON entries
        , uswCompletenessProof = Hex proofBytes
        }

requestSetEntryToJSON
    :: (TxIn, ByteString) -> UtxoEntryRefOnly
requestSetEntryToJSON (txIn, txOutBytes) =
    UtxoEntryRefOnly
        { uerRef = txInToUtxoRef txIn
        , uerTxOutCbor = Hex txOutBytes
        }

txInToUtxoRef :: TxIn -> UtxoRef
txInToUtxoRef (TxIn (TxId sh) (TxIx ix)) =
    UtxoRef
        { urTxId =
            Hex
                ( Crypto.hashToBytes
                    (extractHash sh)
                )
        , urTxIx = fromIntegral ix
        }
