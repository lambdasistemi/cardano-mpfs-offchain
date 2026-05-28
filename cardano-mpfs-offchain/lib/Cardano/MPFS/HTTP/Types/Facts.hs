{-# LANGUAGE DataKinds #-}

-- |
-- Module      : Cardano.MPFS.HTTP.Types.Facts
-- Description : Server-side conversion helpers for facts responses.
-- License     : Apache-2.0
--
-- Keeps facts-only response assembly out of the legacy
-- @Cardano.MPFS.HTTP.Types@ compatibility surface.
module Cardano.MPFS.HTTP.Types.Facts
    ( mkRequestInsertFacts
    , mkRequestDeleteFacts
    , mkRequestUpdateFacts
    , mkUpdateFacts
    , mkRetractFacts
    , mkRejectFacts
    , mkEndFacts
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
import Cardano.MPFS.API.Types.Facts
    ( EndFacts (..)
    , RejectFacts (..)
    , RequestDeleteFacts (..)
    , RequestInsertFacts (..)
    , RequestUpdateFacts (..)
    , RetractFacts (..)
    , TrieFact (..)
    , UpdateFacts (..)
    )
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
import Cardano.MPFS.TxBuilder qualified as Tx

-- | Build the facts-only request-insert response from one
-- atomic indexer snapshot, requester funding UTxOs, the
-- request payload, a submission timestamp, and node protocol
-- parameters.
mkRequestInsertFacts
    :: BundleSnapshot
    -> TokenId
    -> ByteString
    -> ByteString
    -> ByteString
    -> Integer
    -> [ResolvedWalletInput]
    -> PParams ConwayEra
    -> RequestInsertFacts
mkRequestInsertFacts
    snap
    tid
    key
    value
    address
    submittedAt
    walletUtxos
    pparams =
        RequestInsertFacts
            { rifSnapshot = bundleSnapshotToJSON snap
            , rifToken = tokenIdToJSON tid
            , rifKey = Hex key
            , rifValue = Hex value
            , rifAddress = Hex address
            , rifSubmittedAt = submittedAt
            , rifWalletUtxos =
                map resolvedWalletInputToUtxoEntry walletUtxos
            , rifProtocolParameters =
                pparamsToJSON pparams
            }

-- | Build the facts-only request-delete response from one
-- atomic indexer snapshot, requester funding UTxOs, the
-- request payload, a submission timestamp, and node protocol
-- parameters.
mkRequestDeleteFacts
    :: BundleSnapshot
    -> TokenId
    -> ByteString
    -> ByteString
    -> ByteString
    -> Integer
    -> [ResolvedWalletInput]
    -> PParams ConwayEra
    -> RequestDeleteFacts
mkRequestDeleteFacts
    snap
    tid
    key
    value
    address
    submittedAt
    walletUtxos
    pparams =
        RequestDeleteFacts
            { rdfSnapshot = bundleSnapshotToJSON snap
            , rdfToken = tokenIdToJSON tid
            , rdfKey = Hex key
            , rdfValue = Hex value
            , rdfAddress = Hex address
            , rdfSubmittedAt = submittedAt
            , rdfWalletUtxos =
                map resolvedWalletInputToUtxoEntry walletUtxos
            , rdfProtocolParameters =
                pparamsToJSON pparams
            }

-- | Build the facts-only request-update response from one
-- atomic indexer snapshot, requester funding UTxOs, the
-- request payload, a submission timestamp, and node protocol
-- parameters.
mkRequestUpdateFacts
    :: BundleSnapshot
    -> TokenId
    -> ByteString
    -> ByteString
    -> ByteString
    -> ByteString
    -> Integer
    -> [ResolvedWalletInput]
    -> PParams ConwayEra
    -> RequestUpdateFacts
mkRequestUpdateFacts
    snap
    tid
    key
    oldValue
    newValue
    address
    submittedAt
    walletUtxos
    pparams =
        RequestUpdateFacts
            { rufSnapshot = bundleSnapshotToJSON snap
            , rufToken = tokenIdToJSON tid
            , rufKey = Hex key
            , rufOldValue = Hex oldValue
            , rufNewValue = Hex newValue
            , rufAddress = Hex address
            , rufSubmittedAt = submittedAt
            , rufWalletUtxos =
                map resolvedWalletInputToUtxoEntry walletUtxos
            , rufProtocolParameters =
                pparamsToJSON pparams
            }

-- | Build the facts-only update response from one atomic
-- indexer snapshot, the state UTxO, pending request UTxOs,
-- wallet funding UTxOs, trie facts, and node protocol
-- parameters.
mkUpdateFacts
    :: BundleSnapshot
    -> TokenId
    -> ResolvedWalletInput
    -> [ResolvedWalletInput]
    -> [ResolvedWalletInput]
    -> ByteString
    -> [Tx.TrieFact]
    -> Integer
    -> PParams ConwayEra
    -> UpdateFacts
mkUpdateFacts
    snap
    tid
    stateUtxo
    requestUtxos
    walletUtxos
    trieRoot
    trieFacts
    validityUpperSlot
    pparams =
        UpdateFacts
            { ufSnapshot = bundleSnapshotToJSON snap
            , ufToken = tokenIdToJSON tid
            , ufStateUtxo =
                resolvedWalletInputToUtxoEntry stateUtxo
            , ufRequestUtxos =
                map resolvedWalletInputToUtxoEntry requestUtxos
            , ufWalletUtxos =
                map resolvedWalletInputToUtxoEntry walletUtxos
            , ufTrieRoot = Hex trieRoot
            , ufTrieFacts = map trieFactToJSON trieFacts
            , ufValidityUpperSlot = validityUpperSlot
            , ufProtocolParameters = pparamsToJSON pparams
            }

-- | Build the facts-only retract response from one atomic
-- indexer snapshot, the named request UTxO, the cage state
-- UTxO, the requester funding UTxOs, the server-derived Phase
-- 2 validity slot bounds, and node protocol parameters.
mkRetractFacts
    :: BundleSnapshot
    -> TokenId
    -> ResolvedWalletInput
    -> ResolvedWalletInput
    -> [ResolvedWalletInput]
    -> Integer
    -> Integer
    -> PParams ConwayEra
    -> RetractFacts
mkRetractFacts
    snap
    tid
    requestUtxo
    stateUtxo
    walletUtxos
    startSlot
    endSlot
    pparams =
        RetractFacts
            { rfSnapshot = bundleSnapshotToJSON snap
            , rfToken = tokenIdToJSON tid
            , rfRequestUtxo =
                resolvedWalletInputToUtxoEntry requestUtxo
            , rfStateUtxo =
                resolvedWalletInputToUtxoEntry stateUtxo
            , rfWalletUtxos =
                map resolvedWalletInputToUtxoEntry walletUtxos
            , rfValidityStartSlot = startSlot
            , rfValidityEndSlot = endSlot
            , rfProtocolParameters =
                pparamsToJSON pparams
            }

-- | Build the facts-only reject response from one atomic
-- indexer snapshot, the cage state UTxO, the rejectable
-- request UTxOs, the requester funding UTxOs, the
-- server-derived Phase 3 validity slot bounds, and node
-- protocol parameters. Per Q-S2-001 no trie root or trie
-- facts are emitted because reject leaves the state root
-- unchanged.
mkRejectFacts
    :: BundleSnapshot
    -> TokenId
    -> ResolvedWalletInput
    -> [ResolvedWalletInput]
    -> [ResolvedWalletInput]
    -> Integer
    -> Integer
    -> PParams ConwayEra
    -> RejectFacts
mkRejectFacts
    snap
    tid
    stateUtxo
    requestUtxos
    walletUtxos
    lowerSlot
    upperSlot
    pparams =
        RejectFacts
            { rfSnapshot = bundleSnapshotToJSON snap
            , rfToken = tokenIdToJSON tid
            , rfStateUtxo =
                resolvedWalletInputToUtxoEntry stateUtxo
            , rfRequestUtxos =
                map resolvedWalletInputToUtxoEntry requestUtxos
            , rfWalletUtxos =
                map resolvedWalletInputToUtxoEntry walletUtxos
            , rfValidityLowerSlot = lowerSlot
            , rfValidityUpperSlot = upperSlot
            , rfProtocolParameters =
                pparamsToJSON pparams
            }

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
        , efProtocolParameters = pparamsToJSON pparams
        }

pparamsToJSON :: PParams ConwayEra -> UnverifiedPParams
pparamsToJSON pparams =
    UnverifiedPParams
        { uppVerified = False
        , uppCbor =
            Hex
                ( serialize'
                    (natVersion @11)
                    pparams
                )
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

trieFactToJSON :: Tx.TrieFact -> TrieFact
trieFactToJSON fact =
    TrieFact
        { tfKey = Hex (Tx.factKey fact)
        , tfValue = Hex <$> Tx.factValue fact
        , tfMpfProof = Hex (Tx.factMpfProof fact)
        }
