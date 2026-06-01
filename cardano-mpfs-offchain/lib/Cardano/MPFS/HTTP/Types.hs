{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedStrings #-}

-- |
-- Module      : Cardano.MPFS.HTTP.Types
-- Description : Server compatibility layer for MPFS HTTP wire types.
-- License     : Apache-2.0
--
-- The JSON wire contract lives in @cardano-mpfs-api@. This module
-- re-exports that contract for the existing server imports and keeps the
-- ledger/offchain conversion helpers local to the server package.
module Cardano.MPFS.HTTP.Types
    ( -- * Status
      StatusResponse (..)

      -- * Proof-bearing snapshot
    , ChainPointJSON (..)
    , VerificationSnapshot (..)

      -- * Tokens
    , TokenIdJSON (..)
    , tokenIdToJSON
    , tokenIdFromJSON
    , TokenStateJSON (..)
    , tokenStateToJSON

      -- * Requests
    , RequestJSON (..)
    , requestToJSON

      -- * UTxO witnesses
    , TxInJSON (..)
    , txInToJSON
    , WitnessedUtxo (..)
    , UtxoRef (..)
    , UtxoEntry (..)
    , UnverifiedPParams (..)
    , BootFacts (..)
    , resolvedWalletInputToUtxoEntry

      -- * Proof-bearing read responses
    , WitnessedTokenState (..)
    , WitnessedRequest (..)
    , FactWitness (..)
    , TokenResponse (..)
    , FactEntry (..)
    , FactsResponse (..)
    , FactResponse (..)
    , ProofResponse (..)
    , RequestsResponse (..)

      -- * Transaction requests
    , BootRequest (..)
    , InsertRequest (..)
    , DeleteRequest (..)
    , UpdateValueRequest (..)
    , RejectRequest (..)
    , UpdateRequest (..)
    , RetractRequest (..)
    , SweepRequest (..)
    , EndRequest (..)
    , SubmitRequest (..)
    , SubmitResponse (..)
    , SubmitError (..)

      -- * Proof-bearing tx responses
    , TrieFactJSON (..)
    , trieFactToJSON
    , witnessedInputToJSON
    , bundleSnapshotToJSON
    , BootProofJSON (..)
    , RequestProofJSON (..)
    , RetractProofJSON (..)
    , RejectProofJSON (..)
    , EndProofJSON (..)
    , UpdateProofJSON (..)
    , BootTxResponse (..)
    , RequestTxResponse (..)
    , RetractTxResponse (..)
    , RejectTxResponse (..)
    , EndTxResponse (..)
    , SweepTxResponse (..)
    , UpdateTxResponse (..)
    , UnsignedTxResponse (..)
    , mkRequestTxResponse
    , mkRetractTxResponse
    , mkRejectTxResponse
    , mkEndTxResponse
    , mkSweepTxResponse
    , mkUpdateTxResponse

      -- * Address parsing
    , parseAddr
    ) where

import Data.ByteString.Short qualified as SBS
import Data.Text (Text)

import Cardano.Crypto.Hash.Class qualified as Crypto
import Cardano.Ledger.Address (decodeAddrEither)
import Cardano.Ledger.BaseTypes (TxIx (..))
import Cardano.Ledger.Binary (natVersion, serialize')
import Cardano.Ledger.Hashes (extractHash)
import Cardano.Ledger.Keys (KeyHash (..), KeyRole (..))
import Cardano.Ledger.Mary.Value (AssetName (..))
import Cardano.Ledger.TxIn (TxId (..), TxIn (..))
import Cardano.Tx.Ledger (ConwayTx)

import Cardano.MPFS.API.Types
    ( BootFacts (..)
    , BootProofJSON (..)
    , BootRequest (..)
    , BootTxResponse (..)
    , ChainPointJSON (..)
    , DeleteRequest (..)
    , EndProofJSON (..)
    , EndRequest (..)
    , EndTxResponse (..)
    , FactEntry (..)
    , FactResponse (..)
    , FactWitness (..)
    , FactsResponse (..)
    , InsertRequest (..)
    , ProofResponse (..)
    , RejectProofJSON (..)
    , RejectRequest (..)
    , RejectTxResponse (..)
    , RequestJSON (..)
    , RequestProofJSON (..)
    , RequestTxResponse (..)
    , RequestsResponse (..)
    , RetractProofJSON (..)
    , RetractRequest (..)
    , RetractTxResponse (..)
    , StatusResponse (..)
    , SubmitError (..)
    , SubmitRequest (..)
    , SubmitResponse (..)
    , SweepRequest (..)
    , SweepTxResponse (..)
    , TokenIdJSON (..)
    , TokenResponse (..)
    , TokenStateJSON (..)
    , TrieFactJSON (..)
    , TxInJSON (..)
    , UnsignedTxResponse (..)
    , UnverifiedPParams (..)
    , UpdateProofJSON (..)
    , UpdateRequest (..)
    , UpdateTxResponse (..)
    , UpdateValueRequest (..)
    , UtxoEntry (..)
    , UtxoRef (..)
    , VerificationSnapshot (..)
    , WitnessedRequest (..)
    , WitnessedTokenState (..)
    , WitnessedUtxo (..)
    )
import Cardano.MPFS.Core.Types
    ( Addr
    , BlockId (..)
    , Coin (..)
    , Operation (..)
    , Request (..)
    , Root (..)
    , SlotNo (..)
    , TokenId (..)
    , TokenState (..)
    )
import Cardano.MPFS.HTTP.Encoding (Hex (..))
import Cardano.MPFS.TxBuilder
    ( BundleSnapshot (..)
    , EndProof (..)
    , ProofEnvelope (..)
    , RejectProof (..)
    , RequestProof (..)
    , ResolvedWalletInput
    , RetractProof (..)
    , TrieFact (..)
    , UpdateProof (..)
    , WitnessedInput (..)
    )

-- | Convert an internal token id to the shared wire token id.
tokenIdToJSON :: TokenId -> TokenIdJSON
tokenIdToJSON (TokenId (AssetName sbs)) =
    TokenIdJSON (SBS.fromShort sbs)

-- | Convert a shared wire token id to the internal token id.
tokenIdFromJSON :: TokenIdJSON -> TokenId
tokenIdFromJSON (TokenIdJSON bs) =
    TokenId (AssetName (SBS.toShort bs))

-- | Convert internal 'TokenState' to JSON type.
tokenStateToJSON :: TokenState -> TokenStateJSON
tokenStateToJSON
    TokenState
        { owner = tsOwner
        , root = tsRoot
        , tip = tsMaxFee
        , processTime = tsProcessTime
        , retractTime = tsRetractTime
        } =
        TokenStateJSON
            { owner = hashToHex tsOwner
            , root = Hex (unRoot tsRoot)
            , tip = unCoin tsMaxFee
            , processTime = tsProcessTime
            , retractTime = tsRetractTime
            }

-- | Render a 'KeyHash' as hex text.
hashToHex :: KeyHash Payment -> Text
hashToHex (KeyHash h) =
    Crypto.hashToTextAsHex h

-- | Convert internal 'Request' to JSON type.
requestToJSON :: Request -> RequestJSON
requestToJSON
    Request
        { requestToken = tok
        , requestOwner = own
        , requestKey = k
        , requestValue = op
        , requestFee = fee
        , requestSubmittedAt = sat
        } =
        let (opName, mVal) = case op of
                Insert v -> ("insert", Just (Hex v))
                Delete _v -> ("delete", Nothing)
                Update _old new' ->
                    ("update", Just (Hex new'))
        in  RequestJSON
                { rjToken = tokenIdToJSON tok
                , rjOwner = hashToHex own
                , rjKey = Hex k
                , rjOperation = opName
                , rjValue = mVal
                , rjFee = unCoin fee
                , rjSubmittedAt = sat
                }

-- | Convert a ledger 'TxIn' to its JSON form.
txInToJSON :: TxIn -> TxInJSON
txInToJSON (TxIn (TxId sh) (TxIx ix)) =
    TxInJSON
        { tjTxId =
            Hex (Crypto.hashToBytes (extractHash sh))
        , tjTxIx = fromIntegral ix
        }

-- | Parse a hex-encoded serialized address.
parseAddr :: Hex -> Either String Addr
parseAddr (Hex bs) = decodeAddrEither bs

-- | Convert a 'WitnessedInput' to its JSON wire form.
witnessedInputToJSON :: WitnessedInput -> WitnessedUtxo
witnessedInputToJSON
    WitnessedInput
        { witnessedRef = tin
        , witnessedTxOut = out
        , witnessedCsmtProof = prf
        } =
        WitnessedUtxo
            { wuTxIn = txInToJSON tin
            , wuTxOut = Hex out
            , wuProof = Hex prf
            }

-- | Convert a 'WitnessedInput' to a post-split
-- 'UtxoEntry' with a per-entry CSMT inclusion proof.
-- The wire keys are @ref@ / @txout_cbor@ /
-- @inclusion_proof@ rather than the legacy
-- @tx_in@ / @tx_out@ / @proof@.
witnessedInputToUtxoEntry :: WitnessedInput -> UtxoEntry
witnessedInputToUtxoEntry
    WitnessedInput
        { witnessedRef = TxIn (TxId sh) (TxIx ix)
        , witnessedTxOut = out
        , witnessedCsmtProof = prf
        } =
        UtxoEntry
            { ueRef =
                UtxoRef
                    { urTxId =
                        Hex
                            ( Crypto.hashToBytes
                                (extractHash sh)
                            )
                    , urTxIx = fromIntegral ix
                    }
            , ueTxOutCbor = Hex out
            , ueInclusionProof = Hex prf
            }

-- | Convert an indexer-resolved wallet input to a
-- post-split 'UtxoEntry'.
resolvedWalletInputToUtxoEntry
    :: ResolvedWalletInput -> UtxoEntry
resolvedWalletInputToUtxoEntry (tin, out, prf) =
    witnessedInputToUtxoEntry
        WitnessedInput
            { witnessedRef = tin
            , witnessedTxOut = out
            , witnessedCsmtProof = prf
            }

-- | Convert an internal 'TrieFact' to JSON.
trieFactToJSON :: TrieFact -> TrieFactJSON
trieFactToJSON
    TrieFact
        { factKey = k
        , factValue = mv
        , factMpfProof = p
        } =
        TrieFactJSON
            { tfKey = Hex k
            , tfValue = fmap Hex mv
            , tfMpfProof = Hex p
            }

-- | Convert a 'BundleSnapshot' to the HTTP-level
-- 'VerificationSnapshot'.
bundleSnapshotToJSON
    :: BundleSnapshot -> VerificationSnapshot
bundleSnapshotToJSON
    BundleSnapshot
        { snapshotUtxoRoot = r
        , snapshotSlot = SlotNo s
        , snapshotBlockId = BlockId b
        } =
        VerificationSnapshot
            { vsUtxoRoot = Hex r
            , vsChainPoint =
                ChainPointJSON
                    { cpSlot = s
                    , cpBlockId = Hex b
                    }
            }

-- | Serialize a Conway-era 'Tx' to hex CBOR.
serializeTxHex :: ConwayTx -> Hex
serializeTxHex = Hex . serialize' (natVersion @11)

-- | Package a 'ProofEnvelope RequestProof' as the JSON response shared
-- by the three request endpoints.
mkRequestTxResponse
    :: ProofEnvelope RequestProof
    -> RequestTxResponse
mkRequestTxResponse
    ProofEnvelope
        { envTx = tx
        , envSnapshot = snap
        , envProof =
            RequestProof{requestFunding = funding}
        } =
        RequestTxResponse
            { rqtTx = serializeTxHex tx
            , rqtSnapshot = bundleSnapshotToJSON snap
            , rqtProof =
                RequestProofJSON
                    { rqpFunding =
                        map witnessedInputToJSON funding
                    }
            }

-- | Package a bare sweep 'Tx' as the JSON response.
-- Sweep does not bundle a proof envelope (the
-- on-chain validator enforces the owner-signature
-- predicate against the referenced state UTxO).
mkSweepTxResponse :: ConwayTx -> SweepTxResponse
mkSweepTxResponse tx =
    SweepTxResponse{stTx = serializeTxHex tx}

-- | Package a 'ProofEnvelope RetractProof' as the JSON response.
mkRetractTxResponse
    :: ProofEnvelope RetractProof
    -> RetractTxResponse
mkRetractTxResponse
    ProofEnvelope
        { envTx = tx
        , envSnapshot = snap
        , envProof =
            RetractProof
                { retractRequestIn = reqIn
                , retractStateRef = stRef
                , retractFunding = funding
                }
        } =
        RetractTxResponse
            { rttTx = serializeTxHex tx
            , rttSnapshot = bundleSnapshotToJSON snap
            , rttProof =
                RetractProofJSON
                    { rtpRequestIn =
                        witnessedInputToJSON reqIn
                    , rtpStateRef =
                        witnessedInputToJSON stRef
                    , rtpFunding =
                        map witnessedInputToJSON funding
                    }
            }

-- | Package a 'ProofEnvelope RejectProof' as the JSON response.
mkRejectTxResponse
    :: ProofEnvelope RejectProof -> RejectTxResponse
mkRejectTxResponse
    ProofEnvelope
        { envTx = tx
        , envSnapshot = snap
        , envProof =
            RejectProof
                { rejectState = st
                , rejectRequestIns = reqs
                , rejectFunding = funding
                }
        } =
        RejectTxResponse
            { rjtTx = serializeTxHex tx
            , rjtSnapshot = bundleSnapshotToJSON snap
            , rjtProof =
                RejectProofJSON
                    { rjpState =
                        witnessedInputToJSON st
                    , rjpRequestIns =
                        map witnessedInputToJSON reqs
                    , rjpFunding =
                        map witnessedInputToJSON funding
                    }
            }

-- | Package a 'ProofEnvelope EndProof' as the JSON response.
mkEndTxResponse
    :: ProofEnvelope EndProof -> EndTxResponse
mkEndTxResponse
    ProofEnvelope
        { envTx = tx
        , envSnapshot = snap
        , envProof =
            EndProof
                { endState = st
                , endFunding = funding
                }
        } =
        EndTxResponse
            { etTx = serializeTxHex tx
            , etSnapshot = bundleSnapshotToJSON snap
            , etProof =
                EndProofJSON
                    { epState =
                        witnessedInputToJSON st
                    , epFunding =
                        map witnessedInputToJSON funding
                    }
            }

-- | Package a 'ProofEnvelope UpdateProof' as the JSON response.
mkUpdateTxResponse
    :: ProofEnvelope UpdateProof -> UpdateTxResponse
mkUpdateTxResponse
    ProofEnvelope
        { envTx = tx
        , envSnapshot = snap
        , envProof =
            UpdateProof
                { updateState = st
                , updateRequests = reqs
                , updateFunding = funding
                , updateTrieRoot = trieRoot
                , updateTrieRead = facts
                }
        } =
        UpdateTxResponse
            { uptTx = serializeTxHex tx
            , uptSnapshot = bundleSnapshotToJSON snap
            , uptProof =
                UpdateProofJSON
                    { upState =
                        witnessedInputToJSON st
                    , upRequests =
                        map witnessedInputToJSON reqs
                    , upFunding =
                        map witnessedInputToJSON funding
                    , upTrieRoot = Hex trieRoot
                    , upTrieRead =
                        map trieFactToJSON facts
                    }
            }
