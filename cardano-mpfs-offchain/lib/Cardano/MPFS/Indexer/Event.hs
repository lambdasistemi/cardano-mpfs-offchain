{-# LANGUAGE DataKinds #-}
{-# LANGUAGE LambdaCase #-}

-- |
-- Module      : Cardano.MPFS.Indexer.Event
-- Description : Cage event detection from transactions
-- License     : Apache-2.0
--
-- Extracts cage-protocol events (boot, request, update,
-- retract, burn) from Conway-era Cardano transactions by
-- inspecting mints, outputs, and spending redeemers at
-- the cage script address and policy ID.
--
-- Also provides 'inversesOf' for computing the inverse
-- operations needed to undo each event during a chain
-- rollback. These inverses are stored alongside
-- checkpoints by "Cardano.MPFS.Indexer.Rollback".
module Cardano.MPFS.Indexer.Event
    ( -- * Event type
      CageEvent (..)

      -- * Inverse operations (for rollback)
    , CageInverseOp (..)

      -- * Detection
    , detectCageEvents
    , inversesOf
    ) where

import Data.ByteString (ByteString)
import Data.ByteString.Short qualified as SBS
import Data.Foldable (toList)
import Data.Map.Strict qualified as Map
import Data.Word (Word16, Word32)
import Lens.Micro ((^.))

import Cardano.Ledger.Address (Addr (..))
import Cardano.Ledger.Alonzo.Scripts (AsIx (..))
import Cardano.Ledger.Api.Scripts.Data
    ( Data (..)
    , Datum (..)
    , binaryDataToData
    )
import Cardano.Ledger.Api.Tx
    ( Tx
    , bodyTxL
    , txIdTx
    , witsTxL
    )
import Cardano.Ledger.Api.Tx.Body
    ( inputsTxBodyL
    , mintTxBodyL
    , outputsTxBodyL
    )
import Cardano.Ledger.Api.Tx.Out
    ( addrTxOutL
    , datumTxOutL
    , valueTxOutL
    )
import Cardano.Ledger.Api.Tx.Wits
    ( Redeemers (..)
    , rdmrsTxWitsL
    )
import Cardano.Ledger.BaseTypes (TxIx (..))
import Cardano.Ledger.Conway.Scripts
    ( ConwayPlutusPurpose (..)
    )
import Cardano.Ledger.Credential
    ( Credential (..)
    )
import Cardano.Ledger.Hashes (ScriptHash)
import Cardano.Ledger.Mary.Value
    ( MaryValue (..)
    , MultiAsset (..)
    , PolicyID (..)
    )
import Cardano.Ledger.TxIn
    ( TxIn (..)
    )
import Data.Set qualified as Set
import PlutusTx.Builtins.Internal
    ( BuiltinByteString (..)
    , BuiltinData (..)
    )
import PlutusTx.IsData.Class
    ( FromData (..)
    )

import Cardano.Crypto.Hash (hashFromBytes)
import Cardano.Ledger.Keys
    ( KeyHash (..)
    , KeyRole (..)
    )
import Cardano.MPFS.Core.OnChain
    ( CageDatum (..)
    , OnChainOperation (..)
    , OnChainRequest (..)
    , OnChainRoot (..)
    , OnChainTokenId (..)
    , OnChainTokenState (..)
    , UpdateRedeemer (..)
    )
import Cardano.MPFS.Core.Types
    ( AssetName (..)
    , Coin (..)
    , ConwayEra
    , Operation (..)
    , Request (..)
    , Root (..)
    , TokenId (..)
    , TokenState (..)
    , TxOut
    )

-- | A cage-protocol event detected in a transaction.
data CageEvent
    = -- | Mint with cage policyId: new token created.
      -- Carries the new 'TokenId' and its initial
      -- 'TokenState' from the output datum.
      CageBoot !TokenId !TokenState
    | -- | Output to cage address with a 'RequestDatum'.
      -- Carries the UTxO reference and decoded 'Request'.
      CageRequest !TxIn !Request
    | -- | Oracle processes requests: consumes request
      -- UTxOs and updates the trie root. Carries the
      -- token, new root, and consumed request UTxOs.
      CageUpdate !TokenId !Root ![TxIn]
    | -- | Oracle rejects expired Phase 3 requests.
      -- Root unchanged, requests consumed.
      CageReject !TokenId ![TxIn]
    | -- | Requester cancels a pending request.
      CageRetract !TxIn
    | -- | Burn cage token: token permanently removed.
      CageBurn !TokenId
    deriving stock (Eq, Show)

-- | Inverse of a cage event, used for rollback.
data CageInverseOp
    = -- | Re-insert a deleted token
      InvRestoreToken !TokenId !TokenState
    | -- | Remove an inserted token
      InvRemoveToken !TokenId
    | -- | Re-insert a deleted request
      InvRestoreRequest !TxIn !Request
    | -- | Remove an inserted request
      InvRemoveRequest !TxIn
    | -- | Restore a token's previous root
      InvRestoreRoot !TokenId !Root
    | -- | Restore key→value in token's trie (undo delete/update)
      InvTrieInsert !TokenId !ByteString !ByteString
    | -- | Remove key from token's trie (undo insert)
      InvTrieDelete !TokenId !ByteString
    deriving stock (Eq, Show)

-- | Detect cage events from a transaction.
--
-- Inspects mints (boot\/burn), outputs (request),
-- and spending redeemers (update\/retract).
detectCageEvents
    :: ScriptHash
    -- ^ Cage script hash
    -> [(TxIn, TxOut ConwayEra)]
    -- ^ Resolved inputs (UTxOs being spent)
    -> Tx ConwayEra
    -> [CageEvent]
detectCageEvents scriptHash resolvedInputs tx =
    mintEvents ++ requestEvents ++ spendEvents
  where
    body = tx ^. bodyTxL
    policyId = PolicyID scriptHash

    -- --------------------------------------------------
    -- Mint / Burn detection
    -- --------------------------------------------------

    mintEvents =
        let MultiAsset ma = body ^. mintTxBodyL
        in  case Map.lookup policyId ma of
                Nothing -> []
                Just assets ->
                    concatMap
                        (uncurry detectMint)
                        (Map.toList assets)

    detectMint assetName qty
        | qty == 1 =
            -- Boot: find StateDatum in outputs
            let tid = TokenId assetName
                outputs =
                    toList
                        (body ^. outputsTxBodyL)
                mState = findStateDatum outputs
            in  case mState of
                    Just ts -> [CageBoot tid ts]
                    Nothing -> []
        | qty == -1 =
            [CageBurn (TokenId assetName)]
        | otherwise = []

    findStateDatum =
        foldr
            ( \txOut acc -> case acc of
                Just _ -> acc
                Nothing ->
                    case extractDatum txOut of
                        Just (StateDatum ocs) ->
                            Just
                                (fromOnChainState ocs)
                        _ -> Nothing
            )
            Nothing

    -- --------------------------------------------------
    -- Request detection (new outputs at cage addr)
    -- --------------------------------------------------

    requestEvents =
        let outputs =
                toList
                    (body ^. outputsTxBodyL)
            thisTxId = txIdTx tx
        in  concatMap
                (uncurry (detectRequest thisTxId))
                (zip [0 ..] outputs)

    detectRequest txId (ix :: Word16) txOut =
        case txOut ^. addrTxOutL of
            addr@(Addr _ (ScriptHashObj sh) _)
                | sh == scriptHash ->
                    case extractDatum txOut of
                        Just (RequestDatum ocReq) ->
                            let txIn =
                                    TxIn
                                        txId
                                        (TxIx ix)
                                req =
                                    fromOnChainReq
                                        addr
                                        ocReq
                            in  [CageRequest txIn req]
                        _ -> []
            _ -> []

    -- --------------------------------------------------
    -- Spend detection (Update / Retract)
    -- --------------------------------------------------

    spendEvents =
        let allInputs = body ^. inputsTxBodyL
            Redeemers rdmrs =
                tx ^. witsTxL . rdmrsTxWitsL
        in  concatMap
                (detectSpend allInputs rdmrs)
                resolvedInputs

    detectSpend allInputs rdmrs (txIn, txOut) =
        case txOut ^. addrTxOutL of
            Addr _ (ScriptHashObj sh) _
                | sh == scriptHash ->
                    let ix =
                            spendingIndex
                                txIn
                                allInputs
                        purpose =
                            ConwaySpending (AsIx ix)
                    in  case Map.lookup purpose rdmrs of
                            Just (redeemerData, _) ->
                                decodeSpend
                                    txIn
                                    txOut
                                    redeemerData
                            Nothing -> []
            _ -> []

    decodeSpend txIn txOut redeemerData =
        let Data plcData = redeemerData
        in  case fromBuiltinData
                (BuiltinData plcData) of
                Just (Modify _proofs) ->
                    detectUpdate txIn txOut
                Just Reject ->
                    detectReject txIn txOut
                Just (Retract _ref) ->
                    detectRetract txIn
                _ -> []

    detectUpdate _stateTxIn _stateTxOut =
        -- Find the continuing output (new state)
        let outputs =
                toList
                    (body ^. outputsTxBodyL)
        in  case findStateDatumWithToken outputs of
                Just (tid, newRoot) ->
                    let consumed =
                            findConsumedRequests
                                tid
                    in  [ CageUpdate
                            tid
                            newRoot
                            consumed
                        ]
                Nothing -> []

    findStateDatumWithToken =
        foldr
            ( \txOut acc -> case acc of
                Just _ -> acc
                Nothing ->
                    case txOut ^. addrTxOutL of
                        Addr _ (ScriptHashObj sh) _
                            | sh == scriptHash ->
                                case extractDatum txOut of
                                    Just (StateDatum ocs) ->
                                        extractTokenId
                                            txOut
                                            ocs
                                    _ -> Nothing
                        _ -> Nothing
            )
            Nothing

    extractTokenId txOut ocs =
        let MaryValue _ (MultiAsset ma) =
                txOut ^. valueTxOutL
        in  case Map.lookup policyId ma of
                Just assets ->
                    case Map.toList assets of
                        [(an, _)] ->
                            Just
                                ( TokenId an
                                , Root
                                    $ unOnChainRoot
                                    $ stateRoot ocs
                                )
                        _ -> Nothing
                Nothing -> Nothing

    findConsumedRequests tid =
        [ fst ri
        | ri <- resolvedInputs
        , isRequestForToken tid ri
        ]

    isRequestForToken tid (_, txOut) =
        case extractDatum txOut of
            Just (RequestDatum OnChainRequest{requestToken = ocTid}) ->
                let OnChainTokenId (BuiltinByteString bs) =
                        ocTid
                in  TokenId
                        (AssetName (SBS.toShort bs))
                        == tid
            _ -> False

    detectReject _stateTxIn _stateTxOut =
        let outputs =
                toList
                    (body ^. outputsTxBodyL)
        in  case findStateDatumWithToken outputs of
                Just (tid, _root) ->
                    let consumed =
                            findConsumedRequests
                                tid
                    in  [CageReject tid consumed]
                Nothing -> []

    detectRetract txIn = [CageRetract txIn]

-- | Compute inverse operations for a cage event,
-- given the cage state before the event.
--
-- Used to record rollback information alongside
-- cardano-utxo-csmt's rollback points.
inversesOf
    :: (TokenId -> Maybe TokenState)
    -- ^ Token lookup in current state
    -> (TxIn -> Maybe Request)
    -- ^ Request lookup in current state
    -> CageEvent
    -> [CageInverseOp]
inversesOf lookupToken lookupReq = \case
    CageBoot tid _ts ->
        [InvRemoveToken tid]
    CageRequest txIn _req ->
        [InvRemoveRequest txIn]
    CageUpdate tid _newRoot consumed ->
        let restoreRoot = case lookupToken tid of
                Just ts ->
                    [InvRestoreRoot tid (root ts)]
                Nothing -> []
            restoreReqs =
                concatMap
                    ( \txIn' -> case lookupReq txIn' of
                        Just req ->
                            [InvRestoreRequest txIn' req]
                        Nothing -> []
                    )
                    consumed
        in  restoreRoot ++ restoreReqs
    CageReject _tid consumed ->
        concatMap
            ( \txIn' -> case lookupReq txIn' of
                Just req ->
                    [InvRestoreRequest txIn' req]
                Nothing -> []
            )
            consumed
    CageRetract txIn ->
        case lookupReq txIn of
            Just req ->
                [InvRestoreRequest txIn req]
            Nothing -> []
    CageBurn tid ->
        case lookupToken tid of
            Just ts -> [InvRestoreToken tid ts]
            Nothing -> []

-- --------------------------------------------------------
-- On-chain → off-chain conversion
-- --------------------------------------------------------

-- | Convert on-chain 'OnChainTokenState' to off-chain
-- 'TokenState'.
fromOnChainState :: OnChainTokenState -> TokenState
fromOnChainState OnChainTokenState{..} =
    TokenState
        { owner = keyHashFromBBS stateOwner
        , root = Root (unOnChainRoot stateRoot)
        , tip = Coin stateTip
        , processTime = stateProcessTime
        , retractTime = stateRetractTime
        }

-- | Convert on-chain 'OnChainRequest' to off-chain
-- 'Request'. The address is accepted for future use
-- but currently unused.
fromOnChainReq :: Addr -> OnChainRequest -> Request
fromOnChainReq _addr OnChainRequest{..} =
    Request
        { requestToken =
            tokenIdFromOnChain requestToken
        , requestOwner =
            keyHashFromBBS requestOwner
        , requestKey = requestKey
        , requestValue =
            fromOnChainOp requestValue
        , requestFee = Coin requestFee
        , requestSubmittedAt = requestSubmittedAt
        }

-- | Convert on-chain 'OnChainOperation' to off-chain
-- 'Operation'.
fromOnChainOp :: OnChainOperation -> Operation
fromOnChainOp = \case
    OpInsert v -> Insert v
    OpDelete v -> Delete v
    OpUpdate o n -> Update o n

-- | Convert on-chain 'OnChainTokenId' to off-chain
-- 'TokenId'.
tokenIdFromOnChain :: OnChainTokenId -> TokenId
tokenIdFromOnChain (OnChainTokenId (BuiltinByteString bs)) =
    TokenId (AssetName (SBS.toShort bs))

-- --------------------------------------------------------
-- Helpers
-- --------------------------------------------------------

-- | Extract an inline 'CageDatum' from a 'TxOut'.
extractDatum
    :: TxOut ConwayEra -> Maybe CageDatum
extractDatum txOut =
    case txOut ^. datumTxOutL of
        Datum bd ->
            let Data plcData = binaryDataToData bd
            in  fromBuiltinData (BuiltinData plcData)
        _ -> Nothing

-- | Compute the spending index of a 'TxIn' in
-- the set of all transaction inputs.
spendingIndex
    :: TxIn
    -> Set.Set TxIn
    -> Word32
spendingIndex needle inputs =
    go 0 (Set.toAscList inputs)
  where
    go _ [] =
        error "spendingIndex: TxIn not in input set"
    go n (x : xs)
        | x == needle = n
        | otherwise = go (n + 1) xs

-- | Convert a 'BuiltinByteString' containing a
-- payment key hash to a 'KeyHash'.
keyHashFromBBS
    :: BuiltinByteString -> KeyHash 'Payment
keyHashFromBBS (BuiltinByteString bs) =
    case hashFromBytes bs of
        Just h -> KeyHash h
        Nothing ->
            error "keyHashFromBBS: invalid hash"
