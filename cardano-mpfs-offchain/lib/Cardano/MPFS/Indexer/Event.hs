{-# LANGUAGE DataKinds #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE TupleSections #-}

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
    , CageEventFailure (..)
    , CageEventDetectionFailure (..)

      -- * Inverse operations (for rollback)
    , CageInverseOp (..)

      -- * Detection
    , detectCageEvents
    , detectCageEventsDetailed
    , inversesOf

      -- * Scope recognition (for the /tx/submit gate)
    , mintsCagePolicy
    , isCageStateOutput
    , requestOutputToken
    ) where

import Control.Exception (Exception (..), throw)
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
    ( bodyTxL
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
import Cardano.Tx.Ledger (ConwayTx)
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
    , RequestAction (Rejected)
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
      -- Carries the new 'TokenId', the 'TxIn' of the
      -- continuing-state output, and the initial
      -- 'TokenState' from that output's datum.
      CageBoot !TokenId !TxIn !TokenState
    | -- | Output to cage address with a 'RequestDatum'.
      -- Carries the UTxO reference and decoded 'Request'.
      CageRequest !TxIn !Request
    | -- | Oracle processes requests: consumes request
      -- UTxOs and updates the trie root. Carries the
      -- token, the new state 'TxIn', new root, and
      -- consumed request UTxOs.
      CageUpdate !TokenId !TxIn !Root ![TxIn]
    | -- | Oracle rejects expired Phase 3 requests.
      -- Root unchanged; carries the new state 'TxIn'
      -- and the consumed request UTxOs.
      CageReject !TokenId !TxIn ![TxIn]
    | -- | Requester cancels a pending request.
      CageRetract !TxIn
    | -- | Burn cage token: token permanently removed.
      CageBurn !TokenId
    deriving stock (Eq, Show)

-- | Inverse of a cage event, used for rollback.
data CageInverseOp
    = -- | Re-insert a deleted token, together with
      -- the 'TxIn' of the UTxO that carried it.
      InvRestoreToken !TokenId !TxIn !TokenState
    | -- | Remove an inserted token
      InvRemoveToken !TokenId
    | -- | Re-insert a deleted request
      InvRestoreRequest !TxIn !Request
    | -- | Remove an inserted request
      InvRemoveRequest !TxIn
    | -- | Restore a token's previous state location
      -- and root (undo update or reject). 'TxIn' is
      -- the previous state UTxO; 'Root' is the root
      -- at that point.
      InvRestoreRoot !TokenId !TxIn !Root
    | -- | Restore key→value in token's trie (undo delete/update)
      InvTrieInsert !TokenId !ByteString !ByteString
    | -- | Remove key from token's trie (undo insert)
      InvTrieDelete !TokenId !ByteString
    deriving stock (Eq, Show)

-- | Typed detection failure for malformed cage data
-- in a transaction being inspected.
data CageEventFailure
    = MissingSpendingInput !TxIn
    | InvalidPaymentKeyHash !ByteString
    deriving stock (Eq, Show)

-- | Exception used by the legacy pure detection API
-- to fail loudly without a bare panic.
newtype CageEventDetectionFailure
    = CageEventDetectionFailure [CageEventFailure]
    deriving stock (Eq, Show)

instance Exception CageEventDetectionFailure where
    displayException
        (CageEventDetectionFailure failures) =
            "Cage event detection failed: "
                <> show failures

-- | Detect cage events from a transaction.
--
-- Inspects mints (boot\/burn), outputs (request),
-- and spending redeemers (update\/retract).
detectCageEvents
    :: ScriptHash
    -- ^ Cage script hash
    -> [(TxIn, TxOut ConwayEra)]
    -- ^ Resolved inputs (UTxOs being spent)
    -> ConwayTx
    -> [CageEvent]
detectCageEvents scriptHash resolvedInputs tx =
    case failures of
        [] -> events
        _ ->
            throw
                $ CageEventDetectionFailure
                    failures
  where
    (failures, events) =
        detectCageEventsDetailed
            scriptHash
            resolvedInputs
            tx

-- | Detect cage events and return typed failures
-- instead of throwing them.
detectCageEventsDetailed
    :: ScriptHash
    -- ^ Cage script hash
    -> [(TxIn, TxOut ConwayEra)]
    -- ^ Resolved inputs (UTxOs being spent)
    -> ConwayTx
    -> ([CageEventFailure], [CageEvent])
detectCageEventsDetailed scriptHash resolvedInputs tx =
    collectDetections
        $ mintDetections
            ++ requestDetections
            ++ spendDetections
  where
    body = tx ^. bodyTxL
    policyId = PolicyID scriptHash

    collectDetections =
        foldr collect ([], [])
      where
        collect (Left failure) (failures, events) =
            (failure : failures, events)
        collect (Right events') (failures, events) =
            (failures, events' ++ events)

    -- --------------------------------------------------
    -- Mint / Burn detection
    -- --------------------------------------------------

    mintDetections =
        let MultiAsset ma = body ^. mintTxBodyL
        in  case Map.lookup policyId ma of
                Nothing -> []
                Just assets ->
                    map
                        (uncurry detectMint)
                        (Map.toList assets)

    detectMint assetName qty
        | qty == 1 =
            -- Boot: find StateDatum in outputs
            let tid = TokenId assetName
                thisTxId = txIdTx tx
                outputs =
                    zip
                        [0 ..]
                        ( toList
                            (body ^. outputsTxBodyL)
                        )
                mState =
                    findStateDatum
                        thisTxId
                        outputs
            in  case mState of
                    Just (Right (txIn, ts)) ->
                        Right [CageBoot tid txIn ts]
                    Just (Left failure) ->
                        Left failure
                    Nothing -> Right []
        | qty == -1 =
            Right [CageBurn (TokenId assetName)]
        | otherwise = Right []

    findStateDatum thisTxId =
        foldr
            ( \(ix, txOut) acc -> case acc of
                Just _ -> acc
                Nothing ->
                    case extractDatum txOut of
                        Just (StateDatum ocs) ->
                            Just
                                $ fmap
                                    (TxIn
                                        thisTxId
                                        (TxIx ix),)
                                    (fromOnChainState ocs)
                        _ -> Nothing
            )
            Nothing

    -- --------------------------------------------------
    -- Request detection (new outputs at cage addr)
    -- --------------------------------------------------

    requestDetections =
        let outputs =
                toList
                    (body ^. outputsTxBodyL)
            thisTxId = txIdTx tx
        in  zipWith
                (detectRequest thisTxId)
                [0 ..]
                outputs

    detectRequest txId (ix :: Word16) txOut =
        -- PR #50: request UTxOs land at the per-cage
        -- request validator address, whose script
        -- hash is parametrised by
        -- @(statePolicyId, cageTokenName)@. Off-chain
        -- detection cannot enumerate every per-cage
        -- hash up front, so we recognise request
        -- outputs by datum shape: any script-address
        -- output carrying a 'RequestDatum'. Whether
        -- the targeted cage token is a known cage is
        -- enforced downstream when the request is
        -- consumed (see 'isRequestForToken').
        case txOut ^. addrTxOutL of
            addr@(Addr _ (ScriptHashObj _) _) ->
                case extractDatum txOut of
                    Just (RequestDatum ocReq) ->
                        let txIn =
                                TxIn
                                    txId
                                    (TxIx ix)
                        in  case fromOnChainReq
                                addr
                                ocReq of
                                Right req ->
                                    Right
                                        [CageRequest txIn req]
                                Left failure ->
                                    Left failure
                    _ -> Right []
            _ -> Right []

    -- --------------------------------------------------
    -- Spend detection (Update / Retract)
    -- --------------------------------------------------

    spendDetections =
        let allInputs = body ^. inputsTxBodyL
            Redeemers rdmrs =
                tx ^. witsTxL . rdmrsTxWitsL
        in  map
                (detectSpend allInputs rdmrs)
                resolvedInputs

    detectSpend allInputs rdmrs (txIn, txOut) =
        -- PR #50: cage spends now happen at TWO
        -- validator addresses: the global state
        -- validator (Modify) and the per-cage
        -- request validator (Contribute / Sweep /
        -- retract). Both produce script-address
        -- inputs whose redeemer determines the event
        -- shape; the @decodeSpend@ helper below
        -- already discriminates by redeemer.
        case txOut ^. addrTxOutL of
            Addr _ (ScriptHashObj _) _ ->
                case spendingIndex txIn allInputs of
                    Left failure ->
                        Left failure
                    Right ix ->
                        let purpose =
                                ConwaySpending (AsIx ix)
                        in  case Map.lookup purpose rdmrs of
                                Just (redeemerData, _) ->
                                    decodeSpend
                                        txIn
                                        txOut
                                        redeemerData
                                Nothing -> Right []
            _ -> Right []

    decodeSpend txIn txOut redeemerData =
        let Data plcData = redeemerData
        in  case fromBuiltinData
                (BuiltinData plcData) of
                Just (Modify actions)
                    | not (null actions)
                        && all
                            ( \case
                                Rejected -> True
                                _ -> False
                            )
                            actions ->
                        detectReject txIn txOut
                    | otherwise ->
                        detectUpdate txIn txOut
                Just (Retract _ref) ->
                    detectRetract txIn
                _ -> Right []

    detectUpdate _stateTxIn _stateTxOut =
        -- Find the continuing output (new state)
        let thisTxId = txIdTx tx
            outputs =
                zip
                    [0 ..]
                    ( toList
                        (body ^. outputsTxBodyL)
                    )
        in  case findStateDatumWithToken
                thisTxId
                outputs of
                Just (newTxIn, tid, newRoot) ->
                    let consumed =
                            findConsumedRequests
                                tid
                    in  Right
                            [ CageUpdate
                                tid
                                newTxIn
                                newRoot
                                consumed
                            ]
                Nothing -> Right []

    findStateDatumWithToken thisTxId =
        foldr
            ( \(ix, txOut) acc -> case acc of
                Just _ -> acc
                Nothing ->
                    case txOut ^. addrTxOutL of
                        Addr _ (ScriptHashObj sh) _
                            | sh == scriptHash ->
                                case extractDatum txOut of
                                    Just (StateDatum ocs) ->
                                        fmap
                                            ( \(tid, r) ->
                                                ( TxIn
                                                    thisTxId
                                                    (TxIx ix)
                                                , tid
                                                , r
                                                )
                                            )
                                            ( extractTokenId
                                                txOut
                                                ocs
                                            )
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
        let thisTxId = txIdTx tx
            outputs =
                zip
                    [0 ..]
                    ( toList
                        (body ^. outputsTxBodyL)
                    )
        in  case findStateDatumWithToken
                thisTxId
                outputs of
                Just (newTxIn, tid, _root) ->
                    let consumed =
                            findConsumedRequests
                                tid
                    in  Right
                            [ CageReject
                                tid
                                newTxIn
                                consumed
                            ]
                Nothing -> Right []

    detectRetract txIn = Right [CageRetract txIn]

-- | Compute inverse operations for a cage event,
-- given the cage state before the event.
--
-- Used to record rollback information alongside
-- cardano-utxo-csmt's rollback points.
inversesOf
    :: (TokenId -> Maybe (TxIn, TokenState))
    -- ^ Token lookup: returns state UTxO ref and state
    -> (TxIn -> Maybe Request)
    -- ^ Request lookup in current state
    -> CageEvent
    -> [CageInverseOp]
inversesOf lookupToken lookupReq = \case
    CageBoot tid _txIn _ts ->
        [InvRemoveToken tid]
    CageRequest txIn _req ->
        [InvRemoveRequest txIn]
    CageUpdate tid _newTxIn _newRoot consumed ->
        let restoreRoot = case lookupToken tid of
                Just (oldTxIn, ts) ->
                    [ InvRestoreRoot
                        tid
                        oldTxIn
                        (root ts)
                    ]
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
    CageReject tid _newTxIn consumed ->
        let restoreRoot = case lookupToken tid of
                Just (oldTxIn, ts) ->
                    [ InvRestoreRoot
                        tid
                        oldTxIn
                        (root ts)
                    ]
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
    CageRetract txIn ->
        case lookupReq txIn of
            Just req ->
                [InvRestoreRequest txIn req]
            Nothing -> []
    CageBurn tid ->
        case lookupToken tid of
            Just (txIn, ts) ->
                [InvRestoreToken tid txIn ts]
            Nothing -> []

-- --------------------------------------------------------
-- Scope recognition for the /tx/submit gate
-- --------------------------------------------------------
--
-- These tx-body recognition primitives are reused by the
-- @POST \/tx\/submit@ scope gate
-- ("Cardano.MPFS.HTTP.SubmitScope") so it cannot drift
-- from how the indexer classifies the same transaction.
-- They are purely structural and resolve no inputs:
-- spend-only events such as retract and sweep leave no
-- mint or output fingerprint, so the gate recognises
-- those by applying the SAME 'isCageStateOutput' /
-- request-address checks to the transaction's spent
-- inputs after resolving them against the indexed UTxO
-- set (see 'Cardano.MPFS.HTTP.SubmitScope.txTouchesMpfs').
-- The gate keys 'mintsCagePolicy' and 'isCageStateOutput'
-- to the configured cage script hash, and binds
-- 'requestOutputToken' outputs to the per-token request
-- validator address.

-- | 'True' iff the transaction mints or burns any asset
-- under the cage policy (the cage script hash viewed as
-- a 'PolicyID'). Mirrors the @mintDetections@ key in
-- 'detectCageEventsDetailed' (boot, end).
mintsCagePolicy :: ScriptHash -> ConwayTx -> Bool
mintsCagePolicy scriptHash tx =
    let MultiAsset ma = tx ^. bodyTxL . mintTxBodyL
    in  Map.member (PolicyID scriptHash) ma

-- | 'True' iff the output is locked by the cage state
-- script — its payment credential is the cage script
-- hash. Matched by payment credential (not the full
-- address) to mirror 'findStateDatumWithToken' (boot,
-- update, reject).
isCageStateOutput
    :: ScriptHash -> TxOut ConwayEra -> Bool
isCageStateOutput scriptHash txOut =
    case txOut ^. addrTxOutL of
        Addr _ (ScriptHashObj sh) _ ->
            sh == scriptHash
        _ -> False

-- | If the output is a request output — a
-- script-address output carrying a 'RequestDatum' —
-- return the target 'TokenId' the request names;
-- otherwise 'Nothing'. Mirrors 'detectRequest'. The
-- caller is expected to check the output sits at that
-- token's request validator address, since the per-cage
-- request script hash is parametrised by the token and
-- cannot be enumerated up front.
requestOutputToken :: TxOut ConwayEra -> Maybe TokenId
requestOutputToken txOut =
    case txOut ^. addrTxOutL of
        Addr _ (ScriptHashObj _) _ ->
            case extractDatum txOut of
                Just
                    ( RequestDatum
                            OnChainRequest{requestToken = ocTid}
                        ) ->
                        Just (tokenIdFromOnChain ocTid)
                _ -> Nothing
        _ -> Nothing

-- --------------------------------------------------------
-- On-chain → off-chain conversion
-- --------------------------------------------------------

-- | Convert on-chain 'OnChainTokenState' to off-chain
-- 'TokenState'.
fromOnChainState
    :: OnChainTokenState
    -> Either CageEventFailure TokenState
fromOnChainState OnChainTokenState{stateStakeScript = _, ..} = do
    owner <- keyHashFromBBS stateOwner
    pure
        TokenState
            { owner = owner
            , root = Root (unOnChainRoot stateRoot)
            , tip = Coin stateMaxFee
            , processTime = stateProcessTime
            , retractTime = stateRetractTime
            }

-- | Convert on-chain 'OnChainRequest' to off-chain
-- 'Request'. The address is accepted for future use
-- but currently unused.
fromOnChainReq
    :: Addr
    -> OnChainRequest
    -> Either CageEventFailure Request
fromOnChainReq _addr OnChainRequest{..} = do
    owner <- keyHashFromBBS requestOwner
    pure
        Request
            { requestToken =
                tokenIdFromOnChain requestToken
            , requestOwner = owner
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
    -> Either CageEventFailure Word32
spendingIndex needle inputs =
    go 0 (Set.toAscList inputs)
  where
    go _ [] =
        Left $ MissingSpendingInput needle
    go n (x : xs)
        | x == needle = Right n
        | otherwise = go (n + 1) xs

-- | Convert a 'BuiltinByteString' containing a
-- payment key hash to a 'KeyHash'.
keyHashFromBBS
    :: BuiltinByteString
    -> Either CageEventFailure (KeyHash Payment)
keyHashFromBBS (BuiltinByteString bs) =
    case hashFromBytes bs of
        Just h -> Right (KeyHash h)
        Nothing ->
            Left $ InvalidPaymentKeyHash bs
