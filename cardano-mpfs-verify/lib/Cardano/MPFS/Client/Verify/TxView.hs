{-# LANGUAGE LambdaCase #-}

-- |
-- Module      : Cardano.MPFS.Client.Verify.TxView
-- Description : Pure Conway tx input/reference-input view.
--
-- A narrow, WASM-safe CBOR reader for the transaction-body fields
-- needed by the issue #227 proof/tx binding invariant. It deliberately
-- does not decode the whole ledger transaction and does not depend on
-- @cardano-ledger-*@.
module Cardano.MPFS.Client.Verify.TxView
    ( TxAsset (..)
    , TxOutView (..)
    , TxRedeemerPurpose (..)
    , TxRedeemerRole (..)
    , TxView (..)
    , decodeTxView
    , decodeTxOutView
    , stateRootBytesFromWitness
    , verifyBootAssetBinding
    , verifyBootRedeemerBinding
    , verifyContinuingStateOutput
    , verifyEndAssetBinding
    , verifyEndRedeemerBinding
    , verifyNoMint
    , verifyNoRedeemers
    , verifyRejectRedeemerBinding
    , verifyRetractRedeemerBinding
    , verifyStateRootBinding
    , verifyTxInputBinding
    , verifyTxBinding
    , verifyUpdateRedeemerBinding
    ) where

import Codec.CBOR.Read qualified as CBOR
import Codec.CBOR.Term qualified as CBOR
import Control.Monad (void)
import Data.ByteString qualified as BS
import Data.ByteString.Base16 qualified as Base16
import Data.ByteString.Lazy qualified as BSL
import Data.List (sort)
import Data.Maybe (isJust)
import Data.Text (Text)
import Data.Text qualified as T
import Data.Text.Encoding qualified as T
import Data.Text.Lazy qualified as TL
import Data.Word (Word64)

import Cardano.MPFS.Client.Bundle
    ( TrieFact (..)
    , TxIn (..)
    , WitnessedUtxo (..)
    )
import Cardano.MPFS.Client.Snapshot
    ( Hex (..)
    )
import Cardano.MPFS.Client.Verify.Replay
    ( VerifyError (..)
    )

-- | The transaction-body fragment needed by proof binding.
data TxAsset = TxAsset
    { assetPolicy :: Hex
    , assetName :: Hex
    , assetQuantity :: Integer
    }
    deriving stock (Eq, Ord, Show)

-- | The output fragment needed to identify continuing state outputs.
data TxOutView = TxOutView
    { txOutAssets :: [TxAsset]
    , txOutHasInlineDatum :: Bool
    , txOutInlineDatum :: Maybe CBOR.Term
    }
    deriving stock (Eq, Show)

-- | Script purpose for a transaction redeemer.
data TxRedeemerPurpose
    = RedeemerSpend Word64
    | RedeemerMint Word64
    | RedeemerOther Integer Word64
    deriving stock (Eq, Ord, Show)

-- | Endpoint role decoded from a transaction redeemer after
-- resolving spending indexes to concrete transaction inputs.
data TxRedeemerRole
    = RoleMinting
    | RoleBurning
    | RoleSpendEnd TxIn
    | RoleSpendContribute TxIn TxIn
    | RoleSpendModifyRejected TxIn Int
    | RoleSpendModifyUpdate TxIn [CBOR.Term]
    | RoleSpendRetract TxIn TxIn
    | RoleUnsupported TxRedeemerPurpose Text
    deriving stock (Eq, Ord, Show)

data TxRedeemer = TxRedeemer
    { txRedeemerPurpose :: TxRedeemerPurpose
    , txRedeemerData :: CBOR.Term
    }
    deriving stock (Eq, Show)

-- | The transaction-body fragment needed by proof binding.
data TxView = TxView
    { txInputs :: [TxIn]
    , txReferenceInputs :: [TxIn]
    , txMint :: [TxAsset]
    , txOutputs :: [TxOutView]
    , txRedeemers :: [TxRedeemer]
    }
    deriving stock (Eq, Show)

-- | Decode a Conway transaction enough to read regular inputs
-- and reference inputs. Conway CDDL:
--
-- * tx body key @0@: @set<transaction_input>@
-- * tx body key @18@: @nonempty_set<transaction_input>@
decodeTxView :: Text -> Hex -> Either VerifyError TxView
decodeTxView field h = do
    bs <- decodeTxBytes field h
    term <- decodeTerm field bs
    parseTx field term

-- | Decode a serialized Conway TxOut enough to read its non-ADA
-- assets and whether it carries an inline datum.
decodeTxOutView :: Text -> Hex -> Either VerifyError TxOutView
decodeTxOutView field h = do
    bs <- decodeTxBytes field h
    term <- decodeTerm field bs
    parseTxOut field term

-- | Compare decoded tx roles with the endpoint proof roles.
-- Ordering is ignored because Cardano encodes these values as sets.
verifyTxBinding
    :: Text
    -- ^ Endpoint prefix, e.g. @"retract"@.
    -> Hex
    -> [TxIn]
    -- ^ Proof roles that must be regular tx inputs.
    -> [TxIn]
    -- ^ Proof roles that must be tx reference inputs.
    -> Either VerifyError ()
verifyTxBinding endpoint tx expectedInputs expectedRefs =
    void (verifyTxInputBinding endpoint tx expectedInputs expectedRefs)

-- | Compare decoded tx input roles with the endpoint proof roles,
-- returning the parsed view for additional endpoint checks.
verifyTxInputBinding
    :: Text
    -- ^ Endpoint prefix, e.g. @"retract"@.
    -> Hex
    -> [TxIn]
    -- ^ Proof roles that must be regular tx inputs.
    -> [TxIn]
    -- ^ Proof roles that must be tx reference inputs.
    -> Either VerifyError TxView
verifyTxInputBinding endpoint tx expectedInputs expectedRefs = do
    view@TxView
        { txInputs = actualInputs
        , txReferenceInputs = actualRefs
        } <-
        decodeTxView (endpoint <> ".tx") tx
    compareTxIns
        (endpoint <> ".tx.inputs")
        "input set mismatch"
        expectedInputs
        actualInputs
    compareTxIns
        (endpoint <> ".tx.reference_inputs")
        "reference input set mismatch"
        expectedRefs
        actualRefs
    pure view

-- | Assert that an endpoint must not mint or burn assets.
verifyNoMint :: Text -> TxView -> Either VerifyError ()
verifyNoMint endpoint TxView{txMint} =
    compareTxAssets
        (endpoint <> ".tx.mint")
        "mint set mismatch"
        []
        txMint

-- | Boot must mint exactly one token and create exactly one inline
-- datum output carrying that same token.
verifyBootAssetBinding :: Text -> TxView -> Either VerifyError ()
verifyBootAssetBinding endpoint view = do
    minted <- expectSingletonMint endpoint 1 view
    requireStateOutput endpoint minted view

-- | Update/reject must preserve the consumed state token into
-- exactly one inline datum output and must not mint or burn.
verifyContinuingStateOutput
    :: Text -> TxView -> WitnessedUtxo -> Either VerifyError ()
verifyContinuingStateOutput endpoint view stateWitness = do
    verifyNoMint endpoint view
    stateAsset <- stateAssetFromWitness endpoint stateWitness
    requireStateOutput endpoint stateAsset view

-- | End must burn the consumed state token and must not leave a
-- continuing inline state output carrying that token.
verifyEndAssetBinding
    :: Text -> TxView -> WitnessedUtxo -> Either VerifyError ()
verifyEndAssetBinding endpoint view stateWitness = do
    stateAsset <- stateAssetFromWitness endpoint stateWitness
    let burnAsset =
            stateAsset
                { assetQuantity = negate (assetQuantity stateAsset)
                }
    compareTxAssets
        (endpoint <> ".tx.mint")
        "burn set mismatch"
        [burnAsset]
        (txMint view)
    forbidStateOutput endpoint stateAsset view

-- | Request endpoints currently have no script redeemers.
verifyNoRedeemers :: Text -> TxView -> Either VerifyError ()
verifyNoRedeemers endpoint view =
    compareRedeemerRoles
        (endpoint <> ".tx.redeemers")
        []
        =<< redeemerRoles endpoint view

-- | Boot must carry exactly one minting redeemer.
verifyBootRedeemerBinding :: Text -> TxView -> Either VerifyError ()
verifyBootRedeemerBinding endpoint view =
    compareRedeemerRoles
        (endpoint <> ".tx.redeemers")
        [RoleMinting]
        =<< redeemerRoles endpoint view

-- | Retract must spend the request input with a `Retract` redeemer
-- that points at the witnessed state reference input.
verifyRetractRedeemerBinding
    :: Text -> TxView -> TxIn -> TxIn -> Either VerifyError ()
verifyRetractRedeemerBinding endpoint view requestIn stateRef =
    compareRedeemerRoles
        (endpoint <> ".tx.redeemers")
        [RoleSpendRetract requestIn stateRef]
        =<< redeemerRoles endpoint view

-- | Reject must spend the state input with rejected actions and each
-- request input with a `Contribute` redeemer pointing at the state.
verifyRejectRedeemerBinding
    :: Text -> TxView -> TxIn -> [TxIn] -> Either VerifyError ()
verifyRejectRedeemerBinding endpoint view stateIn requestIns =
    compareRedeemerRoles
        (endpoint <> ".tx.redeemers")
        expected
        =<< redeemerRoles endpoint view
  where
    expected =
        RoleSpendModifyRejected stateIn (length requestIns)
            : [ RoleSpendContribute requestIn stateIn
              | requestIn <- requestIns
              ]

-- | End must spend the state input with `End` and burn with the
-- minting-policy `Burning` redeemer.
verifyEndRedeemerBinding
    :: Text -> TxView -> TxIn -> Either VerifyError ()
verifyEndRedeemerBinding endpoint view stateIn =
    compareRedeemerRoles
        (endpoint <> ".tx.redeemers")
        [RoleSpendEnd stateIn, RoleBurning]
        =<< redeemerRoles endpoint view

-- | Update must spend the state input with `Modify (Update ...)`,
-- spend every request with `Contribute stateRef`, and carry exactly
-- the MPF proof payloads advertised in `UpdateProof.trie_read`.
verifyUpdateRedeemerBinding
    :: Text
    -> TxView
    -> TxIn
    -> [TxIn]
    -> [TrieFact]
    -> Either VerifyError ()
verifyUpdateRedeemerBinding endpoint view stateIn requestIns trieRead = do
    proofTerms <- trieReadProofTerms endpoint trieRead
    roles <- redeemerRoles endpoint view
    compareRedeemerRoles
        (endpoint <> ".tx.redeemers")
        (expected proofTerms)
        roles
  where
    expected proofTerms =
        RoleSpendModifyUpdate stateIn proofTerms
            : [ RoleSpendContribute requestIn stateIn
              | requestIn <- requestIns
              ]

-- | Bind `UpdateProof.trie_root` to the root stored in the witnessed
-- state datum consumed by the transaction.
verifyStateRootBinding
    :: Text -> WitnessedUtxo -> Hex -> Either VerifyError ()
verifyStateRootBinding endpoint stateWitness expectedRoot = do
    actualRoot <- stateRootFromWitness endpoint stateWitness
    if actualRoot == expectedRoot
        then Right ()
        else
            Left
                ( TxBindingFailed
                    (endpoint <> ".state.tx_out.datum.root")
                    "state root mismatch"
                )

decodeTxBytes :: Text -> Hex -> Either VerifyError BS.ByteString
decodeTxBytes field (Hex txt) =
    case Base16.decode (T.encodeUtf8 txt) of
        Right bs
            | BS.null bs -> Left (MalformedTxCbor field)
            | otherwise -> Right bs
        Left _ -> Left (MalformedTxCbor field)

decodeTerm :: Text -> BS.ByteString -> Either VerifyError CBOR.Term
decodeTerm field bs =
    case CBOR.deserialiseFromBytes CBOR.decodeTerm (BSL.fromStrict bs) of
        Left _ ->
            Left (TxBindingFailed field "unsupported tx CBOR")
        Right (remaining, term)
            | BSL.null remaining -> Right term
            | otherwise ->
                Left (TxBindingFailed field "trailing tx CBOR bytes")

parseTx :: Text -> CBOR.Term -> Either VerifyError TxView
parseTx field = \case
    CBOR.TList [body, wits, _valid, _aux] ->
        parseTxParts field body wits
    CBOR.TListI [body, wits, _valid, _aux] ->
        parseTxParts field body wits
    _ ->
        Left (TxBindingFailed field "unsupported tx CBOR")

parseTxParts
    :: Text -> CBOR.Term -> CBOR.Term -> Either VerifyError TxView
parseTxParts field body wits = do
    view <- parseBody field body
    redeemers <- parseWitnessRedeemers (field <> ".redeemers") wits
    pure view{txRedeemers = redeemers}

parseBody :: Text -> CBOR.Term -> Either VerifyError TxView
parseBody field body = do
    entries <- case body of
        CBOR.TMap xs -> Right xs
        CBOR.TMapI xs -> Right xs
        _ ->
            Left
                ( TxBindingFailed
                    field
                    "unsupported transaction body CBOR"
                )
    inputs <-
        case lookupIntKey 0 entries of
            Nothing ->
                Left (TxBindingFailed (field <> ".inputs") "missing inputs")
            Just t ->
                parseInputSet (field <> ".inputs") t
    refs <-
        case lookupIntKey 18 entries of
            Nothing -> Right []
            Just t ->
                parseInputSet (field <> ".reference_inputs") t
    outputs <-
        case lookupIntKey 1 entries of
            Nothing -> Right []
            Just t ->
                parseOutputs (field <> ".outputs") t
    mint <-
        case lookupIntKey 9 entries of
            Nothing -> Right []
            Just t ->
                parseMultiAsset SignedMint (field <> ".mint") t
    pure
        TxView
            { txInputs = inputs
            , txReferenceInputs = refs
            , txMint = mint
            , txOutputs = outputs
            , txRedeemers = []
            }

parseWitnessRedeemers
    :: Text -> CBOR.Term -> Either VerifyError [TxRedeemer]
parseWitnessRedeemers field wits = do
    entries <- case wits of
        CBOR.TMap xs -> Right xs
        CBOR.TMapI xs -> Right xs
        _ ->
            Left
                ( TxBindingFailed
                    field
                    "unsupported witness set CBOR"
                )
    case lookupIntKey 5 entries of
        Nothing -> Right []
        Just t -> parseRedeemers field t

parseRedeemers :: Text -> CBOR.Term -> Either VerifyError [TxRedeemer]
parseRedeemers field = \case
    CBOR.TList xs ->
        traverse (parseFlatRedeemer field) xs
    CBOR.TListI xs ->
        traverse (parseFlatRedeemer field) xs
    CBOR.TMap xs ->
        traverse (uncurry (parseMapRedeemer field)) xs
    CBOR.TMapI xs ->
        traverse (uncurry (parseMapRedeemer field)) xs
    _ -> Left (TxBindingFailed field "unsupported redeemers CBOR")

parseFlatRedeemer
    :: Text -> CBOR.Term -> Either VerifyError TxRedeemer
parseFlatRedeemer field = \case
    CBOR.TList [tagTerm, ixTerm, datumTerm, _exUnits] ->
        mkRedeemer field tagTerm ixTerm datumTerm
    CBOR.TListI [tagTerm, ixTerm, datumTerm, _exUnits] ->
        mkRedeemer field tagTerm ixTerm datumTerm
    _ -> Left (TxBindingFailed field "unsupported flat redeemer CBOR")

parseMapRedeemer
    :: Text -> CBOR.Term -> CBOR.Term -> Either VerifyError TxRedeemer
parseMapRedeemer field keyTerm valueTerm = case (keyTerm, valueTerm) of
    (CBOR.TList [tagTerm, ixTerm], CBOR.TList [datumTerm, _exUnits]) ->
        mkRedeemer field tagTerm ixTerm datumTerm
    (CBOR.TListI [tagTerm, ixTerm], CBOR.TListI [datumTerm, _exUnits]) ->
        mkRedeemer field tagTerm ixTerm datumTerm
    (CBOR.TList [tagTerm, ixTerm], CBOR.TListI [datumTerm, _exUnits]) ->
        mkRedeemer field tagTerm ixTerm datumTerm
    (CBOR.TListI [tagTerm, ixTerm], CBOR.TList [datumTerm, _exUnits]) ->
        mkRedeemer field tagTerm ixTerm datumTerm
    _ -> Left (TxBindingFailed field "unsupported map redeemer CBOR")

mkRedeemer
    :: Text
    -> CBOR.Term
    -> CBOR.Term
    -> CBOR.Term
    -> Either VerifyError TxRedeemer
mkRedeemer field tagTerm ixTerm datumTerm = do
    tag <- parseNonNegativeInteger (field <> ".purpose") tagTerm
    ix <- parseWord64Unbounded (field <> ".index") ixTerm
    let purpose = case tag of
            0 -> RedeemerSpend ix
            1 -> RedeemerMint ix
            _ -> RedeemerOther tag ix
    pure
        TxRedeemer
            { txRedeemerPurpose = purpose
            , txRedeemerData = normalizeTerm datumTerm
            }

lookupIntKey :: Integer -> [(CBOR.Term, CBOR.Term)] -> Maybe CBOR.Term
lookupIntKey wanted = go
  where
    go [] = Nothing
    go ((k, v) : rest)
        | termInteger k == Just wanted = Just v
        | otherwise = go rest

termInteger :: CBOR.Term -> Maybe Integer
termInteger = \case
    CBOR.TInt n -> Just (fromIntegral n)
    CBOR.TInteger n -> Just n
    _ -> Nothing

parseInputSet :: Text -> CBOR.Term -> Either VerifyError [TxIn]
parseInputSet field = \case
    CBOR.TTagged 258 t -> parseInputSet field t
    CBOR.TList xs -> traverse (parseInput field) xs
    CBOR.TListI xs -> traverse (parseInput field) xs
    _ -> Left (TxBindingFailed field "unsupported input set CBOR")

parseInput :: Text -> CBOR.Term -> Either VerifyError TxIn
parseInput field = \case
    CBOR.TList [CBOR.TBytes txIdBs, ixTerm] ->
        mkInput field txIdBs ixTerm
    CBOR.TListI [CBOR.TBytes txIdBs, ixTerm] ->
        mkInput field txIdBs ixTerm
    _ -> Left (TxBindingFailed field "unsupported input CBOR")

mkInput
    :: Text -> BS.ByteString -> CBOR.Term -> Either VerifyError TxIn
mkInput field txIdBs ixTerm = do
    if BS.length txIdBs == 32
        then pure ()
        else Left (TxBindingFailed field "transaction id length mismatch")
    ix <- parseWord64 field ixTerm
    pure
        TxIn
            { txId = Hex (T.decodeUtf8 (Base16.encode txIdBs))
            , txIx = ix
            }

parseWord64 :: Text -> CBOR.Term -> Either VerifyError Word64
parseWord64 field t =
    case termInteger t of
        Just n
            | n >= 0 && n <= maxTxInputIndex ->
                Right (fromInteger n)
        _ -> Left (TxBindingFailed field "input index out of range")

parseWord64Unbounded :: Text -> CBOR.Term -> Either VerifyError Word64
parseWord64Unbounded field t =
    case termInteger t of
        Just n
            | n >= 0 && n <= toInteger (maxBound :: Word64) ->
                Right (fromInteger n)
        _ -> Left (TxBindingFailed field "index out of range")

parseNonNegativeInteger
    :: Text -> CBOR.Term -> Either VerifyError Integer
parseNonNegativeInteger field t =
    case termInteger t of
        Just n | n >= 0 -> Right n
        _ -> Left (TxBindingFailed field "expected non-negative integer")

maxTxInputIndex :: Integer
maxTxInputIndex = 65535

data QuantityMode = SignedMint | PositiveValue

parseOutputs :: Text -> CBOR.Term -> Either VerifyError [TxOutView]
parseOutputs field = \case
    CBOR.TList xs -> traverse (parseTxOut field) xs
    CBOR.TListI xs -> traverse (parseTxOut field) xs
    _ -> Left (TxBindingFailed field "unsupported outputs CBOR")

parseTxOut :: Text -> CBOR.Term -> Either VerifyError TxOutView
parseTxOut field = \case
    CBOR.TMap xs -> parseBabbageTxOut field xs
    CBOR.TMapI xs -> parseBabbageTxOut field xs
    CBOR.TList [_, valueTerm] ->
        txOutWithoutInlineDatum field valueTerm
    CBOR.TList [_, valueTerm, _datumHash] ->
        txOutWithoutInlineDatum field valueTerm
    CBOR.TListI [_, valueTerm] ->
        txOutWithoutInlineDatum field valueTerm
    CBOR.TListI [_, valueTerm, _datumHash] ->
        txOutWithoutInlineDatum field valueTerm
    _ -> Left (TxBindingFailed field "unsupported tx output CBOR")

parseBabbageTxOut
    :: Text -> [(CBOR.Term, CBOR.Term)] -> Either VerifyError TxOutView
parseBabbageTxOut field entries = do
    valueTerm <- case lookupIntKey 1 entries of
        Nothing ->
            Left (TxBindingFailed (field <> ".value") "missing value")
        Just t -> Right t
    assets <- parseValue (field <> ".value") valueTerm
    inlineDatum <- case lookupIntKey 2 entries of
        Nothing -> Right Nothing
        Just t -> parseDatumOption (field <> ".datum") t
    pure
        TxOutView
            { txOutAssets = assets
            , txOutHasInlineDatum = isJust inlineDatum
            , txOutInlineDatum = inlineDatum
            }

txOutWithoutInlineDatum
    :: Text -> CBOR.Term -> Either VerifyError TxOutView
txOutWithoutInlineDatum field valueTerm = do
    assets <- parseValue (field <> ".value") valueTerm
    pure
        TxOutView
            { txOutAssets = assets
            , txOutHasInlineDatum = False
            , txOutInlineDatum = Nothing
            }

parseDatumOption
    :: Text -> CBOR.Term -> Either VerifyError (Maybe CBOR.Term)
parseDatumOption field = \case
    CBOR.TList [tagTerm, _]
        | termInteger tagTerm == Just 0 -> Right Nothing
    CBOR.TList [tagTerm, datumTerm]
        | termInteger tagTerm == Just 1 -> do
            datum <- parseInlineDatum (field <> ".inline") datumTerm
            Right (Just datum)
    CBOR.TListI [tagTerm, _]
        | termInteger tagTerm == Just 0 -> Right Nothing
    CBOR.TListI [tagTerm, datumTerm]
        | termInteger tagTerm == Just 1 -> do
            datum <- parseInlineDatum (field <> ".inline") datumTerm
            Right (Just datum)
    _ -> Left (TxBindingFailed field "unsupported datum option CBOR")

parseInlineDatum :: Text -> CBOR.Term -> Either VerifyError CBOR.Term
parseInlineDatum field = \case
    CBOR.TTagged 24 (CBOR.TBytes encoded) -> do
        term <- decodeTerm field encoded
        Right (normalizeTerm term)
    CBOR.TTagged 24 _ ->
        Left (TxBindingFailed field "unsupported encoded datum CBOR")
    datumTerm ->
        Right (normalizeTerm datumTerm)

parseValue :: Text -> CBOR.Term -> Either VerifyError [TxAsset]
parseValue field = \case
    t | isJust (termInteger t) -> Right []
    CBOR.TList [_coinTerm, maTerm] ->
        parseMultiAsset PositiveValue (field <> ".assets") maTerm
    CBOR.TListI [_coinTerm, maTerm] ->
        parseMultiAsset PositiveValue (field <> ".assets") maTerm
    _ -> Left (TxBindingFailed field "unsupported value CBOR")

parseMultiAsset
    :: QuantityMode -> Text -> CBOR.Term -> Either VerifyError [TxAsset]
parseMultiAsset mode field term = do
    policies <- parseMap field term
    concat
        <$> traverse (uncurry (parsePolicyAssets mode field)) policies

parsePolicyAssets
    :: QuantityMode
    -> Text
    -> CBOR.Term
    -> CBOR.Term
    -> Either VerifyError [TxAsset]
parsePolicyAssets mode field policyTerm assetMapTerm = do
    policy <- parseBoundedBytes (field <> ".policy_id") 28 28 policyTerm
    assets <- parseMap (field <> ".assets") assetMapTerm
    traverse (uncurry (parseAsset mode field policy)) assets

parseAsset
    :: QuantityMode
    -> Text
    -> Hex
    -> CBOR.Term
    -> CBOR.Term
    -> Either VerifyError TxAsset
parseAsset mode field policy nameTerm quantityTerm = do
    name <- parseBoundedBytes (field <> ".asset_name") 0 32 nameTerm
    quantity <-
        parseAssetQuantity mode (field <> ".quantity") quantityTerm
    pure
        TxAsset
            { assetPolicy = policy
            , assetName = name
            , assetQuantity = quantity
            }

parseMap
    :: Text -> CBOR.Term -> Either VerifyError [(CBOR.Term, CBOR.Term)]
parseMap field = \case
    CBOR.TMap xs -> Right xs
    CBOR.TMapI xs -> Right xs
    _ -> Left (TxBindingFailed field "unsupported map CBOR")

parseBoundedBytes
    :: Text -> Int -> Int -> CBOR.Term -> Either VerifyError Hex
parseBoundedBytes field minLen maxLen = \case
    CBOR.TBytes bs
        | BS.length bs >= minLen && BS.length bs <= maxLen ->
            Right (Hex (T.decodeUtf8 (Base16.encode bs)))
        | otherwise ->
            Left (TxBindingFailed field "byte length mismatch")
    _ -> Left (TxBindingFailed field "expected bytes")

parseAssetQuantity
    :: QuantityMode -> Text -> CBOR.Term -> Either VerifyError Integer
parseAssetQuantity mode field t =
    case termInteger t of
        Just n
            | n >= minInt64 && n <= maxInt64 && quantityAllowed mode n ->
                Right n
        _ -> Left (TxBindingFailed field "asset quantity out of range")

quantityAllowed :: QuantityMode -> Integer -> Bool
quantityAllowed SignedMint n = n /= 0
quantityAllowed PositiveValue n = n > 0

normalizeTerm :: CBOR.Term -> CBOR.Term
normalizeTerm = \case
    CBOR.TInt n -> CBOR.TInteger (fromIntegral n)
    CBOR.TBytesI bs -> CBOR.TBytes (BSL.toStrict bs)
    CBOR.TStringI txt -> CBOR.TString (TL.toStrict txt)
    CBOR.TList xs -> CBOR.TList (map normalizeTerm xs)
    CBOR.TListI xs -> CBOR.TList (map normalizeTerm xs)
    CBOR.TMap xs ->
        CBOR.TMap
            [ (normalizeTerm k, normalizeTerm v)
            | (k, v) <- xs
            ]
    CBOR.TMapI xs ->
        CBOR.TMap
            [ (normalizeTerm k, normalizeTerm v)
            | (k, v) <- xs
            ]
    CBOR.TTagged tag t -> CBOR.TTagged tag (normalizeTerm t)
    t -> t

minInt64, maxInt64 :: Integer
minInt64 = -9223372036854775808
maxInt64 = 9223372036854775807

compareTxIns
    :: Text
    -> Text
    -> [TxIn]
    -> [TxIn]
    -> Either VerifyError ()
compareTxIns field reason expected actual
    | canonical expected == canonical actual = Right ()
    | otherwise =
        Left
            ( TxBindingFailed
                field
                ( reason
                    <> ": expected "
                    <> renderCount expected
                    <> ", got "
                    <> renderCount actual
                )
            )
  where
    canonical = sort
    renderCount xs =
        T.pack (show (length xs)) <> " tx input(s)"

compareTxAssets
    :: Text
    -> Text
    -> [TxAsset]
    -> [TxAsset]
    -> Either VerifyError ()
compareTxAssets field reason expected actual
    | canonical expected == canonical actual = Right ()
    | otherwise =
        Left
            ( TxBindingFailed
                field
                ( reason
                    <> ": expected "
                    <> renderCount expected
                    <> ", got "
                    <> renderCount actual
                )
            )
  where
    canonical = sort
    renderCount xs =
        T.pack (show (length xs)) <> " asset(s)"

compareRedeemerRoles
    :: Text -> [TxRedeemerRole] -> [TxRedeemerRole] -> Either VerifyError ()
compareRedeemerRoles field expected actual
    | canonical expected == canonical actual = Right ()
    | otherwise =
        Left
            ( TxBindingFailed
                field
                ( "redeemer role mismatch: expected "
                    <> renderCount expected
                    <> ", got "
                    <> renderCount actual
                )
            )
  where
    canonical = sort
    renderCount xs =
        T.pack (show (length xs)) <> " redeemer(s)"

redeemerRoles :: Text -> TxView -> Either VerifyError [TxRedeemerRole]
redeemerRoles endpoint view@TxView{txRedeemers} =
    traverse (redeemerRole endpoint view) txRedeemers

redeemerRole
    :: Text -> TxView -> TxRedeemer -> Either VerifyError TxRedeemerRole
redeemerRole endpoint view TxRedeemer{..} =
    case txRedeemerPurpose of
        RedeemerMint _ ->
            parseMintRedeemerRole
                (endpoint <> ".tx.redeemers.mint")
                txRedeemerPurpose
                txRedeemerData
        RedeemerSpend ix -> do
            input <- redeemerInput endpoint view ix
            parseSpendRedeemerRole
                (endpoint <> ".tx.redeemers.spend")
                input
                txRedeemerData
        RedeemerOther{} ->
            pure
                ( RoleUnsupported
                    txRedeemerPurpose
                    "unsupported redeemer purpose"
                )

redeemerInput
    :: Text -> TxView -> Word64 -> Either VerifyError TxIn
redeemerInput endpoint TxView{txInputs} ix =
    case drop (fromIntegral ix) txInputs of
        input : _ -> Right input
        [] ->
            Left
                ( TxBindingFailed
                    (endpoint <> ".tx.redeemers.spend")
                    "spending redeemer index out of range"
                )

parseMintRedeemerRole
    :: Text
    -> TxRedeemerPurpose
    -> CBOR.Term
    -> Either VerifyError TxRedeemerRole
parseMintRedeemerRole field purpose term = do
    (tag, _fields) <- parseConstr field term
    case tag of
        0 -> Right RoleMinting
        2 -> Right RoleBurning
        _ ->
            Right
                ( RoleUnsupported
                    purpose
                    "unsupported mint redeemer"
                )

parseSpendRedeemerRole
    :: Text -> TxIn -> CBOR.Term -> Either VerifyError TxRedeemerRole
parseSpendRedeemerRole field input term = do
    (tag, fields) <- parseConstr field term
    case (tag, fields) of
        (0, []) -> Right (RoleSpendEnd input)
        (1, [refTerm]) ->
            RoleSpendContribute input
                <$> parseTxOutRef (field <> ".contribute") refTerm
        (2, [actionsTerm]) ->
            parseModifyRole field input actionsTerm
        (3, [refTerm]) ->
            RoleSpendRetract input
                <$> parseTxOutRef (field <> ".retract") refTerm
        _ ->
            Right
                ( RoleUnsupported
                    (RedeemerSpend 0)
                    "unsupported spend redeemer"
                )

parseModifyRole
    :: Text -> TxIn -> CBOR.Term -> Either VerifyError TxRedeemerRole
parseModifyRole field input actionsTerm = do
    actions <- parseList (field <> ".modify.actions") actionsTerm
    parsed <- traverse (parseRequestAction field) actions
    case parsed of
        [] -> Right (RoleSpendModifyUpdate input [])
        xs
            | all isRejectedAction xs ->
                Right (RoleSpendModifyRejected input (length xs))
            | all isUpdateAction xs ->
                Right
                    ( RoleSpendModifyUpdate
                        input
                        [ proof
                        | ParsedUpdateAction proof <- xs
                        ]
                    )
            | otherwise ->
                Right
                    ( RoleUnsupported
                        (RedeemerSpend 0)
                        "mixed request action redeemer"
                    )

data ParsedRequestAction
    = ParsedUpdateAction CBOR.Term
    | ParsedRejectedAction
    deriving stock (Eq, Show)

isRejectedAction :: ParsedRequestAction -> Bool
isRejectedAction ParsedRejectedAction = True
isRejectedAction _ = False

isUpdateAction :: ParsedRequestAction -> Bool
isUpdateAction ParsedUpdateAction{} = True
isUpdateAction _ = False

parseRequestAction
    :: Text -> CBOR.Term -> Either VerifyError ParsedRequestAction
parseRequestAction field term = do
    (tag, fields) <- parseConstr (field <> ".action") term
    case (tag, fields) of
        (0, [proofTerm]) ->
            Right (ParsedUpdateAction (normalizeTerm proofTerm))
        (1, []) -> Right ParsedRejectedAction
        _ ->
            Left
                ( TxBindingFailed
                    (field <> ".action")
                    "unsupported request action redeemer"
                )

parseTxOutRef :: Text -> CBOR.Term -> Either VerifyError TxIn
parseTxOutRef field term = do
    (tag, fields) <- parseConstr field term
    case (tag, fields) of
        (0, [CBOR.TBytes txIdBs, ixTerm])
            | BS.length txIdBs == 32 -> do
                ix <- parseWord64Unbounded (field <> ".tx_ix") ixTerm
                Right
                    TxIn
                        { txId = Hex (T.decodeUtf8 (Base16.encode txIdBs))
                        , txIx = ix
                        }
        _ ->
            Left
                ( TxBindingFailed
                    field
                    "unsupported tx out ref CBOR"
                )

parseConstr
    :: Text -> CBOR.Term -> Either VerifyError (Integer, [CBOR.Term])
parseConstr field = \case
    CBOR.TTagged tag fieldsTerm -> do
        constr <- constructorIndex tag
        fields <- parseList field fieldsTerm
        Right (constr, fields)
    _ -> Left (TxBindingFailed field "expected constructor data")
  where
    constructorIndex tag
        | tag >= 121 && tag <= 127 =
            Right (toInteger tag - 121)
        | tag >= 1280 && tag <= 1400 =
            Right (toInteger tag - 1280 + 7)
        | tag == 102 =
            Left
                ( TxBindingFailed
                    field
                    "generic constructor data unsupported"
                )
        | otherwise =
            Left (TxBindingFailed field "unsupported constructor tag")

parseList :: Text -> CBOR.Term -> Either VerifyError [CBOR.Term]
parseList field = \case
    CBOR.TList xs -> Right (map normalizeTerm xs)
    CBOR.TListI xs -> Right (map normalizeTerm xs)
    _ -> Left (TxBindingFailed field "expected list data")

trieReadProofTerms
    :: Text -> [TrieFact] -> Either VerifyError [CBOR.Term]
trieReadProofTerms endpoint = go (0 :: Int)
  where
    go _ [] = Right []
    go i (TrieFact{mpfProof} : rest) = do
        let field =
                endpoint
                    <> ".trie_read["
                    <> T.pack (show i)
                    <> "].mpf_proof"
        proofBs <- decodeHexBinding field mpfProof
        term <- decodeTerm field proofBs
        (normalizeTerm term :) <$> go (i + 1) rest

decodeHexBinding :: Text -> Hex -> Either VerifyError BS.ByteString
decodeHexBinding field (Hex txt) =
    case Base16.decode (T.encodeUtf8 txt) of
        Right bs -> Right bs
        Left _ -> Left (TxBindingFailed field "malformed hex")

expectSingletonMint
    :: Text -> Integer -> TxView -> Either VerifyError TxAsset
expectSingletonMint endpoint expectedQuantity TxView{txMint} =
    case txMint of
        [asset]
            | assetQuantity asset == expectedQuantity -> Right asset
        _ ->
            Left
                ( TxBindingFailed
                    (endpoint <> ".tx.mint")
                    "expected exactly one mint asset"
                )

stateAssetFromWitness
    :: Text -> WitnessedUtxo -> Either VerifyError TxAsset
stateAssetFromWitness endpoint WitnessedUtxo{txOut} = do
    TxOutView{txOutAssets} <-
        decodeTxOutView (endpoint <> ".state.tx_out") txOut
    case txOutAssets of
        [asset] -> Right asset
        [] ->
            Left
                ( TxBindingFailed
                    (endpoint <> ".state.tx_out.value")
                    "state token missing"
                )
        _ ->
            Left
                ( TxBindingFailed
                    (endpoint <> ".state.tx_out.value")
                    "state token ambiguous"
                )

stateRootFromWitness
    :: Text -> WitnessedUtxo -> Either VerifyError Hex
stateRootFromWitness endpoint WitnessedUtxo{txOut} = do
    TxOutView{txOutInlineDatum} <-
        decodeTxOutView (endpoint <> ".state.tx_out") txOut
    case txOutInlineDatum of
        Nothing ->
            Left
                ( TxBindingFailed
                    (endpoint <> ".state.tx_out.datum")
                    "state datum missing"
                )
        Just datum ->
            parseStateDatumRoot
                (endpoint <> ".state.tx_out.datum")
                datum

-- | Extract the raw 32-byte trie root from the inline state datum of
-- a witnessed state UTxO. Like 'stateRootFromWitness' but returns the
-- decoded bytes rather than the hex rendering, for callers that
-- reconstruct or compare the root in its binary form.
stateRootBytesFromWitness
    :: Text -> WitnessedUtxo -> Either VerifyError BS.ByteString
stateRootBytesFromWitness endpoint witness = do
    Hex rootHex <- stateRootFromWitness endpoint witness
    case Base16.decode (T.encodeUtf8 rootHex) of
        Right bs -> Right bs
        Left _ ->
            Left
                ( TxBindingFailed
                    (endpoint <> ".state.tx_out.datum.root")
                    "malformed state root hex"
                )

parseStateDatumRoot :: Text -> CBOR.Term -> Either VerifyError Hex
parseStateDatumRoot field datum = do
    (datumTag, datumFields) <- parseConstr field datum
    case (datumTag, datumFields) of
        (1, [stateTerm]) -> parseTokenStateRoot field stateTerm
        _ -> Left (TxBindingFailed field "expected state datum")

parseTokenStateRoot :: Text -> CBOR.Term -> Either VerifyError Hex
parseTokenStateRoot field stateTerm = do
    (stateTag, stateFields) <- parseConstr (field <> ".state") stateTerm
    case (stateTag, stateFields) of
        (0, [_owner, CBOR.TBytes rootBs, _fee, _process, _retract])
            | BS.length rootBs == 32 ->
                Right (Hex (T.decodeUtf8 (Base16.encode rootBs)))
        _ ->
            Left
                ( TxBindingFailed
                    (field <> ".state.root")
                    "state root missing"
                )

requireStateOutput
    :: Text -> TxAsset -> TxView -> Either VerifyError ()
requireStateOutput endpoint asset TxView{txOutputs} =
    case filter (outputCarries asset) txOutputs of
        [TxOutView{txOutHasInlineDatum = True}] -> Right ()
        _ ->
            Left
                ( TxBindingFailed
                    (endpoint <> ".tx.state_outputs")
                    "state output mismatch"
                )

forbidStateOutput
    :: Text -> TxAsset -> TxView -> Either VerifyError ()
forbidStateOutput endpoint asset TxView{txOutputs}
    | any (outputCarries asset) txOutputs =
        Left
            ( TxBindingFailed
                (endpoint <> ".tx.state_outputs")
                "unexpected continuing state output"
            )
    | otherwise = Right ()

outputCarries :: TxAsset -> TxOutView -> Bool
outputCarries expected TxOutView{txOutAssets} =
    any (sameAsset expected) txOutAssets

sameAsset :: TxAsset -> TxAsset -> Bool
sameAsset expected actual =
    assetPolicy expected == assetPolicy actual
        && assetName expected == assetName actual
        && assetQuantity expected == assetQuantity actual
