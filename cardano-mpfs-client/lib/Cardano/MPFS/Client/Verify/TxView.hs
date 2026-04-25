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
    , TxView (..)
    , decodeTxView
    , decodeTxOutView
    , verifyBootAssetBinding
    , verifyContinuingStateOutput
    , verifyEndAssetBinding
    , verifyNoMint
    , verifyTxInputBinding
    , verifyTxBinding
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
import Data.Word (Word64)

import Cardano.MPFS.Client.Bundle
    ( TxIn (..)
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
    }
    deriving stock (Eq, Show)

-- | The transaction-body fragment needed by proof binding.
data TxView = TxView
    { txInputs :: [TxIn]
    , txReferenceInputs :: [TxIn]
    , txMint :: [TxAsset]
    , txOutputs :: [TxOutView]
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
    CBOR.TList [body, _wits, _valid, _aux] ->
        parseBody field body
    CBOR.TListI [body, _wits, _valid, _aux] ->
        parseBody field body
    _ ->
        Left (TxBindingFailed field "unsupported tx CBOR")

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
    hasInlineDatum <- case lookupIntKey 2 entries of
        Nothing -> Right False
        Just t -> parseDatumOption (field <> ".datum") t
    pure
        TxOutView{txOutAssets = assets, txOutHasInlineDatum = hasInlineDatum}

txOutWithoutInlineDatum
    :: Text -> CBOR.Term -> Either VerifyError TxOutView
txOutWithoutInlineDatum field valueTerm = do
    assets <- parseValue (field <> ".value") valueTerm
    pure TxOutView{txOutAssets = assets, txOutHasInlineDatum = False}

parseDatumOption :: Text -> CBOR.Term -> Either VerifyError Bool
parseDatumOption field = \case
    CBOR.TList [tagTerm, _]
        | termInteger tagTerm == Just 0 -> Right False
        | termInteger tagTerm == Just 1 -> Right True
    CBOR.TListI [tagTerm, _]
        | termInteger tagTerm == Just 0 -> Right False
        | termInteger tagTerm == Just 1 -> Right True
    _ -> Left (TxBindingFailed field "unsupported datum option CBOR")

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
