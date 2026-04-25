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
    ( TxView (..)
    , decodeTxView
    , verifyTxBinding
    ) where

import Codec.CBOR.Read qualified as CBOR
import Codec.CBOR.Term qualified as CBOR
import Data.ByteString qualified as BS
import Data.ByteString.Base16 qualified as Base16
import Data.ByteString.Lazy qualified as BSL
import Data.List (sort)
import Data.Text (Text)
import Data.Text qualified as T
import Data.Text.Encoding qualified as T
import Data.Word (Word64)

import Cardano.MPFS.Client.Bundle
    ( TxIn (..)
    )
import Cardano.MPFS.Client.Snapshot
    ( Hex (..)
    )
import Cardano.MPFS.Client.Verify.Replay
    ( VerifyError (..)
    )

-- | The transaction-body fragment needed by proof binding.
data TxView = TxView
    { txInputs :: [TxIn]
    , txReferenceInputs :: [TxIn]
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
verifyTxBinding endpoint tx expectedInputs expectedRefs = do
    TxView actualInputs actualRefs <-
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
    CBOR.TList (body : _wits : _valid : _aux : []) ->
        parseBody field body
    CBOR.TListI (body : _wits : _valid : _aux : []) ->
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
    pure TxView{txInputs = inputs, txReferenceInputs = refs}

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
