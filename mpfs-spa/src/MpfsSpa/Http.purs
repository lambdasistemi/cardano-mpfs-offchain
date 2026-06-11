-- | HTTP client for the MPFS offchain server.
-- |
-- | Read endpoints (`/tokens`, `/tokens/{id}`, `/tokens/{id}/requests`,
-- | `/tokens/{id}/facts/{key}`) and the `/submit` forward. Responses are
-- | proof-bearing; the SPA decodes only the display fields it needs and
-- | leaves snapshots / inclusion proofs as opaque JSON — proof verification
-- | belongs to the verifier (wasm), not the UI. No protocol logic here.
module MpfsSpa.Http
  ( Config
  , TokenState
  , PendingRequest
  , RequestPhase(..)
  , FactEntry
  , getTokens
  , getFacts
  , getTokenState
  , getRequests
  , getFactValue
  , getTrustedRoot
  , getEvalContext
  , isProcessable
  , phaseLabel
  , requestPhase
  , postBootFacts
  , submitTx
  ) where

import Prelude

import Control.Promise (Promise, toAffE)
import Data.Argonaut.Core (Json, stringify)
import Data.Argonaut.Decode (decodeJson)
import Data.Argonaut.Decode.Combinators ((.:), (.:?))
import Data.Argonaut.Decode.Error (JsonDecodeError(..), printJsonDecodeError)
import Data.Argonaut.Encode (encodeJson)
import Data.Bifunctor (lmap)
import Data.Either (Either(..))
import Data.Maybe (Maybe(..))
import Data.Nullable (Nullable, toNullable)
import Data.Traversable (sequence, traverse)
import Effect (Effect)
import Effect.Aff (Aff)

import MpfsSpa.CageHelpers.Reactor (decodeTxOut)
import MpfsSpa.Types
  ( CageError
  , Key(..)
  , RequestId(..)
  , TokenId(..)
  , TrustedRoot(..)
  , Value(..)
  , WalletAddr(..)
  , cageErrorMessage
  )

-- | Server connection config. `baseUrl` has no trailing slash.
type Config = { baseUrl :: String }

-- | On-chain token state surfaced for display (proof fields omitted).
type TokenState =
  { owner :: String
  , root :: String
  , tip :: Int
  , processTime :: Int
  , retractTime :: Int
  }

-- | A pending request for display + retract. `requestId` is the request
-- | UTxO reference (`tx_id#tx_ix`) from the witness. `value` is the
-- | primary value to show (the inserted value for inserts, the removed
-- | value for deletes, the new value for updates); `oldValue` carries an
-- | update's previous value so updates can display BOTH sides. All fields
-- | are decoded from the witnessed `tx_out` inline datum.
type PendingRequest =
  { token :: TokenId
  , owner :: String
  , key :: Key
  , value :: Maybe Value
  , oldValue :: Maybe Value
  , operation :: String
  , fee :: Number
  , submittedAt :: Number
  , requestId :: RequestId
  }

data RequestPhase
  = PhaseProcessable
  | PhaseRetractable
  | PhaseExpired

derive instance Eq RequestPhase

type FactEntry =
  { key :: Key
  , value :: Value
  }

isProcessable :: Number -> Number -> PendingRequest -> Boolean
isProcessable nowMillis processTime req =
  nowMillis - req.submittedAt <= processTime

requestPhase :: Number -> Number -> Number -> PendingRequest -> RequestPhase
requestPhase nowMillis processTime retractTime req =
  let
    age = nowMillis - req.submittedAt
  in
    if age <= processTime then
      PhaseProcessable
    else if age <= processTime + retractTime then
      PhaseRetractable
    else
      PhaseExpired

phaseLabel :: RequestPhase -> String
phaseLabel = case _ of
  PhaseProcessable -> "processable"
  PhaseRetractable -> "retractable"
  PhaseExpired -> "expired"

type RawResponse = { ok :: Boolean, status :: Int, json :: Json, text :: String }

foreign import _fetchJson
  :: String -> String -> Nullable String -> Effect (Promise RawResponse)

-- | Issue a request, returning the decoded JSON body or an HTTP/error string.
request :: String -> String -> Maybe String -> Aff (Either String Json)
request method url mbody = do
  res <- toAffE (_fetchJson method url (toNullable mbody))
  pure
    if res.ok then Right res.json
    else Left ("HTTP " <> show res.status <> ": " <> res.text)

-- | GET and decode, threading decode errors into the error string.
getDecoded
  :: forall a
   . Config
  -> String
  -> (Json -> Either JsonDecodeError a)
  -> Aff (Either String a)
getDecoded cfg path decode = do
  ej <- request "GET" (cfg.baseUrl <> path) Nothing
  pure (ej >>= (lmap printJsonDecodeError <<< decode))

-- | List all token ids the server knows. Each token id is the asset name
-- | of the state token inside the entry's witnessed `txout_cbor`, decoded
-- | through the reactor rather than read from a removed projection (#345).
getTokens :: Config -> Aff (Either String (Array TokenId))
getTokens cfg = do
  ej <- request "GET" (cfg.baseUrl <> "/tokens") Nothing
  case ej of
    Left err -> pure (Left err)
    Right j -> case tokenEntryTxOuts j of
      Right hexes -> map sequence (traverse decodeTokenIdVia hexes)
      Left _ ->
        -- Fallback: a plain array of token-id strings carries no witness
        -- to decode.
        pure
          ( lmap printJsonDecodeError
              (map TokenId <$> (decodeJson j :: Either JsonDecodeError (Array String)))
          )

-- | Enumerate every fact in a token's trie.
getFacts :: Config -> TokenId -> Aff (Either String (Array FactEntry))
getFacts cfg (TokenId tid) =
  getDecoded cfg ("/tokens/" <> tid <> "/facts") decodeFacts

-- | Fetch a token's on-chain state by decoding the witnessed state
-- | `tx_out` inline datum through the reactor (#345).
getTokenState :: Config -> TokenId -> Aff (Either String TokenState)
getTokenState cfg (TokenId tid) = do
  ej <- request "GET" (cfg.baseUrl <> "/tokens/" <> tid) Nothing
  case ej of
    Left err -> pure (Left err)
    Right j -> case stateTxOutHex j of
      Left de -> pure (Left (printJsonDecodeError de))
      Right hex -> do
        decoded <- decodeTxOut hex
        pure
          ( reactorEither decoded
              >>= (lmap printJsonDecodeError <<< decodeReactorState)
          )

-- | Fetch a token's pending requests, decoding each witnessed request
-- | `tx_out` inline datum through the reactor (#345).
getRequests :: Config -> TokenId -> Aff (Either String (Array PendingRequest))
getRequests cfg (TokenId tid) = do
  ej <- request "GET" (cfg.baseUrl <> "/tokens/" <> tid <> "/requests") Nothing
  case ej of
    Left err -> pure (Left err)
    Right j -> case requestWitnesses j of
      Left de -> pure (Left (printJsonDecodeError de))
      Right witnesses -> map sequence (traverse decodeRequestVia witnesses)

-- | Fetch the current value of a fact key (hex), if present.
getFactValue :: Config -> TokenId -> Key -> Aff (Either String Value)
getFactValue cfg (TokenId tid) (Key key) =
  getDecoded cfg ("/tokens/" <> tid <> "/facts/" <> key) decodeFactValue

-- | Fetch the trusted UTxO root from `/status`.
getTrustedRoot :: Config -> Aff (Either String TrustedRoot)
getTrustedRoot cfg = getDecoded cfg "/status" decodeTrustedRoot

-- | Fetch the trusted-not-proven ledger evaluation context as opaque JSON.
getEvalContext :: Config -> Aff (Either String Json)
getEvalContext cfg = request "GET" (cfg.baseUrl <> "/eval-context") Nothing

-- | Fetch raw proof-bearing boot facts for the reactor envelope.
postBootFacts :: Config -> WalletAddr -> Aff (Either String Json)
postBootFacts cfg (WalletAddr address) = do
  let body = stringify (encodeJson { address })
  request "POST" (cfg.baseUrl <> "/facts/boot") (Just body)

-- | Forward a signed transaction; returns the accepted transaction id.
submitTx :: Config -> String -> Aff (Either String String)
submitTx cfg signedHex = do
  let body = stringify (encodeJson { signedTxCbor: signedHex })
  ej <- request "POST" (cfg.baseUrl <> "/submit") (Just body)
  pure (ej >>= (lmap printJsonDecodeError <<< decodeTxId))

-- --- decoders ---------------------------------------------------------------

-- | #342 removed the server-side projections (`state.state`, `token_id`,
-- | `request`) these read paths used to trust. They are now reconstructed
-- | by decoding the witnessed `tx_out` inline datum through the reactor
-- | `decode` op (#345): proof-bearing bytes only, no PureScript CBOR, no
-- | fabricated values.

-- | Lift a reactor `CageError` into the shared `Either String` channel.
reactorEither :: forall a. Either CageError a -> Either String a
reactorEither = lmap cageErrorMessage

-- | Pull each token entry's witnessed `txout_cbor` hex from a `/tokens`
-- | response.
tokenEntryTxOuts :: Json -> Either JsonDecodeError (Array String)
tokenEntryTxOuts j = do
  top <- decodeJson j
  tokens <- top .: "tokens"
  (entries :: Array Json) <- tokens .: "entries"
  traverse tokenEntryTxOut entries

tokenEntryTxOut :: Json -> Either JsonDecodeError String
tokenEntryTxOut j = do
  entry <- decodeJson j
  entry .: "txout_cbor"

-- | Decode one token id from its witnessed state `tx_out` via the reactor.
decodeTokenIdVia :: String -> Aff (Either String TokenId)
decodeTokenIdVia hex = do
  decoded <- decodeTxOut hex
  pure
    ( reactorEither decoded
        >>= (lmap printJsonDecodeError <<< decodeReactorTokenId)
    )

decodeReactorTokenId :: Json -> Either JsonDecodeError TokenId
decodeReactorTokenId j = do
  o <- decodeJson j
  tid <- o .: "token_id"
  pure (TokenId tid)

-- | Pull the witnessed state `tx_out` hex from a `/tokens/:id` response.
stateTxOutHex :: Json -> Either JsonDecodeError String
stateTxOutHex j = do
  top <- decodeJson j
  state <- top .: "state"
  utxo <- state .: "utxo"
  utxo .: "tx_out"

-- | Build the display `TokenState` from the reactor-decoded state datum.
decodeReactorState :: Json -> Either JsonDecodeError TokenState
decodeReactorState j = do
  o <- decodeJson j
  owner <- o .: "owner"
  root <- o .: "root"
  tip <- o .: "tip"
  processTime <- o .: "process_time"
  retractTime <- o .: "retract_time"
  pure { owner, root, tip, processTime, retractTime }

decodeFacts :: Json -> Either JsonDecodeError (Array FactEntry)
decodeFacts j = do
  top <- decodeJson j
  (facts :: Array Json) <- top .: "facts"
  traverse decodeFactEntry facts

decodeFactEntry :: Json -> Either JsonDecodeError FactEntry
decodeFactEntry j = do
  fact <- decodeJson j
  key <- fact .: "key"
  value <- fact .: "value"
  pure { key: Key key, value: Value value }

-- | A witnessed pending request: the inline-datum `tx_out` hex to decode
-- | plus the request UTxO reference (`tx_id#tx_ix`) for retract.
type RequestWitness =
  { txOutHex :: String
  , requestId :: RequestId
  }

requestWitnesses :: Json -> Either JsonDecodeError (Array RequestWitness)
requestWitnesses j = do
  top <- decodeJson j
  (reqs :: Array Json) <- top .: "requests"
  traverse requestWitness reqs

requestWitness :: Json -> Either JsonDecodeError RequestWitness
requestWitness j = do
  o <- decodeJson j
  utxo <- o .: "utxo"
  txOutHex <- utxo .: "tx_out"
  txin <- utxo .: "tx_in"
  txId <- txin .: "tx_id"
  (txIx :: Int) <- txin .: "tx_ix"
  pure { txOutHex, requestId: RequestId (txId <> "#" <> show txIx) }

-- | Decode one pending request from its witnessed `tx_out` via the reactor.
decodeRequestVia :: RequestWitness -> Aff (Either String PendingRequest)
decodeRequestVia w = do
  decoded <- decodeTxOut w.txOutHex
  pure
    ( reactorEither decoded
        >>= (lmap printJsonDecodeError <<< decodeReactorRequest w.requestId)
    )

-- | Build a display `PendingRequest` from the reactor-decoded request
-- | datum, preserving both sides of an update (the lossiness #342 called
-- | out). Never fabricates a value: absent reactor fields stay `Nothing`.
decodeReactorRequest :: RequestId -> Json -> Either JsonDecodeError PendingRequest
decodeReactorRequest reqId j = do
  o <- decodeJson j
  token <- o .: "token"
  owner <- o .: "owner"
  key <- o .: "key"
  operation <- o .: "operation"
  oldVal <- o .:? "old"
  newVal <- o .:? "new"
  fee <- o .: "fee"
  submittedAt <- o .: "submitted_at"
  let
    primaryValue = if operation == "delete" then oldVal else newVal
    updateOldValue = if operation == "update" then oldVal else Nothing
  pure
    { token: TokenId token
    , owner
    , key: Key key
    , value: Value <$> primaryValue
    , oldValue: Value <$> updateOldValue
    , operation
    , fee
    , submittedAt
    , requestId: reqId
    }

decodeFactValue :: Json -> Either JsonDecodeError Value
decodeFactValue j = do
  top <- decodeJson j
  value <- top .: "value"
  pure (Value value)

decodeTrustedRoot :: Json -> Either JsonDecodeError TrustedRoot
decodeTrustedRoot j = do
  top <- decodeJson j
  mroot <- top .: "utxo_root"
  case mroot of
    Just root -> pure (TrustedRoot root)
    Nothing -> Left (TypeMismatch "status.utxo_root is not available yet")

decodeTxId :: Json -> Either JsonDecodeError String
decodeTxId j = do
  top <- decodeJson j
  top .: "txId"
