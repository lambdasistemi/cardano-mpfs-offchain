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
  , getTokens
  , getTokenState
  , getRequests
  , getFactValue
  , getTrustedRoot
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
import Data.Traversable (traverse)
import Effect (Effect)
import Effect.Aff (Aff)

import MpfsSpa.Types
  ( Key(..)
  , RequestId(..)
  , TokenId(..)
  , TrustedRoot(..)
  , Value(..)
  , WalletAddr(..)
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
-- | UTxO reference (`tx_id#tx_ix`) from the witness.
type PendingRequest =
  { token :: TokenId
  , owner :: String
  , key :: Key
  , value :: Maybe Value
  , operation :: String
  , fee :: Int
  , submittedAt :: Int
  , requestId :: RequestId
  }

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

-- | List all token ids the server knows.
getTokens :: Config -> Aff (Either String (Array TokenId))
getTokens cfg = getDecoded cfg "/tokens" decode
  where
  decode j = map TokenId <$> (decodeJson j :: Either JsonDecodeError (Array String))

-- | Fetch a token's on-chain state.
getTokenState :: Config -> TokenId -> Aff (Either String TokenState)
getTokenState cfg (TokenId tid) =
  getDecoded cfg ("/tokens/" <> tid) decodeTokenState

-- | Fetch a token's pending requests.
getRequests :: Config -> TokenId -> Aff (Either String (Array PendingRequest))
getRequests cfg (TokenId tid) =
  getDecoded cfg ("/tokens/" <> tid <> "/requests") decodeRequests

-- | Fetch the current value of a fact key (hex), if present.
getFactValue :: Config -> TokenId -> Key -> Aff (Either String Value)
getFactValue cfg (TokenId tid) (Key key) =
  getDecoded cfg ("/tokens/" <> tid <> "/facts/" <> key) decodeFactValue

-- | Fetch the trusted UTxO root from `/status`.
getTrustedRoot :: Config -> Aff (Either String TrustedRoot)
getTrustedRoot cfg = getDecoded cfg "/status" decodeTrustedRoot

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

decodeTokenState :: Json -> Either JsonDecodeError TokenState
decodeTokenState j = do
  top <- decodeJson j
  wts <- top .: "state"
  st <- wts .: "state"
  owner <- st .: "owner"
  root <- st .: "root"
  tip <- st .: "tip"
  processTime <- st .: "process_time"
  retractTime <- st .: "retract_time"
  pure { owner, root, tip, processTime, retractTime }

decodeRequests :: Json -> Either JsonDecodeError (Array PendingRequest)
decodeRequests j = do
  top <- decodeJson j
  (reqs :: Array Json) <- top .: "requests"
  traverse decodeWitnessedRequest reqs

decodeWitnessedRequest :: Json -> Either JsonDecodeError PendingRequest
decodeWitnessedRequest wj = do
  o <- decodeJson wj
  req <- o .: "request"
  utxo <- o .: "utxo"
  txin <- utxo .: "tx_in"
  txId <- txin .: "tx_id"
  (txIx :: Int) <- txin .: "tx_ix"
  token <- req .: "token"
  owner <- req .: "owner"
  key <- req .: "key"
  mvalue <- req .:? "value"
  operation <- req .: "operation"
  fee <- req .: "fee"
  submittedAt <- req .: "submitted_at"
  pure
    { token: TokenId token
    , owner
    , key: Key key
    , value: Value <$> mvalue
    , operation
    , fee
    , submittedAt
    , requestId: RequestId (txId <> "#" <> show txIx)
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
