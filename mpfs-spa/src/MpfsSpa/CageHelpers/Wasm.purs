-- | WASM-backed `CageHelpers` implementation.
-- |
-- | The SPA only marshals server facts into the reactor envelope and parses
-- | the reactor's stable stdout line. All transaction construction and witness
-- | assembly remains inside the Haskell WASM reactor.
module MpfsSpa.CageHelpers.Wasm
  ( ReactorResult
  , assembleTx
  , buildBootEnvelope
  , parseCageTxOutput
  , parseSignedTxOutput
  , runCageReactor
  , wasmCageHelpers
  ) where

import Prelude

import Control.Promise (Promise, toAffE)
import Data.Array (head)
import Data.Argonaut.Core (Json, fromNumber, fromObject, fromString, stringify)
import Data.Argonaut.Encode (encodeJson)
import Data.Either (Either(..))
import Data.Int (toNumber)
import Data.Maybe (Maybe(..), fromMaybe)
import Data.String.CodeUnits as CU
import Data.String.Common as String
import Data.String.Pattern (Pattern(..))
import Data.Tuple (Tuple(..))
import Effect (Effect)
import Effect.Aff (Aff)
import Effect.Class (liftEffect)
import Foreign.Object as Object

import MpfsSpa.CageHelpers (CageHelpers, CageResult)
import MpfsSpa.Config (serverConfig, walletPolicyJson)
import MpfsSpa.Http (Config, getTrustedRoot, postBootFacts)
import MpfsSpa.Types
  ( CageConfig
  , CageError(..)
  , Key(..)
  , RequestId(..)
  , TokenId(..)
  , TrustedRoot(..)
  , UnsignedTxCbor(..)
  , Value(..)
  , WalletAddr(..)
  )

type ReactorResult =
  { stdout :: String
  , stderr :: String
  , exitOk :: Boolean
  }

type RawResponse =
  { ok :: Boolean
  , status :: Int
  , json :: Json
  , text :: String
  }

foreign import runCageReactorImpl :: String -> Effect (Promise ReactorResult)

foreign import postJsonImpl :: String -> String -> Effect (Promise RawResponse)

-- | UTF-8 encode a user-typed string to a lowercase hex string. Trie
-- keys/values travel as `Hex` on the wire, so plain text entered in the
-- forms (e.g. "start"/"amaru") must be hex-encoded before POST.
foreign import encodeUtf8Hex :: String -> String

runCageReactor :: String -> Aff ReactorResult
runCageReactor = toAffE <<< runCageReactorImpl

wasmCageHelpers :: CageHelpers
wasmCageHelpers =
  { registerToken
  , insertFact
  , updateFact
  , deleteFact
  , retractRequest
  , rejectExpired
  , endCage
  , updateToken
  }

registerToken :: WalletAddr -> CageConfig -> CageResult
registerToken addr cfg = do
  httpCfg <- liftEffect serverConfig
  eroot <- getTrustedRoot httpCfg
  efacts <- postBootFacts httpCfg addr
  case eroot, efacts of
    Left err, _ -> pure (Left (CageError err))
    _, Left err -> pure (Left (CageError err))
    Right root, Right facts -> do
      result <- runCageReactor (buildBootEnvelope root cfg facts)
      pure (UnsignedTxCbor <$> parseCageTxOutput result)

insertFact :: WalletAddr -> CageConfig -> TokenId -> Key -> Value -> CageResult
insertFact addr cfg token key value =
  requestCageTx "request_insert" cfg
    (\httpCfg -> postInsertFacts httpCfg addr token key value)

updateFact :: WalletAddr -> CageConfig -> TokenId -> Key -> Value -> Value -> CageResult
updateFact addr cfg token key oldValue newValue =
  requestCageTx "request_update" cfg
    (\httpCfg -> postUpdateFacts httpCfg addr token key oldValue newValue)

deleteFact :: WalletAddr -> CageConfig -> TokenId -> Key -> Value -> CageResult
deleteFact addr cfg token key value =
  requestCageTx "request_delete" cfg
    (\httpCfg -> postDeleteFacts httpCfg addr token key value)

retractRequest :: WalletAddr -> CageConfig -> TokenId -> RequestId -> CageResult
retractRequest addr cfg _token requestId =
  requestCageTx "retract" cfg
    (\httpCfg -> postRetractFacts httpCfg addr requestId)

rejectExpired :: WalletAddr -> CageConfig -> TokenId -> Array RequestId -> CageResult
rejectExpired addr cfg token requestIds =
  requestCageTx "reject" cfg
    (\httpCfg -> postRejectFacts httpCfg addr token requestIds)

-- | Owner/oracle op: fold all pending requests into the trie root.
updateToken :: WalletAddr -> CageConfig -> TokenId -> Array RequestId -> CageResult
updateToken addr cfg token requestIds =
  requestCageTx "update" cfg
    (\httpCfg -> postUpdateRootFacts httpCfg addr token requestIds)

endCage :: WalletAddr -> CageConfig -> TokenId -> CageResult
endCage addr cfg token = do
  httpCfg <- liftEffect serverConfig
  eroot <- getTrustedRoot httpCfg
  efacts <- postEndFacts httpCfg addr token
  case eroot, efacts of
    Left err, _ -> pure (Left (CageError err))
    _, Left err -> pure (Left (CageError err))
    Right root, Right facts -> do
      result <- runCageReactor (buildEndEnvelope root cfg facts)
      pure (UnsignedTxCbor <$> parseCageTxOutput result)

assembleTx :: String -> String -> Aff (Either CageError String)
assembleTx unsignedTx witnessSet = do
  result <- runCageReactor (buildAssembleEnvelope unsignedTx witnessSet)
  pure (parseSignedTxOutput result)

buildBootEnvelope :: TrustedRoot -> CageConfig -> Json -> String
buildBootEnvelope root cfg facts =
  stringify (buildEnvelope "boot" root cfg facts)

buildEndEnvelope :: TrustedRoot -> CageConfig -> Json -> String
buildEndEnvelope root cfg facts =
  stringify (buildEnvelope "end" root cfg facts)

buildRequestEnvelope :: String -> TrustedRoot -> CageConfig -> Json -> String
buildRequestEnvelope op root cfg facts =
  stringify (buildEnvelope op root cfg facts)

buildAssembleEnvelope :: String -> String -> String
buildAssembleEnvelope unsignedTx witnessSet =
  stringify
    ( obj
        [ Tuple "op" (fromString "assemble")
        , Tuple "unsigned_tx" (fromString unsignedTx)
        , Tuple "witness_set" (fromString witnessSet)
        ]
    )

parseCageTxOutput :: ReactorResult -> Either CageError String
parseCageTxOutput = parsePrefixedOutput "cage_tx: "

parseSignedTxOutput :: ReactorResult -> Either CageError String
parseSignedTxOutput = parsePrefixedOutput "signed_tx: "

requestCageTx
  :: String
  -> CageConfig
  -> (Config -> Aff (Either String Json))
  -> CageResult
requestCageTx op cfg fetchFacts = do
  httpCfg <- liftEffect serverConfig
  eroot <- getTrustedRoot httpCfg
  efacts <- fetchFacts httpCfg
  case eroot, efacts of
    Left err, _ -> pure (Left (CageError err))
    _, Left err -> pure (Left (CageError err))
    Right root, Right facts -> do
      result <- runCageReactor (buildRequestEnvelope op root cfg facts)
      pure (UnsignedTxCbor <$> parseCageTxOutput result)

postInsertFacts :: Config -> WalletAddr -> TokenId -> Key -> Value -> Aff (Either String Json)
postInsertFacts
  cfg
  (WalletAddr address)
  (TokenId token)
  (Key key)
  (Value value) =
  postFacts cfg "/facts/request/insert"
    ( encodeJson
        { token
        , key: encodeUtf8Hex key
        , value: encodeUtf8Hex value
        , address
        }
    )

postUpdateFacts
  :: Config
  -> WalletAddr
  -> TokenId
  -> Key
  -> Value
  -> Value
  -> Aff (Either String Json)
postUpdateFacts
  cfg
  (WalletAddr address)
  (TokenId token)
  (Key key)
  (Value oldValue)
  (Value newValue) =
  postFacts cfg "/facts/request/update"
    ( encodeJson
        { token
        , key: encodeUtf8Hex key
        , old_value: encodeUtf8Hex oldValue
        , new_value: encodeUtf8Hex newValue
        , address
        }
    )

postDeleteFacts :: Config -> WalletAddr -> TokenId -> Key -> Value -> Aff (Either String Json)
postDeleteFacts
  cfg
  (WalletAddr address)
  (TokenId token)
  (Key key)
  (Value value) =
  postFacts cfg "/facts/request/delete"
    ( encodeJson
        { token
        , key: encodeUtf8Hex key
        , value: encodeUtf8Hex value
        , address
        }
    )

postRetractFacts :: Config -> WalletAddr -> RequestId -> Aff (Either String Json)
postRetractFacts cfg (WalletAddr address) (RequestId utxo) =
  postFacts cfg "/facts/retract" (encodeJson { utxo, address })

postRejectFacts
  :: Config -> WalletAddr -> TokenId -> Array RequestId -> Aff (Either String Json)
postRejectFacts cfg (WalletAddr address) (TokenId token) requestIds =
  postFacts cfg "/facts/reject"
    ( encodeJson
        { token
        , address
        , requests: map requestIdText requestIds
        }
    )

postEndFacts :: Config -> WalletAddr -> TokenId -> Aff (Either String Json)
postEndFacts cfg (WalletAddr address) (TokenId token) =
  postFacts cfg "/facts/end" (encodeJson { token, address })

postUpdateRootFacts
  :: Config -> WalletAddr -> TokenId -> Array RequestId -> Aff (Either String Json)
postUpdateRootFacts cfg (WalletAddr address) (TokenId token) requestIds =
  postFacts cfg "/facts/update"
    ( encodeJson
        { token
        , address
        , requests: map requestIdText requestIds
        }
    )

postFacts :: Config -> String -> Json -> Aff (Either String Json)
postFacts cfg path body = do
  res <-
    toAffE
      ( postJsonImpl
          (cfg.baseUrl <> path)
          (stringify body)
      )
  pure
    if res.ok then Right res.json
    else Left ("HTTP " <> show res.status <> ": " <> res.text)

buildEnvelope :: String -> TrustedRoot -> CageConfig -> Json -> Json
buildEnvelope op (TrustedRoot trustedRoot) cfg facts =
  obj
    [ Tuple "op" (fromString op)
    , Tuple "trusted_root" (fromString trustedRoot)
    , Tuple "cage_config" (cageConfigJson cfg)
    , Tuple "wallet_policy" walletPolicyJson
    , Tuple "facts" facts
    ]

cageConfigJson :: CageConfig -> Json
cageConfigJson cfg =
  obj
    [ Tuple "cage_script_bytes" (fromString cfg.cageScriptBytes)
    , Tuple "request_script_bytes" (fromString cfg.requestScriptBytes)
    , Tuple "default_process_time" (intJson cfg.defaultProcessTime)
    , Tuple "default_retract_time" (intJson cfg.defaultRetractTime)
    , Tuple "default_tip" (intJson cfg.defaultTip)
    , Tuple "network" (fromString cfg.network)
    ]

parsePrefixedOutput :: String -> ReactorResult -> Either CageError String
parsePrefixedOutput prefix result
  | not result.exitOk =
      Left (CageError (firstMessage result))
  | otherwise =
      case CU.stripPrefix (Pattern prefix) (firstLine result.stdout) of
        Just hex | String.trim hex /= "" -> Right (String.trim hex)
        _ -> Left (CageError (firstMessage result))

firstMessage :: ReactorResult -> String
firstMessage result =
  let
    err = String.trim result.stderr
    out = String.trim result.stdout
  in
    if err /= "" then err
    else if out /= "" then firstLine out
    else "reactor returned no output"

firstLine :: String -> String
firstLine text =
  String.trim
    (fromMaybe text (head (String.split (Pattern "\n") text)))

intJson :: Int -> Json
intJson = fromNumber <<< toNumber

obj :: Array (Tuple String Json) -> Json
obj = fromObject <<< Object.fromFoldable

requestIdText :: RequestId -> String
requestIdText (RequestId requestId) = requestId
