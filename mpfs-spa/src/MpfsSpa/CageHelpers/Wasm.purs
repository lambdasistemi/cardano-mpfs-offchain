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
import MpfsSpa.Http (getTrustedRoot, postBootFacts)
import MpfsSpa.Types
  ( CageConfig
  , CageError(..)
  , TrustedRoot(..)
  , UnsignedTxCbor(..)
  , WalletAddr
  )

type ReactorResult =
  { stdout :: String
  , stderr :: String
  , exitOk :: Boolean
  }

foreign import runCageReactorImpl :: String -> Effect (Promise ReactorResult)

runCageReactor :: String -> Aff ReactorResult
runCageReactor = toAffE <<< runCageReactorImpl

wasmCageHelpers :: CageHelpers
wasmCageHelpers =
  { registerToken
  , insertFact: \_ _ _ _ _ -> notYet "S3"
  , updateFact: \_ _ _ _ _ _ -> notYet "S4"
  , deleteFact: \_ _ _ _ _ -> notYet "S5"
  , retractRequest: \_ _ _ _ -> notYet "S6"
  , rejectExpired: \_ _ _ -> notYet "S7"
  , endCage: \_ _ _ -> notYet "S2"
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

assembleTx :: String -> String -> Aff (Either CageError String)
assembleTx unsignedTx witnessSet = do
  result <- runCageReactor (buildAssembleEnvelope unsignedTx witnessSet)
  pure (parseSignedTxOutput result)

buildBootEnvelope :: TrustedRoot -> CageConfig -> Json -> String
buildBootEnvelope root cfg facts =
  stringify (buildEnvelope "boot" root cfg facts)

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

notYet :: String -> CageResult
notYet slice =
  pure (Left (CageError ("not yet implemented (slice " <> slice <> ")")))

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
