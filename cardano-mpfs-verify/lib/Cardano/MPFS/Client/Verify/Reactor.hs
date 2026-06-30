-- |
-- Module      : Cardano.MPFS.Client.Verify.Reactor
-- Description : Target-agnostic verifier dispatch for cross-target builds.
--
-- A single pure entry point, 'runEnvelope', shared verbatim by the
-- native test harness and the WASM / GHC-JS reactor executables. Given a
-- JSON request envelope it decodes the named facts payload, runs the
-- matching @verify*Facts@ against the supplied trusted root, and renders
-- a deterministic one-line verdict.
--
-- The verdict is rendered with 'show' on 'VerifyError' (never aeson key
-- ordering), so the bytes 'runEnvelope' returns are identical on every
-- backend — the byte-identity property the cross-target QuickCheck suite
-- asserts (constitution IX, "One Verifier, Many Targets").
--
-- Request envelope:
--
-- > { "op": "boot" | "request_insert" | "request_delete"
-- >       | "request_update" | "update" | "retract" | "reject" | "end"
-- >       | "verify_tokens" | "verify_snapshot"
-- >       | "verify_fact_inclusion" | "verify_facts"
-- > , "trusted_root": "<hex>"
-- > , "facts": { ... }          -- the op's facts wire object
-- > , "cage_config": { ... }    -- required for end/tokens/snapshot
-- > , "token_id": "<hex>"       -- required for verify_snapshot
-- > , "key": "<hex>"            -- required for verify_fact_inclusion
-- > }
--
-- Verdict (one line, UTF-8): @verify_ok@, @verify_error: <VerifyError>@,
-- @bad_facts: <reason>@, @unknown_op: <op>@, or @bad_envelope: <reason>@.
module Cardano.MPFS.Client.Verify.Reactor
    ( runEnvelope
    ) where

import Control.Monad (void)
import Data.Aeson
    ( FromJSON
    , Result (..)
    , Value
    , eitherDecodeStrict
    , fromJSON
    , withObject
    , (.:)
    )
import Data.Aeson.Types (Parser, parseEither)
import Data.ByteString (ByteString)
import Data.ByteString.Base16 qualified as B16
import Data.ByteString.Short (ShortByteString, toShort)
import Data.Text (Text)
import Data.Text qualified as T
import Data.Text.Encoding qualified as T

import Cardano.Ledger.BaseTypes (Network (..))
import Cardano.Ledger.Coin (Coin (..))

-- The facts types are decoded via 'fromJSON' inside 'run'; we need only
-- their 'FromJSON' instances in scope, not the type names.
import Cardano.MPFS.API.Encoding (Hex)
import Cardano.MPFS.API.Types
    ( FactResponse
    )
import Cardano.MPFS.API.Types.Facts ()
import Cardano.MPFS.Client.Cage.Config
    ( CageConfig (..)
    , computeScriptHash
    )
import Cardano.MPFS.Client.Facts
    ( FactPresentFacts (..)
    , verifyFactPresentFacts
    )
import Cardano.MPFS.Client.TrustedRoot (TrustedRoot (..))
import Cardano.MPFS.Client.Verify
    ( VerifyError
    , verifyBootFacts
    , verifyEndFacts
    , verifyRejectFacts
    , verifyRequestDeleteFacts
    , verifyRequestInsertFacts
    , verifyRequestUpdateFacts
    , verifyRetractFacts
    , verifyUpdateFacts
    )
import Cardano.MPFS.Client.Verify.Read
    ( verifyTokenFacts
    , verifyTokenRequests
    , verifyTokens
    )

-- | Decode a request envelope and render the verifier verdict as bytes.
-- Total: every failure mode maps to a tagged line, never an exception.
runEnvelope :: ByteString -> ByteString
runEnvelope input =
    T.encodeUtf8 $ case eitherDecodeStrict input of
        Left err -> "bad_envelope: " <> T.pack err
        Right value -> case parseEither dispatch value of
            Left err -> "bad_envelope: " <> T.pack err
            Right verdict -> verdict

-- | Pull @op@ and @trusted_root@ from the envelope and route to the
-- verifier named by @op@. The facts object (and, for @end@, the cage
-- config) are decoded inside the matching branch.
dispatch :: Value -> Parser Text
dispatch = withObject "Envelope" $ \o -> do
    op <- o .: "op"
    tr <- TrustedRoot <$> o .: "trusted_root"
    facts <- o .: "facts"
    case op :: Text of
        "boot" -> pure (run (verifyBootFacts tr) facts)
        "request_insert" ->
            pure (run (verifyRequestInsertFacts tr) facts)
        "request_delete" ->
            pure (run (verifyRequestDeleteFacts tr) facts)
        "request_update" ->
            pure (run (verifyRequestUpdateFacts tr) facts)
        "update" -> pure (run (verifyUpdateFacts tr) facts)
        "retract" -> pure (run (verifyRetractFacts tr) facts)
        "reject" -> pure (run (verifyRejectFacts tr) facts)
        "end" -> do
            cfg <- o .: "cage_config" >>= parseCageConfig
            pure (run (verifyEndFacts cfg tr) facts)
        "verify_tokens" -> do
            cfg <- o .: "cage_config" >>= parseCageConfig
            pure (run (verifyTokens cfg tr) facts)
        "verify_snapshot" -> do
            cfg <- o .: "cage_config" >>= parseCageConfig
            token <- o .: "token_id"
            pure (run (verifyTokenRequests cfg token tr) facts)
        "verify_requests" -> do
            cfg <- o .: "cage_config" >>= parseCageConfig
            token <- o .: "token_id"
            pure (run (verifyTokenRequests cfg token tr) facts)
        "verify_fact_inclusion" -> do
            key <- o .: "key"
            pure (run (verifyFactInclusion tr key) facts)
        "verify_facts" -> pure (run (verifyTokenFacts tr) facts)
        _ -> pure ("unknown_op: " <> op)

-- | Decode the facts payload, run the verifier, render the verdict.
run
    :: (FromJSON a)
    => (a -> Either VerifyError b)
    -> Value
    -> Text
run verify facts = case fromJSON facts of
    Error err -> "bad_facts: " <> T.pack err
    Success x -> case verify x of
        Left e -> "verify_error: " <> T.pack (show e)
        Right _ -> "verify_ok"

verifyFactInclusion
    :: TrustedRoot -> Hex -> FactResponse -> Either VerifyError ()
verifyFactInclusion tr key response =
    void
        $ verifyFactPresentFacts
            tr
            FactPresentFacts
                { fpfKey = key
                , fpfResponse = response
                }

-- | Parse the blueprint-derived 'CageConfig' the @end@ verifier needs.
-- Script bytes arrive as hex; the script hash is recomputed locally so
-- the same bytes yield the same config on every target.
parseCageConfig :: Value -> Parser CageConfig
parseCageConfig = withObject "CageConfig" $ \o -> do
    cageBytes <- o .: "cage_script_bytes" >>= hexShort
    requestBytes <- o .: "request_script_bytes" >>= hexShort
    processTime <- o .: "default_process_time"
    retractTime <- o .: "default_retract_time"
    tip <- o .: "default_tip"
    net <- o .: "network" >>= parseNetwork
    pure
        CageConfig
            { cageScriptBytes = cageBytes
            , requestScriptBytes = requestBytes
            , cfgScriptHash = computeScriptHash cageBytes
            , defaultProcessTime = processTime
            , defaultRetractTime = retractTime
            , defaultTip = Coin tip
            , network = net
            }

-- | Decode a hex string to a strict 'ShortByteString'.
hexShort :: Text -> Parser ShortByteString
hexShort t = case B16.decode (T.encodeUtf8 t) of
    Left err -> fail ("invalid hex: " <> err)
    Right bs -> pure (toShort bs)

-- | Parse the target network from a lowercase tag.
parseNetwork :: Text -> Parser Network
parseNetwork "testnet" = pure Testnet
parseNetwork "mainnet" = pure Mainnet
parseNetwork other = fail ("unknown network: " <> T.unpack other)
