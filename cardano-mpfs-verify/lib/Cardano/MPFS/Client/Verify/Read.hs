-- |
-- Module      : Cardano.MPFS.Client.Verify.Read
-- Description : Verifiers for the read-side responses.
--
-- Client-side verifiers for the read endpoints, mirroring the
-- opaque-witness pattern of "Cardano.MPFS.Client.Facts": each
-- verifier threads in an externally-supplied trusted UTxO-CSMT root,
-- performs only pure checks (no fetching), and returns an opaque
-- @Verified*@ value whose constructor is not exported, so public
-- callers cannot bypass verification.
module Cardano.MPFS.Client.Verify.Read
    ( VerifiedTokenState
    , verifiedTokenState
    , verifyTokenState
    , VerifiedTokenFacts
    , verifiedTokenFacts
    , verifyTokenFacts
    , VerifiedTokenRequests
    , verifiedTokenRequests
    , verifyTokenRequests
    ) where

import Data.ByteString qualified as BS
import Data.ByteString.Base16 qualified as Base16
import Data.Text (Text)
import Data.Text.Encoding qualified as T

import MPF.Hashes
    ( aikenKeyPath
    , mkMPFHash
    , mpfHashing
    , nullHash
    , renderMPFHash
    )
import MPF.Insertion (buildComposeFromList, scanMPFCompose)
import MPF.Interface (hexValue)

import Cardano.MPFS.API.Encoding (Hex (..))
import Cardano.MPFS.API.Types
    ( ChainPointJSON (..)
    , FactEntry (..)
    , FactsResponse (..)
    , RequestsResponse (..)
    , TokenIdJSON
    , TokenResponse (..)
    , TokenStateJSON (..)
    , TxInJSON (..)
    , VerificationSnapshot (..)
    , WitnessedTokenState (..)
    , WitnessedUtxo (..)
    )
import Cardano.MPFS.Client.Bundle qualified as ClientWire
import Cardano.MPFS.Client.Cage.Config
    ( CageConfig
    )
import Cardano.MPFS.Client.Cage.Identity
    ( requestSetPrefixFromCfg
    )
import Cardano.MPFS.Client.Snapshot qualified as ClientSnapshot
import Cardano.MPFS.Client.TrustedRoot
    ( TrustedRoot (..)
    )
import Cardano.MPFS.Client.Verify.Completeness
    ( verifyUtxoSetCompleteness
    )
import Cardano.MPFS.Client.Verify.Replay
    ( VerifyError (..)
    , replayWitnessedUtxo
    )
import Cardano.MPFS.Client.Verify.Snapshot
    ( verifyVerificationSnapshot
    )

-- | Opaque witness that a token response has been checked against the
-- caller-supplied trusted root: the embedded 'WitnessedTokenState' is
-- anchored to that root via the state UTxO inclusion proof. The
-- constructor is intentionally not exported, so public callers cannot
-- bypass 'verifyTokenState'.
newtype VerifiedTokenState = VerifiedTokenState TokenResponse
    deriving stock (Eq, Show)

-- | Extract the verified response after 'verifyTokenState' has
-- established the trusted-root and state-UTxO anchoring checks.
verifiedTokenState :: VerifiedTokenState -> TokenResponse
verifiedTokenState (VerifiedTokenState resp) = resp

-- | Verify a @GET \/tokens\/:id@ 'TokenResponse' against an
-- externally-supplied trusted UTxO-CSMT root. Confirms the snapshot is
-- structurally valid, its @utxo_root@ equals the trusted root, and the
-- state UTxO's inclusion proof replays against that root — i.e. the
-- 'WitnessedTokenState' is anchored to the trusted root.
verifyTokenState
    :: TrustedRoot
    -> TokenResponse
    -> Either VerifyError VerifiedTokenState
verifyTokenState (TrustedRoot (Hex trustedBs)) resp@TokenResponse{..} = do
    verifyAnchoredState "token" trustedBs trSnapshot trState
    Right (VerifiedTokenState resp)

-- | Opaque witness that a facts response has been checked against the
-- caller-supplied trusted root: the 'WitnessedTokenState' is anchored
-- to that root, and the MPF root reconstructed from the enumerated
-- fact set equals the on-chain trie root in that state. The
-- constructor is not exported, so public callers cannot bypass
-- 'verifyTokenFacts'.
newtype VerifiedTokenFacts = VerifiedTokenFacts FactsResponse
    deriving stock (Eq, Show)

-- | Extract the verified response after 'verifyTokenFacts' has
-- established the anchoring and completeness checks.
verifiedTokenFacts :: VerifiedTokenFacts -> FactsResponse
verifiedTokenFacts (VerifiedTokenFacts resp) = resp

-- | Verify a @GET \/tokens\/:id\/facts@ 'FactsResponse' against an
-- externally-supplied trusted UTxO-CSMT root. First anchors the
-- 'WitnessedTokenState' to the trusted root (as 'verifyTokenState'
-- does), then reconstructs the MPF trie root from the complete
-- enumerated @[FactEntry]@ and asserts it equals the on-chain trie
-- root advertised in the (now-anchored) state. The root binds the
-- whole map, so any omitted, added, or tampered fact makes the
-- reconstructed root diverge — this is the completeness proof.
verifyTokenFacts
    :: TrustedRoot
    -> FactsResponse
    -> Either VerifyError VerifiedTokenFacts
verifyTokenFacts (TrustedRoot (Hex trustedBs)) resp@FactsResponse{..} = do
    verifyAnchoredState "facts" trustedBs frsSnapshot frsState
    let Hex onChainRoot = root (wtsState frsState)
        reconstructed = reconstructMpfRoot (map factPair frsFacts)
    if reconstructed == onChainRoot
        then Right (VerifiedTokenFacts resp)
        else Left (MpfReplayFailed "facts.facts" "root mismatch")
  where
    factPair FactEntry{feKey = Hex keyBs, feValue = Hex valueBs} =
        (keyBs, valueBs)

-- | Opaque witness that a requests response has been checked against
-- the caller-supplied trusted root: the snapshot is pinned to that
-- root, and the request-address UTxO set witness proves the complete
-- set for the requested token's locally-derived request prefix.
newtype VerifiedTokenRequests = VerifiedTokenRequests RequestsResponse
    deriving stock (Eq, Show)

-- | Extract the verified response after 'verifyTokenRequests' has
-- established the trusted-root and request-set completeness checks.
verifiedTokenRequests :: VerifiedTokenRequests -> RequestsResponse
verifiedTokenRequests (VerifiedTokenRequests resp) = resp

-- | Verify a @GET \/tokens\/:id\/requests@ 'RequestsResponse'
-- against an externally-supplied trusted UTxO-CSMT root and the token
-- id from the request path. The response body does not repeat the
-- path token id, so callers must supply it explicitly for local
-- request-address prefix derivation.
verifyTokenRequests
    :: CageConfig
    -> TokenIdJSON
    -> TrustedRoot
    -> RequestsResponse
    -> Either VerifyError VerifiedTokenRequests
verifyTokenRequests
    cfg
    token
    (TrustedRoot (Hex trustedBs))
    resp@RequestsResponse{..} = do
        verifyTrustedSnapshot "requests" trustedBs rrSnapshot
        verifyUtxoSetCompleteness
            "requests.request_set"
            trustedBs
            (requestSetPrefixFromCfg cfg token)
            rrRequestSet
        Right (VerifiedTokenRequests resp)

-- | Reconstruct the Aiken-compatible MPF trie root from a complete
-- set of @(key, value)@ byte pairs, purely. Each key becomes its
-- Aiken nibble path and each value its MPF value hash, then the trie
-- is folded to a root via the same primitives the write path uses
-- (@buildComposeFromList@ / @scanMPFCompose@). @scanMPFCompose@
-- returns the root indirect with the node hash already in its
-- @hexValue@ — the same value @MPF.MTS@'s @mtsRootHash@ reports for
-- the stored root node — so the trie root is exactly that. An empty
-- fact set yields the null (empty-trie) root.
reconstructMpfRoot
    :: [(BS.ByteString, BS.ByteString)] -> BS.ByteString
reconstructMpfRoot kvs =
    case buildComposeFromList items of
        Nothing -> renderMPFHash nullHash
        Just compose ->
            let (rootInd, _) = scanMPFCompose [] mpfHashing compose
            in  renderMPFHash (hexValue rootInd)
  where
    items = [(aikenKeyPath k, mkMPFHash v) | (k, v) <- kvs]

-- | Shared anchoring check for read-side responses that carry a
-- snapshot plus a 'WitnessedTokenState': the trusted root has the
-- right length, the snapshot is structurally valid and pins the
-- trusted @utxo_root@, and the state UTxO inclusion proof replays
-- against that root.
verifyAnchoredState
    :: Text
    -> BS.ByteString
    -> VerificationSnapshot
    -> WitnessedTokenState
    -> Either VerifyError ()
verifyAnchoredState prefix trustedBs snapshot state = do
    verifyTrustedSnapshot prefix trustedBs snapshot
    replayWitnessedUtxo
        (prefix <> ".state.utxo")
        trustedBs
        (toClientWitnessedUtxo (wtsUtxo state))

verifyTrustedSnapshot
    :: Text
    -> BS.ByteString
    -> VerificationSnapshot
    -> Either VerifyError ()
verifyTrustedSnapshot prefix trustedBs snapshot = do
    checkLength (prefix <> ".trusted_root") trustedBs
    verifyVerificationSnapshot (toClientSnapshot snapshot)
    let snapshotPath = prefix <> ".snapshot.utxo_root"
        Hex snapshotBs = vsUtxoRoot snapshot
    if snapshotBs == trustedBs
        then Right ()
        else Left (TrustedRootMismatch snapshotPath)
  where
    checkLength field bs
        | BS.length bs == 32 = Right ()
        | otherwise = Left (WrongHexLength field 32 (BS.length bs))

toClientSnapshot
    :: VerificationSnapshot -> ClientSnapshot.VerificationSnapshot
toClientSnapshot VerificationSnapshot{..} =
    ClientSnapshot.VerificationSnapshot
        { ClientSnapshot.utxoRoot = toClientHex vsUtxoRoot
        , ClientSnapshot.chainpoint =
            let ChainPointJSON{..} = vsChainPoint
            in  ClientSnapshot.ChainPoint
                    { ClientSnapshot.slot = cpSlot
                    , ClientSnapshot.blockId = toClientHex cpBlockId
                    }
        }

toClientWitnessedUtxo :: WitnessedUtxo -> ClientWire.WitnessedUtxo
toClientWitnessedUtxo WitnessedUtxo{..} =
    ClientWire.WitnessedUtxo
        { ClientWire.txIn = toClientTxIn wuTxIn
        , ClientWire.txOut = toClientHex wuTxOut
        , ClientWire.utxoProof = toClientHex wuProof
        }

toClientTxIn :: TxInJSON -> ClientWire.TxIn
toClientTxIn TxInJSON{..} =
    ClientWire.TxIn
        { ClientWire.txId = toClientHex tjTxId
        , ClientWire.txIx = tjTxIx
        }

toClientHex :: Hex -> ClientSnapshot.Hex
toClientHex (Hex bs) =
    ClientSnapshot.Hex (T.decodeUtf8 (Base16.encode bs))
