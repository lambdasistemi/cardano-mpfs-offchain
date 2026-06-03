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
    ) where

import Data.ByteString qualified as BS
import Data.ByteString.Base16 qualified as Base16
import Data.Text (Text)
import Data.Text.Encoding qualified as T

import Cardano.MPFS.API.Encoding (Hex (..))
import Cardano.MPFS.API.Types
    ( ChainPointJSON (..)
    , TokenResponse (..)
    , TxInJSON (..)
    , VerificationSnapshot (..)
    , WitnessedTokenState (..)
    , WitnessedUtxo (..)
    )
import Cardano.MPFS.Client.Bundle qualified as ClientWire
import Cardano.MPFS.Client.Snapshot qualified as ClientSnapshot
import Cardano.MPFS.Client.TrustedRoot
    ( TrustedRoot (..)
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
    checkLength (prefix <> ".trusted_root") trustedBs
    verifyVerificationSnapshot (toClientSnapshot snapshot)
    let snapshotPath = prefix <> ".snapshot.utxo_root"
        Hex snapshotBs = vsUtxoRoot snapshot
    if snapshotBs == trustedBs
        then Right ()
        else Left (TrustedRootMismatch snapshotPath)
    replayWitnessedUtxo
        (prefix <> ".state.utxo")
        trustedBs
        (toClientWitnessedUtxo (wtsUtxo state))
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
