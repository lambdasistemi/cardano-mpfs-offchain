-- |
-- Module      : Cardano.MPFS.Client.Verify.Write
-- Description : Verifier for the uniform write-side response.
--
-- Mirrors the Lean 'Phase4.ProofRedesign.replayInputs' fold and
-- the 'forge_boot_input_breaks_validity' theorem: every input
-- carried by the response replays cryptographically against the
-- externally-supplied 'TrustedRoot', and the snapshot's
-- @utxo_root@ is byte-for-byte the trusted root.
--
-- Endpoints that bundle a per-cage requests completeness witness
-- (@POST \/tx\/oracle\/update@, @POST \/tx\/oracle\/end@) extend
-- this verifier with a 'verifyCompleteness' step in their own
-- slices.
--
-- Per-endpoint asset-name and redeemer-binding checks (the boot
-- mint, the request redeemer, the oracle update redeemer, …)
-- are intentionally out of scope here. Those bindings are
-- endpoint-specific and will be layered on by per-endpoint
-- wrappers as their slices land.
module Cardano.MPFS.Client.Verify.Write
    ( verifyUnsignedTxResponse
    ) where

import Data.ByteString qualified as BS
import Data.Text (Text)
import Data.Text qualified as T

import Cardano.MPFS.API.Encoding (Hex (..))
import Cardano.MPFS.API.Types
    ( UnsignedTxResponse (..)
    , VerificationSnapshot (..)
    )
import Cardano.MPFS.Client.TrustedRoot
    ( TrustedRoot (..)
    )
import Cardano.MPFS.Client.Verify.Replay
    ( VerifyError (..)
    , replayUtxoEntry
    )

-- | Verify an 'UnsignedTxResponse' against an externally-supplied
-- 'TrustedRoot'. Two checks:
--
--  1. @snapshot.utxo_root@ is exactly the trusted root bytes
--     ('TrustedRootMismatch' otherwise; 'WrongHexLength' if
--     either side is not 32 bytes).
--  2. Each input in @inputs@ replays cryptographically against
--     the trusted root via 'replayUtxoEntry'.
--
-- Returns @Right ()@ when every check passes; otherwise the
-- first failing check's 'VerifyError', tagged with a dotted
-- field path rooted at the supplied @endpoint@ label.
verifyUnsignedTxResponse
    :: Text
    -- ^ endpoint label used as the dotted-path prefix
    -- (e.g. @"boot"@).
    -> TrustedRoot
    -> UnsignedTxResponse
    -> Either VerifyError ()
verifyUnsignedTxResponse
    endpoint
    (TrustedRoot (Hex trustedBs))
    UnsignedTxResponse{..} = do
        checkLength
            (endpoint <> ".trusted_root")
            trustedBs
        let snapshotPath = endpoint <> ".snapshot.utxo_root"
            Hex snapshotBs = vsUtxoRoot utrSnapshot
        checkLength snapshotPath snapshotBs
        if snapshotBs == trustedBs
            then Right ()
            else Left (TrustedRootMismatch snapshotPath)
        replayInputs trustedBs utrInputs
      where
        checkLength field bs
            | BS.length bs == 32 = Right ()
            | otherwise =
                Left (WrongHexLength field 32 (BS.length bs))
        replayInputs root entries =
            mapM_
                ( \(ix, entry) ->
                    replayUtxoEntry
                        ( endpoint
                            <> ".inputs["
                            <> T.pack (show (ix :: Int))
                            <> "]"
                        )
                        root
                        entry
                )
                (zip [0 ..] entries)
