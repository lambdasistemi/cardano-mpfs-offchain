-- |
-- Module      : Cardano.MPFS.Client.Facts
-- Description : Client surface for facts-only proof responses.
--
-- Reuses the shared API wire DTOs and adds the opaque
-- post-verification witness returned by facts-only verifiers.
module Cardano.MPFS.Client.Facts
    ( BootFacts (..)
    , UnverifiedPParams (..)
    , VerifiedBootFacts
    , verifiedBootFacts
    , verifyBootFacts
    ) where

import Data.ByteString qualified as BS
import Data.Text qualified as T

import Cardano.MPFS.API.Encoding (Hex (..))
import Cardano.MPFS.API.Types
    ( BootFacts (..)
    , UnverifiedPParams (..)
    , VerificationSnapshot (..)
    )
import Cardano.MPFS.Client.TrustedRoot
    ( TrustedRoot (..)
    )
import Cardano.MPFS.Client.Verify.Replay
    ( VerifyError (..)
    , replayUtxoEntry
    )

-- | Opaque witness that boot facts have been checked against the
-- caller-supplied trusted root. The constructor is intentionally
-- not exported, so public callers cannot bypass 'verifyBootFacts'.
newtype VerifiedBootFacts = VerifiedBootFacts BootFacts
    deriving stock (Eq, Show)

-- | Extract the verified facts after 'verifyBootFacts' has
-- established the trusted-root and CSMT proof checks.
verifiedBootFacts :: VerifiedBootFacts -> BootFacts
verifiedBootFacts (VerifiedBootFacts facts) = facts

-- | Verify a facts-only boot response against an externally-supplied
-- trusted UTxO-CSMT root.
verifyBootFacts
    :: TrustedRoot
    -> BootFacts
    -> Either VerifyError VerifiedBootFacts
verifyBootFacts
    (TrustedRoot (Hex trustedBs))
    facts@BootFacts{..} = do
        checkLength "boot.trusted_root" trustedBs
        let snapshotPath = "boot.snapshot.utxo_root"
            Hex snapshotBs = vsUtxoRoot bfSnapshot
        checkLength snapshotPath snapshotBs
        if snapshotBs == trustedBs
            then Right ()
            else Left (TrustedRootMismatch snapshotPath)
        replayWalletUtxos trustedBs bfWalletUtxos
        Right (VerifiedBootFacts facts)
      where
        checkLength field bs
            | BS.length bs == 32 = Right ()
            | otherwise =
                Left (WrongHexLength field 32 (BS.length bs))
        replayWalletUtxos root entries =
            mapM_
                ( \(ix, entry) ->
                    replayUtxoEntry
                        ( "boot.wallet_utxos["
                            <> T.pack (show (ix :: Int))
                            <> "]"
                        )
                        root
                        entry
                )
                (zip [0 ..] entries)
