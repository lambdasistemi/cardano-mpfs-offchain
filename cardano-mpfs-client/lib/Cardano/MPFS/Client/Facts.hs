-- |
-- Module      : Cardano.MPFS.Client.Facts
-- Description : Client surface for facts-only proof responses.
--
-- Reuses the shared API wire DTOs and adds the opaque
-- post-verification witness returned by facts-only verifiers.
module Cardano.MPFS.Client.Facts
    ( BootFacts (..)
    , RequestInsertFacts (..)
    , RequestDeleteFacts (..)
    , EndFacts (..)
    , UnverifiedPParams (..)
    , VerifiedBootFacts
    , VerifiedRequestInsertFacts
    , VerifiedRequestDeleteFacts
    , VerifiedEndFacts
    , verifiedBootFacts
    , verifiedRequestInsertFacts
    , verifiedRequestDeleteFacts
    , verifiedEndFacts
    , verifyBootFacts
    , verifyRequestInsertFacts
    , verifyRequestDeleteFacts
    , verifyEndFacts
    ) where

import Data.ByteString qualified as BS
import Data.Text qualified as T

import Cardano.MPFS.API.Encoding (Hex (..))
import Cardano.MPFS.API.Types.Common
    ( UnverifiedPParams (..)
    , UtxoEntryRefOnly (..)
    , UtxoSetWitness (..)
    , VerificationSnapshot (..)
    )
import Cardano.MPFS.API.Types.Facts
    ( BootFacts (..)
    , EndFacts (..)
    , RequestDeleteFacts (..)
    , RequestInsertFacts (..)
    )
import Cardano.MPFS.Client.Cage.Config
    ( CageConfig
    )
import Cardano.MPFS.Client.Cage.Identity
    ( requestSetPrefixFromCfg
    )
import Cardano.MPFS.Client.TrustedRoot
    ( TrustedRoot (..)
    )
import Cardano.MPFS.Client.Verify.Completeness
    ( verifyUtxoSetCompleteness
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

-- | Opaque witness that request-insert facts have been checked
-- against the caller-supplied trusted root.
newtype VerifiedRequestInsertFacts
    = VerifiedRequestInsertFacts RequestInsertFacts
    deriving stock (Eq, Show)

-- | Opaque witness that request-delete facts have been checked
-- against the caller-supplied trusted root.
newtype VerifiedRequestDeleteFacts
    = VerifiedRequestDeleteFacts RequestDeleteFacts
    deriving stock (Eq, Show)

-- | Opaque witness that end facts have been checked against the
-- caller-supplied trusted root and locally-derived request prefix.
newtype VerifiedEndFacts = VerifiedEndFacts EndFacts
    deriving stock (Eq, Show)

-- | Extract the verified facts after 'verifyBootFacts' has
-- established the trusted-root and CSMT proof checks.
verifiedBootFacts :: VerifiedBootFacts -> BootFacts
verifiedBootFacts (VerifiedBootFacts facts) = facts

-- | Extract the verified facts after 'verifyRequestInsertFacts'
-- has established the trusted-root and CSMT proof checks.
verifiedRequestInsertFacts
    :: VerifiedRequestInsertFacts -> RequestInsertFacts
verifiedRequestInsertFacts (VerifiedRequestInsertFacts facts) =
    facts

-- | Extract the verified facts after 'verifyRequestDeleteFacts'
-- has established the trusted-root and CSMT proof checks.
verifiedRequestDeleteFacts
    :: VerifiedRequestDeleteFacts -> RequestDeleteFacts
verifiedRequestDeleteFacts (VerifiedRequestDeleteFacts facts) =
    facts

-- | Extract the verified facts after 'verifyEndFacts' has
-- established the trusted-root, inclusion proof, and
-- request-set completeness checks.
verifiedEndFacts :: VerifiedEndFacts -> EndFacts
verifiedEndFacts (VerifiedEndFacts facts) = facts

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

-- | Verify a facts-only request-insert response against an
-- externally-supplied trusted UTxO-CSMT root.
verifyRequestInsertFacts
    :: TrustedRoot
    -> RequestInsertFacts
    -> Either VerifyError VerifiedRequestInsertFacts
verifyRequestInsertFacts
    (TrustedRoot (Hex trustedBs))
    facts@RequestInsertFacts{..} = do
        checkLength "request_insert.trusted_root" trustedBs
        let snapshotPath = "request_insert.snapshot.utxo_root"
            Hex snapshotBs = vsUtxoRoot rifSnapshot
        checkLength snapshotPath snapshotBs
        if snapshotBs == trustedBs
            then Right ()
            else Left (TrustedRootMismatch snapshotPath)
        replayWalletUtxos trustedBs rifWalletUtxos
        Right (VerifiedRequestInsertFacts facts)
      where
        checkLength field bs
            | BS.length bs == 32 = Right ()
            | otherwise =
                Left (WrongHexLength field 32 (BS.length bs))
        replayWalletUtxos root entries =
            mapM_
                ( \(ix, entry) ->
                    replayUtxoEntry
                        ( "request_insert.wallet_utxos["
                            <> T.pack (show (ix :: Int))
                            <> "]"
                        )
                        root
                        entry
                )
                (zip [0 ..] entries)

-- | Verify a facts-only request-delete response against an
-- externally-supplied trusted UTxO-CSMT root.
verifyRequestDeleteFacts
    :: TrustedRoot
    -> RequestDeleteFacts
    -> Either VerifyError VerifiedRequestDeleteFacts
verifyRequestDeleteFacts
    (TrustedRoot (Hex trustedBs))
    facts@RequestDeleteFacts{..} = do
        checkLength "request_delete.trusted_root" trustedBs
        let snapshotPath = "request_delete.snapshot.utxo_root"
            Hex snapshotBs = vsUtxoRoot rdfSnapshot
        checkLength snapshotPath snapshotBs
        if snapshotBs == trustedBs
            then Right ()
            else Left (TrustedRootMismatch snapshotPath)
        replayWalletUtxos trustedBs rdfWalletUtxos
        Right (VerifiedRequestDeleteFacts facts)
      where
        checkLength field bs
            | BS.length bs == 32 = Right ()
            | otherwise =
                Left (WrongHexLength field 32 (BS.length bs))
        replayWalletUtxos root entries =
            mapM_
                ( \(ix, entry) ->
                    replayUtxoEntry
                        ( "request_delete.wallet_utxos["
                            <> T.pack (show (ix :: Int))
                            <> "]"
                        )
                        root
                        entry
                )
                (zip [0 ..] entries)

-- | Verify a facts-only end response against an externally-supplied
-- trusted UTxO-CSMT root and a client-owned cage configuration.
verifyEndFacts
    :: CageConfig
    -> TrustedRoot
    -> EndFacts
    -> Either VerifyError VerifiedEndFacts
verifyEndFacts
    cfg
    (TrustedRoot (Hex trustedBs))
    facts@EndFacts{..} = do
        checkLength "end.trusted_root" trustedBs
        let snapshotPath = "end.snapshot.utxo_root"
            Hex snapshotBs = vsUtxoRoot efSnapshot
        checkLength snapshotPath snapshotBs
        if snapshotBs == trustedBs
            then Right ()
            else Left (TrustedRootMismatch snapshotPath)
        replayUtxoEntry "end.state_utxo" trustedBs efStateUtxo
        replayWalletUtxos trustedBs efWalletUtxos
        rejectRequestEntries (uswEntries efRequestSet)
        verifyUtxoSetCompleteness
            "end.request_set"
            trustedBs
            (requestSetPrefixFromCfg cfg efToken)
            efRequestSet
        Right (VerifiedEndFacts facts)
      where
        checkLength field bs
            | BS.length bs == 32 = Right ()
            | otherwise =
                Left (WrongHexLength field 32 (BS.length bs))
        replayWalletUtxos root entries =
            mapM_
                ( \(ix, entry) ->
                    replayUtxoEntry
                        ( "end.wallet_utxos["
                            <> T.pack (show (ix :: Int))
                            <> "]"
                        )
                        root
                        entry
                )
                (zip [0 ..] entries)
        rejectRequestEntries [] = Right ()
        rejectRequestEntries (entry : _) =
            Left
                ( CompletenessExtraLeaf
                    "end.request_set.entries[0]"
                    (uerRef entry)
                )
