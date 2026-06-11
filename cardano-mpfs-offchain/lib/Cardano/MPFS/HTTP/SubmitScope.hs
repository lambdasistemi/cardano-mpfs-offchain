-- |
-- Module      : Cardano.MPFS.HTTP.SubmitScope
-- Description : Structural scope gate for POST /tx/submit
-- License     : Apache-2.0
--
-- The MPFS server is a fact-provider for the MPFS cage
-- contract, not a general-purpose transaction relay.
-- 'txTouchesMpfs' is the cheap, mostly-structural gate
-- that @POST \/tx\/submit@ runs on a decoded
-- transaction before relaying it to the node: it admits
-- only transactions that interact with the cage contract
-- surface and rejects everything else — most plainly,
-- value transfers that have nothing to do with MPFS.
--
-- A transaction touches the cage surface when any of the
-- following hold:
--
--   * it mints or burns under the cage state-token
--     policy (boot, end);
--   * it has an output locked by the cage state script
--     (boot, update, reject) or a request output at this
--     cage's per-token request validator address (request
--     create);
--   * it /spends/ a cage-owned UTxO — a state UTxO at the
--     cage state address or a request UTxO at a request
--     validator address. This last clause is what admits
--     spend-only operations (retract, sweep) that produce
--     no cage mint and no cage output.
--
-- 'txTouchesMpfs' stays pure: the caller resolves the
-- transaction's spent inputs against the server's indexed
-- UTxO view (see 'spentTxIns' and
-- 'Cardano.MPFS.Indexer.Reads.readSpentTxOuts') and passes
-- the resolved 'SpentInput's in. An input the indexer
-- cannot resolve is conservatively treated as touching the
-- cage, so a transaction is never false-rejected because
-- the server's view lags the chain.
--
-- The mint and state-output recognition is reused from
-- "Cardano.MPFS.Indexer.Event" so the gate cannot drift
-- from how the indexer classifies the same transaction;
-- request outputs are additionally bound to this cage's
-- per-token request validator address.
module Cardano.MPFS.HTTP.SubmitScope
    ( txTouchesMpfs
    , SpentInput (..)
    , spentTxIns
    ) where

import Data.Foldable (toList)
import Lens.Micro ((^.))

import Cardano.Ledger.Address (Addr (..))
import Cardano.Ledger.Api.Tx (bodyTxL)
import Cardano.Ledger.Api.Tx.Body
    ( inputsTxBodyL
    , outputsTxBodyL
    )
import Cardano.Ledger.Api.Tx.Out (addrTxOutL)
import Cardano.Ledger.TxIn (TxIn)
import Cardano.Tx.Ledger (ConwayTx)

import Cardano.MPFS.Core.Types (ConwayEra, TxOut)
import Cardano.MPFS.Indexer.Event
    ( isCageStateOutput
    , mintsCagePolicy
    , requestOutputToken
    )
import Cardano.MPFS.TxBuilder.Config
    ( CageConfig (cfgScriptHash, network)
    )
import Cardano.MPFS.TxBuilder.Real.Internal
    ( requestAddrFromCfg
    )

-- | A transaction's spent input, resolved against the
-- server's indexed UTxO view, as consumed by
-- 'txTouchesMpfs'.
data SpentInput
    = -- | The indexer resolved this spent input to a known
      -- UTxO; the gate inspects the decoded 'TxOut' to see
      -- whether it sits on the cage surface.
      ResolvedSpent !(TxOut ConwayEra)
    | -- | The indexer could not resolve this spent input —
      -- its UTxO view may lag the chain. The gate treats
      -- such an input conservatively (as if it touched the
      -- cage) and so never hard-rejects a transaction on an
      -- unknown input alone; a stale or unknown input is
      -- the node's concern, not the gate's.
      UnresolvedSpent
    deriving (Eq, Show)

-- | The transaction inputs the @POST \/tx\/submit@ scope
-- gate must resolve against the indexed UTxO view: the
-- body's spent inputs. Reference inputs are excluded — a
-- referenced cage UTxO (e.g. retract\/sweep reference the
-- state UTxO) is not what the operation acts on.
spentTxIns :: ConwayTx -> [TxIn]
spentTxIns tx =
    toList (tx ^. bodyTxL . inputsTxBodyL)

-- | Does this transaction touch the MPFS cage contract
-- surface, given the server's cage configuration and the
-- server's view of its spent inputs?
--
-- This is the @POST \/tx\/submit@ scope gate. It admits a
-- transaction iff any of the following hold:
--
--   * a mint or burn under the cage policy (boot, end);
--   * an output locked by the cage state script (boot,
--     update, reject);
--   * a request output: a 'RequestDatum'-bearing output
--     sitting at THIS cage's request validator address for
--     the token the datum names (request create);
--   * a spent input resolved to a cage-owned UTxO — a
--     state UTxO at the cage state address (end, update,
--     reject) or a request UTxO at this cage's request
--     validator address (retract, sweep), the latter being
--     the only fingerprint a spend-only operation leaves.
--
-- An 'UnresolvedSpent' input is treated as touching the
-- cage, so the gate is conservative: it rejects only a
-- transaction that is definitely non-MPFS — no cage mint,
-- no cage output, and every spent input resolved to a
-- non-cage UTxO. A plain value transfer is rejected; any
-- real MPFS operation is admitted.
txTouchesMpfs :: CageConfig -> [SpentInput] -> ConwayTx -> Bool
txTouchesMpfs cfg spentInputs tx =
    mintsCagePolicy scriptHash tx
        || any touchesCageSurface outputs
        || any spentTouchesCage spentInputs
  where
    scriptHash = cfgScriptHash cfg
    outputs =
        toList (tx ^. bodyTxL . outputsTxBodyL)
    touchesCageSurface txOut =
        isCageStateOutput scriptHash txOut
            || isCageRequestOutput cfg txOut
    spentTouchesCage UnresolvedSpent = True
    spentTouchesCage (ResolvedSpent txOut) =
        touchesCageSurface txOut

-- | 'True' iff the output is a request output bound to
-- THIS cage: it carries a 'RequestDatum' and sits at the
-- request validator address derived for the datum's token
-- via 'requestAddrFromCfg'. Matched by payment credential,
-- so an optional stake part is ignored. A crafted
-- 'RequestDatum' at any other script address is rejected.
isCageRequestOutput :: CageConfig -> TxOut ConwayEra -> Bool
isCageRequestOutput cfg txOut =
    case requestOutputToken txOut of
        Just tid ->
            paymentOf (txOut ^. addrTxOutL)
                == paymentOf
                    ( requestAddrFromCfg
                        cfg
                        tid
                        (network cfg)
                    )
        Nothing -> False
  where
    paymentOf (Addr _ pc _) = Just pc
    paymentOf (AddrBootstrap _) = Nothing
