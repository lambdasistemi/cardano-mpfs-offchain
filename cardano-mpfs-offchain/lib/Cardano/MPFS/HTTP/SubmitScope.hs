-- |
-- Module      : Cardano.MPFS.HTTP.SubmitScope
-- Description : Structural scope gate for POST /tx/submit
-- License     : Apache-2.0
--
-- The MPFS server is a fact-provider for the MPFS cage
-- contract, not a general-purpose transaction relay.
-- 'txTouchesMpfs' is the cheap, purely structural gate
-- that @POST \/tx\/submit@ runs on a decoded
-- transaction before relaying it to the node: it admits
-- only transactions that interact with the cage contract
-- surface (the state-token policy, the cage state
-- address, or a request output at this cage's request
-- validator) and rejects everything else — most plainly,
-- value transfers that have nothing to do with MPFS.
--
-- The mint and state-output recognition is reused from
-- "Cardano.MPFS.Indexer.Event" so the gate cannot drift
-- from how the indexer classifies the same transaction;
-- request outputs are additionally bound to this cage's
-- per-token request validator address.
module Cardano.MPFS.HTTP.SubmitScope
    ( txTouchesMpfs
    ) where

import Data.Foldable (toList)
import Lens.Micro ((^.))

import Cardano.Ledger.Address (Addr (..))
import Cardano.Ledger.Api.Tx (bodyTxL)
import Cardano.Ledger.Api.Tx.Body (outputsTxBodyL)
import Cardano.Ledger.Api.Tx.Out (addrTxOutL)
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

-- | Does this transaction touch the MPFS cage contract
-- surface, given the server's cage configuration?
--
-- This is the @POST \/tx\/submit@ scope gate. It admits a
-- transaction iff its body shows any of:
--
--   * a mint or burn under the cage policy (boot, end);
--   * an output locked by the cage state script (boot,
--     update, reject);
--   * a request output: a 'RequestDatum'-bearing output
--     sitting at THIS cage's request validator address for
--     the token the datum names (request create).
--
-- A plain value transfer touches none of these and is
-- rejected.
txTouchesMpfs :: CageConfig -> ConwayTx -> Bool
txTouchesMpfs cfg tx =
    mintsCagePolicy scriptHash tx
        || any (isCageStateOutput scriptHash) outputs
        || any (isCageRequestOutput cfg) outputs
  where
    scriptHash = cfgScriptHash cfg
    outputs =
        toList (tx ^. bodyTxL . outputsTxBodyL)

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
