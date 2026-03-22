# WIP: Issue #153 - Migrate to chain-follower Backend/Runner API

## Status
In progress - Backend + CageFollower done, Application wiring blocked on CSMTOps type

## Plan
1. [x] Create worktree and PR
2. [x] Bump deps (utxo-csmt, chain-follower, kv-transactions, mts, node-clients)
3. [x] Fix Follower/Intersector type params
4. [x] Write composed Backend module (Backend.Init with cage + UTxO)
5. [x] Rewrite CageFollower using Runner.processBlock/rollbackTo
6. [ ] Rewrite Application wiring
   BLOCKED: composedInit takes CSMTOps(UTxOT) but Application has Ops KVOnly.
   Need: composedInit should take raw Ops and construct CSMTOps internally,
   or accept individual insert/delete fns. The toFollowing transition must
   upgrade KVOnly→Full ops (journal replay).
7. [ ] Update exe and tests
8. [ ] Clean up dead code (Rollback.hs etc)

## Key business logic (from old CageFollower + Follower.hs)
- extractConwayTxs (pure)
- detectCageBlockEvents (needs UTxO resolver via mapColumns InUtxo)
- applyCageBlockEvents (needs State + TrieManager, returns [CageInverseOp])
- applyCageInverses (for rollback, reverse chronological order)
- UTxO ops: changeToOp maps Change → Operation, then forwardTip or direct csmtInsert/csmtDelete

## Notes
- Follower.hs business logic is already monad-generic — reusable as-is
- mapColumns still exists (44c3c2a is newer than 0888387)
- Rollback.hs to be eliminated — Runner handles storage + pruning
- putCheckpointT replaced by queryHistory on composed rollback column
