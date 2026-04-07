# Implementation Plan: Fix PastHorizon error

**Branch**: `fix/posix-horizon` | **Date**: 2026-04-07 | **Spec**: `spec.md`

## Summary

Replace naive `posixMsToSlot`/`posixMsCeilSlot` arithmetic with
Provider-based conversion using the node's hard-fork interpreter.
Remove hardcoded era parameters from CageConfig.

## Implementation Steps

### 1. Bump cardano-node-clients pin

Update `cabal.project` source-repository-package to the commit from PR #33
(dcad2eb). Update sha256.

### 2. Add fields to mpfs Provider

**File**: `lib/Cardano/MPFS/Provider.hs`

Add:
```haskell
, posixMsToSlot :: Integer -> m SlotNo
, posixMsCeilSlot :: Integer -> m SlotNo
```

### 3. Wire in mkNodeClientProvider

**File**: `lib/Cardano/MPFS/Provider/NodeClient.hs`

Delegate to `Lib.posixMsToSlot` and `Lib.posixMsCeilSlot`.

### 4. Wire in mock Provider

**File**: `lib/Cardano/MPFS/Mock/Provider.hs`

Implement using the same naive arithmetic (mock has no era boundaries).

### 5. Update call sites

**Update.hs:268**: `posixMsToSlot cfg ms` → `posixMsToSlot prov ms`
**Retract.hs:174**: `posixMsCeilSlot cfg ms` → `posixMsCeilSlot prov ms`
**Retract.hs:180**: `posixMsToSlot cfg ms` → `posixMsToSlot prov ms`

These become monadic (need to bind).

### 6. Remove era params from CageConfig

**File**: `lib/Cardano/MPFS/TxBuilder/Config.hs`

Remove `systemStartPosixMs` and `slotLengthMs` fields.
Update all CageConfig construction sites.

### 7. Remove pure posixMsToSlot/posixMsCeilSlot from Internal.hs

Delete the functions and their exports.

## Source Code

```text
cabal.project                                         # pin bump
lib/Cardano/MPFS/Provider.hs                          # add fields
lib/Cardano/MPFS/Provider/NodeClient.hs               # wire upstream
lib/Cardano/MPFS/Mock/Provider.hs                     # mock impl
lib/Cardano/MPFS/TxBuilder/Config.hs                  # remove fields
lib/Cardano/MPFS/TxBuilder/Real/Internal.hs           # delete functions
lib/Cardano/MPFS/TxBuilder/Real/Update.hs             # use Provider
lib/Cardano/MPFS/TxBuilder/Real/Retract.hs            # use Provider
exe/RunPreprod.hs, exe/Serve.hs, exe/DevnetServer.hs  # update CageConfig
e2e-test/...                                          # update CageConfig
```
