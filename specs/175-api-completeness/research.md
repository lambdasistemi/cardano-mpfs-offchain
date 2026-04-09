# Research: API Completeness

## On-Chain Reject Semantics

The `Reject` redeemer (cage.ak:817-865) works as follows:

- **State UTxO**: spent with `Reject` redeemer. Validator checks root unchanged, time params immutable.
- **Request UTxOs**: consumed in the same tx. The `mkReject` fold (cage.ak:764-807) iterates all inputs, checks `is_rejectable` for matching requests.
- **is_rejectable** (cage.ak:494-507): Phase 3 expired (`submitted_at + process_time + retract_time` is before the validity range) OR dishonest `submitted_at` (in the future).
- **Refunds**: `(locked ADA - fee)` to each request owner. Oracle keeps fees.
- **Key difference from Modify**: no proofs, no trie changes, root must stay the same.

## Request UTxO Redeemers in Reject

Looking at the on-chain dispatch (cage.ak:250-277), `Reject` is a **State UTxO redeemer** (handled under `StateDatum`). The request UTxOs in a reject tx need their own spending redeemer. Looking at `validRequest` (cage.ak:417-438), the `Contribute(ref)` redeemer on request UTxOs checks:
- `requestToken == tokenId` (correct target)
- Phase 1 OR `is_rejectable` (line 429-436)

So in a reject tx, request UTxOs use `Contribute(stateRef)` as their redeemer, same as in update. The `is_rejectable` condition allows Contribute in Phase 3.

## Existing Patterns

### requestInsert/requestDelete (Request.hs)
Both follow identical structure: get fee UTxO, build datum with `OpInsert`/`OpDelete`, create output at cage address, balance. `requestUpdate` will use `OpUpdate oldVal newVal`.

### Reject tx structure
Closest to `updateTokenImpl` (Update.hs) but simpler:
- Find state UTxO + rejectable request UTxOs
- State redeemer: `Reject` (not `Modify`)
- Request redeemers: `Contribute stateRef` (same as update)
- New state output: same root (unchanged), same params
- Refund outputs: same pattern as update's refunds
- No proofs, no trie operations
- Validity interval: must be entirely after `submitted_at + process_time + retract_time`

## TS Implementation Reference

The TS implementation (mpfs/off_chain) has:
- `request-insert`, `request-delete`, `request-update` endpoints — all three
- No reject endpoint (same gap as Haskell)
- Request.ts line 80: `operation = mConStr2([change.oldValue, change.newValue])` for update
