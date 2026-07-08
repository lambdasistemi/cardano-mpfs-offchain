# Feature Specification: On-chain #76 Pin and State Policy Parameter

**Issue**: #388
**Branch**: `feat/388-onchain-pin-previouspolicies`
**PR**: #389

## User Story

As an MPFS wallet/verifier consumer, I need the offchain verifier and cage
transaction helpers to derive the same genesis cage state policy id as the
fixed on-chain #76 validators, so verification and transaction building remain
compatible after the `cardano-mpfs-onchain` pin is bumped.

## Functional Requirements

- FR1: `flake.nix` and `flake.lock` must pin `cardano-mpfs-onchain` to the
  GitHub-resolved `e37e33e` commit:
  `e37e33ed2ebad5b079d59eb1d4250f5f6d0c93e3`.
- FR2: The verifier must apply the genesis `previousPolicies = []` parameter to
  raw state validator bytes before deriving the state script hash or address.
- FR3: `CageConfig.cageScriptBytes` must carry the fully applied state script
  bytes, not the raw parameterized program, wherever the config is built from
  blueprint `state.` compiled code.
- FR4: Request validator parameterization remains unchanged, but must receive
  the corrected state policy id derived from the applied state script.
- FR5: Every discovered test config that extracts `state.` compiled code and
  computes a `CageConfig` must apply the same genesis parameter.
- FR6: The verifier reactor JSON config path must apply the same genesis
  parameter when parsing `cage_script_bytes`.

## Acceptance Criteria

- AC1: A RED test proves that, after the on-chain pin is bumped, hashing raw
  state bytes no longer matches the genesis state script expected by the bumped
  on-chain helper.
- AC2: The GREEN implementation makes the recomputed genesis state policy id
  and address match the applied `previousPolicies = []` state script.
- AC3: Cage boot, request, update, end, retract, reject, read-verifier, end
  facts, and reactor focused suites pass against the bumped blueprint.
- AC4: `./gate.sh` passes at HEAD before the implementation commit is accepted.
- AC5: The final PR branch passes the repo CI gate or equivalent local
  evidence recorded by the ticket owner.

## Non-goals

- Implementing offchain migration from predecessor policies.
- Changing request-validator parameter order or request datum/redeemer shape.
- Touching `cardano-mpfs-onchain`; the on-chain fix is already merged.

## Notes

The issue body contains the invalid full SHA `e37e33e2...`; GitHub resolves the
short `e37e33e` to `e37e33ed2ebad5b079d59eb1d4250f5f6d0c93e3`, which is the
commit this ticket will pin.
