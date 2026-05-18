# Quickstart: Boot fact-provider pivot

## Preconditions

- Branch: `261-boot-fact-provider-pivot`
- Nix shell available.
- Offchain devnet can be started by existing e2e tests.
- Paired MOOG readiness is tracked through the boundary issue
  https://github.com/cardano-foundation/moog/issues/96 before this PR
  is marked ready.

## Baseline Gate

```bash
./gate.sh
```

Current baseline gate:

```bash
git diff --check
nix develop --quiet -c just ci
```

## Expected Boot Flow

1. Start the offchain devnet server through the e2e harness.
2. Fund a wallet address.
3. Request facts:

   ```http
   POST /facts/boot
   {"address":"<hex address>"}
   ```

4. Fetch or derive the trusted UTxO root independently.
5. Run `verifyBootFacts trustedRoot bootFacts`.
6. Run `bootCageTx cageConfig walletPolicy verifiedBootFacts`.
7. Sign the returned transaction locally.
8. Submit through the existing submit path.
9. Wait for the boot transaction to be indexed.
10. Confirm the indexed event is a boot event for the minted token.

## Focused Verification Commands

The exact test names may be adjusted during implementation, but the gate
must eventually include focused equivalents for:

```bash
nix develop --quiet -c just unit "verifyBootFacts"
nix develop --quiet -c just unit "bootCageTx"
nix develop --quiet -c just e2e "facts boot"
```

## Review Checklist

- `POST /facts/boot` works through the real HTTP server boundary.
- Legacy boot tx route is absent.
- No unsigned boot transaction is returned by the facts route.
- Verifier rejects tampered facts before build.
- `bootCageTx` rejects policy violations before signing.
- Swagger is regenerated.
- PR body names the paired MOOG boundary issue and remains draft until
  the canary-backed MOOG-v2 path is ready or an explicit replacement
  decision is recorded.
