# Quickstart: Exercise the atomic POST /tx/boot

This walkthrough drives the boot endpoint end-to-end and verifies the
atomic-read property. It assumes a Linux host with `nix` installed and
a clone of `cardano-mpfs-offchain` at `/code/cardano-mpfs-offchain-atomic-handlers`
(the worktree where this slice lives).

## 1. Enter the dev shell

```bash
cd /code/cardano-mpfs-offchain-atomic-handlers
direnv allow            # first time only
nix develop             # or rely on direnv to do this for you
```

`just` recipes drive every common task from the dev shell.

## 2. Build the server and tests

```bash
just build              # cabal build all -O0
just unit-offchain      # offchain unit tests
just e2e                # devnet-backed end-to-end suite (long)
```

Or, for the slice's full quality gate (matches CI exactly):

```bash
nix build \
    .#offchain-tests \
    .#e2e-tests \
    .#cardano-mpfs-offchain \
    .#docker-image \
    .#checks.x86_64-linux.swagger-up-to-date
just format-check
just hlint
```

All of the above MUST be green before pushing the slice.

## 3. Start a local devnet + MPFS server

The e2e harness does this automatically for tests, but for manual
exploration:

```bash
# In one terminal, start a devnet (this is the cardano-node subprocess
# the e2e suite already manages).
just devnet-up          # spawns cardano-node + waits until tip > origin

# In a second terminal, run the server against the devnet socket.
just serve              # mpfs-serve --socket /tmp/devnet/node.socket --db /tmp/mpfs-db ...
```

The server logs include the chain follower's progress. Wait until you
see `TraceChainTip` with a non-Origin slot.

## 4. Fund a wallet address on the devnet

The harness has a built-in faucet wallet. From a third terminal:

```bash
just faucet-fund <addr>     # transfers 100 ADA to <addr>
```

Wait until the chain follower's tip moves past the slot containing the
faucet transfer. The atomic reader will then see the new wallet UTxO
inside its snapshot.

## 5. Drive POST /tx/boot

```bash
curl -sS http://localhost:3000/tx/boot \
    -H 'Content-Type: application/json' \
    -d '{"address":"<addr-as-hex>"}' \
    | jq
```

Expected shape (abbreviated):

```json
{
  "transaction": "<unsigned tx CBOR hex>",
  "snapshot": {
    "utxo_root":  "<32-byte root hex>",
    "slot":       <slot>,
    "block_id":   "<block id hex>"
  },
  "proof": {
    "boot_funding": [
      {
        "ref":  { "tx_id": "...", "ix": 0 },
        "txout":      "<TxOut CBOR hex>",
        "csmt_proof": "<proof bytes hex>"
      }
    ]
  }
}
```

## 6. Verify the response offline

```bash
just verify-boot-response /tmp/boot-response.json \
    --trusted-root <utxo_root>
```

The verifier (`cardano-mpfs-client`) reads the response, uses the
response's own `utxo_root` as the trusted root for the demo, and
checks every proof against it. It MUST return exit 0 (`accepted`).

To exercise the negative path, tamper with the trusted root:

```bash
just verify-boot-response /tmp/boot-response.json \
    --trusted-root <utxo_root_with_one_byte_flipped>
```

Expected: exit 2, message
`snapshot mismatch: trusted_root != response_root`.

## 7. Sign and submit (sanity check the on-chain validator)

```bash
just sign-and-submit /tmp/boot-response.json <signing-key-path>
```

The script signs the response's `transaction`, submits via N2C, and
reports the resulting `TxId`. The chain follower will index the
resulting on-chain mutation in the next block.

## 8. Verify atomicity under chain churn

This is the SC-001 scenario.

```bash
just chain-churn-load &      # background: 1+ block per second of dummy traffic
sleep 60                     # let the chain churn

# Hit boot 50 times back-to-back; every response must verify cleanly.
for i in $(seq 1 50); do
    just boot-and-verify <addr>     # POST + verifier in one shot
done
```

All 50 invocations MUST exit 0. On the previous racy implementation
this rate was observably below 100% (the e2e suite reproduced
rejections under load); with the atomic reader in place, it is 100%.

## 9. Latency curve (SC-003)

```bash
just db-prepopulate /tmp/mpfs-db --total-utxos 1000 --addr-utxos 2
just serve &
just bench-boot --addr <addr>      # records median latency

just db-prepopulate /tmp/mpfs-db --total-utxos 1000000 --addr-utxos 2
just serve &
just bench-boot --addr <addr>      # records median latency

# Compare: ratio MUST be <= 2.
```

## 10. followerEnabled = False fixture path

The harness used by `CageFlowSpec` sets `followerEnabled = False` and
drives the indexer manually. After this slice it also installs the
`atomicCageReaderOverride`:

```haskell
let appCfg =
        AppConfig
            { …
            , followerEnabled = False
            , atomicCageReaderOverride =
                Just (mkWalletStubAtomicReader walletProv)
            }
```

The fixture stub uses the wallet-side `Provider.queryUTxOs` (allowed
on the wallet side; forbidden on the server side) and emits empty
proofs against an empty-root snapshot the in-test verifier accepts.
Run the fixtures with:

```bash
just unit-offchain --match "CageFlow"
```

If the override is left out, the fixture fails fast with
`Indexer not ready: no chain checkpoint` — that is the exact error
shape FR-004 specifies, and it is the signal to the developer that
the override is missing.
