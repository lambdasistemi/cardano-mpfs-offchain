import { expect, test } from "@playwright/test";

const walletAddressHex = "addr_test_wallet_hex";
const trustedRoot =
  "1111111111111111111111111111111111111111111111111111111111111111";

async function installWallet(page) {
  await page.addInitScript((address) => {
    window.__signArgs = [];
    window.cardano = {
      stubwallet: {
        name: "Stub Wallet",
        icon: "",
        enable: async () => ({
          getNetworkId: async () => 0,
          getUsedAddresses: async () => [address],
          getChangeAddress: async () => address,
          getBalance: async () => "1a001e8480",
          signTx: async (tx, partial) => {
            window.__signArgs.push({ tx, partial });
            return "bead";
          },
          submitTx: async () => "unused-wallet-submit",
        }),
      },
    };
  }, walletAddressHex);
}

async function installServerRoutes(page) {
  const seen = { bootBodies: [], submitBodies: [] };

  await page.route("**/status", async (route) => {
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({
        tip_slot: 10,
        tip_block_id: "aa",
        checkpoint_slot: 10,
        checkpoint_block_id: "bb",
        utxo_root: trustedRoot,
      }),
    });
  });

  await page.route("**/facts/boot", async (route) => {
    seen.bootBodies.push(JSON.parse(route.request().postData() || "{}"));
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({
        snapshot: { utxo_root: trustedRoot, chainpoint: "stub" },
        wallet_utxos: [{ ref: "tx#0", txout_cbor: "00", inclusion_proof: "proof" }],
        protocol_parameters: { cbor: "00", verified: false },
      }),
    });
  });

  await page.route("**/submit", async (route) => {
    seen.submitBodies.push(JSON.parse(route.request().postData() || "{}"));
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({ txId: "txid123" }),
    });
  });

  return seen;
}

async function installReactorStub(page) {
  await page.evaluate(() => {
    window.__reactorEnvelopes = [];
    globalThis.runCageReactor = async (stdin) => {
      const envelope = JSON.parse(stdin);
      window.__reactorEnvelopes.push(envelope);

      if (envelope.op === "boot") {
        return { stdout: "cage_tx: cafe01", stderr: "", exitOk: true };
      }

      if (envelope.op === "assemble") {
        return { stdout: "signed_tx: f00d", stderr: "", exitOk: true };
      }

      return { stdout: "unknown_op: " + envelope.op, stderr: "", exitOk: true };
    };
  });
}

test("register builds boot and assemble envelopes with a reactor stub", async ({ page }) => {
  await installWallet(page);
  const server = await installServerRoutes(page);

  await page.goto("/");
  await installReactorStub(page);

  await page.getByRole("button", { name: "Connect" }).click();
  await expect(page.getByRole("banner").getByText("Stub Wallet")).toBeVisible();

  await page.getByRole("tab", { name: "Tokens" }).click();
  await page.getByRole("button", { name: "Register new token" }).click();

  await expect(page.getByText("Submitted transaction")).toBeVisible();
  await expect(page.getByText("txid123")).toBeVisible();

  expect(server.bootBodies).toEqual([{ address: walletAddressHex }]);
  expect(server.submitBodies).toEqual([{ signedTxCbor: "f00d" }]);

  const signArgs = await page.evaluate(() => window.__signArgs);
  expect(signArgs).toEqual([{ tx: "cafe01", partial: true }]);

  const envelopes = await page.evaluate(() => window.__reactorEnvelopes);
  expect(envelopes).toHaveLength(2);
  expect(envelopes[0]).toMatchObject({
    op: "boot",
    trusted_root: trustedRoot,
    cage_config: {
      default_process_time: 300000,
      default_retract_time: 300000,
      default_tip: 2000000,
      network: "preprod",
    },
    wallet_policy: {
      max_fee: 10000000,
      max_min_utxo_coin_per_byte: 10000,
    },
  });
  expect(envelopes[0].cage_config.cage_script_bytes.length).toBeGreaterThan(1000);
  expect(envelopes[0].cage_config.request_script_bytes.length).toBeGreaterThan(1000);
  expect(envelopes[0].facts.snapshot.utxo_root).toBe(trustedRoot);
  expect(envelopes[1]).toEqual({
    op: "assemble",
    unsigned_tx: "cafe01",
    witness_set: "bead",
  });
});

test.skip("registers a token with the integrated wasm reactor on preprod", async () => {
  // NOTE: pending until mpfs-cage-reactor.wasm is supplied by the reactor worker
  // on the parent integration branch.
});
