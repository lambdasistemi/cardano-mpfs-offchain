import { expect, test } from "@playwright/test";

import {
  devnetGenesisAddressHex,
  signWitnessSet,
} from "../../scripts/devnet-cip30-signer.mjs";

test.setTimeout(180_000);

const devnetBaseUrl = process.env.MPFS_DEVNET_BASE_URL;
const walletBalance = "1b006a94d74f430000";

test("registers and ends a token through the real browser reactor on devnet", async ({
  page,
  request,
}) => {
  expect(devnetBaseUrl, "MPFS_DEVNET_BASE_URL is required").toBeTruthy();

  const submittedTxIds = [];
  const proxiedFacts = { boot: [], end: [] };

  await waitForTrustedRoot(request);
  await installDevnetProxy(page, submittedTxIds, proxiedFacts);
  await installDevnetWallet(page);

  await page.goto("/");
  await page.getByRole("button", { name: "Connect" }).click();
  await expect(page.getByRole("banner").getByText("Devnet Genesis")).toBeVisible();

  await page.getByRole("tab", { name: "Tokens" }).click();
  await page.getByRole("button", { name: "Register new token" }).click();
  await waitForSubmittedOrFail(page);

  await expect.poll(() => submittedTxIds.length, { timeout: 15_000 }).toBe(1);
  const bootTxId = submittedTxIds[0];
  await awaitTx(request, bootTxId);

  const tokenId = await waitForSingleToken(request);
  await page.getByRole("button", { name: "Refresh" }).click();
  await page.getByText(tokenId, { exact: true }).click();

  await page.getByRole("tab", { name: "End" }).click();
  await page.getByRole("button", { name: "End this cage" }).click();
  await waitForSubmittedOrFail(page);

  await expect.poll(() => submittedTxIds.length, { timeout: 15_000 }).toBe(2);
  const endTxId = submittedTxIds[1];
  await awaitTx(request, endTxId);
  await waitForTokenGone(request, tokenId);

  expect(proxiedFacts.boot).toEqual([{ address: devnetGenesisAddressHex }]);
  expect(proxiedFacts.end).toEqual([
    { token: tokenId, address: devnetGenesisAddressHex },
  ]);

  const signArgs = await page.evaluate(() => window.__signArgs);
  expect(signArgs).toHaveLength(2);
  expect(signArgs.every((arg) => arg.partial === true)).toBe(true);
  expect(signArgs.every((arg) => /^[0-9a-f]+$/.test(arg.tx))).toBe(true);

  const reactorCalls = await page.evaluate(() => window.__reactorCalls);
  expect(reactorCalls.map((call) => call.op)).toEqual([
    "boot",
    "assemble",
    "end",
    "assemble",
  ]);
  expect(reactorCalls[0].stdout).toMatch(/^cage_tx: [0-9a-f]+$/);
  expect(reactorCalls[1].stdout).toMatch(/^signed_tx: [0-9a-f]+$/);
  expect(reactorCalls[2].stdout).toMatch(/^cage_tx: [0-9a-f]+$/);
  expect(reactorCalls[3].stdout).toMatch(/^signed_tx: [0-9a-f]+$/);
});

async function installDevnetWallet(page) {
  await page.exposeFunction("__mpfsSignTx", async (txHex) => signWitnessSet(txHex));
  await page.addInitScript(
    ({ address, balance, baseUrl }) => {
      window.MPFS_BASE_URL = baseUrl;
      window.__signArgs = [];
      window.__signErrors = [];
      window.__reactorCalls = [];

      Object.defineProperty(globalThis, "runCageReactor", {
        configurable: true,
        set(fn) {
          const wrapped = async (stdin) => {
            const envelope = JSON.parse(stdin);
            const result = await fn(stdin);
            window.__reactorCalls.push({
              op: envelope.op,
              exitOk: result.exitOk,
              stdout: result.stdout,
              stderr: result.stderr,
            });
            return result;
          };
          Object.defineProperty(globalThis, "runCageReactor", {
            configurable: true,
            writable: true,
            value: wrapped,
          });
        },
      });

      window.cardano = {
        devnet: {
          name: "Devnet Genesis",
          icon: "",
          enable: async () => ({
            getNetworkId: async () => 0,
            getUsedAddresses: async () => [address],
            getChangeAddress: async () => address,
            getBalance: async () => balance,
            signTx: async (tx, partial) => {
              window.__signArgs.push({ tx, partial });
              try {
                return await window.__mpfsSignTx(tx);
              } catch (error) {
                window.__signErrors.push(
                  String((error && (error.stack || error.message)) || error),
                );
                throw error;
              }
            },
            submitTx: async () => "unused-devnet-submit",
          }),
        },
      };
    },
    {
      address: devnetGenesisAddressHex,
      balance: walletBalance,
      baseUrl: devnetBaseUrl,
    },
  );
}

async function installDevnetProxy(page, submittedTxIds, proxiedFacts) {
  await page.route(`${devnetBaseUrl}/**`, async (route) => {
    const req = route.request();
    const corsHeaders = {
      "access-control-allow-origin": "*",
      "access-control-allow-methods": "GET,POST,OPTIONS",
      "access-control-allow-headers": "content-type",
    };

    if (req.method() === "OPTIONS") {
      await route.fulfill({ status: 204, headers: corsHeaders, body: "" });
      return;
    }

    const response = await route.fetch({ timeout: 90_000 });
    const body = await response.body();
    const headers = {
      ...response.headers(),
      ...corsHeaders,
    };
    delete headers["content-encoding"];
    delete headers["content-length"];

    const url = new URL(req.url());
    if (req.method() === "POST" && url.pathname === "/facts/boot") {
      proxiedFacts.boot.push(JSON.parse(req.postData() || "{}"));
    }
    if (req.method() === "POST" && url.pathname === "/facts/end") {
      proxiedFacts.end.push(JSON.parse(req.postData() || "{}"));
    }
    if (req.method() === "POST" && url.pathname === "/submit" && response.ok()) {
      submittedTxIds.push(JSON.parse(body.toString("utf8")).txId);
    }

    await route.fulfill({
      status: response.status(),
      headers,
      body,
    });
  });
}

async function waitForSubmittedOrFail(page) {
  const success = page.getByText("Submitted transaction").waitFor({
    state: "visible",
    timeout: 120_000,
  });
  const failure = page
    .locator('[role="alert"]')
    .filter({ hasText: /failed|declined|error|HTTP /i })
    .waitFor({ state: "visible", timeout: 120_000 })
    .then(async () => {
      const alertText = await page
        .locator('[role="alert"]')
        .filter({ hasText: /failed|declined|error|HTTP /i })
        .last()
        .textContent();
      const diagnostics = await page.evaluate(() => ({
        signErrors: window.__signErrors || [],
        reactorCalls: window.__reactorCalls || [],
      }));
      throw new Error(
        `operation failed before submit: ${alertText}\n${JSON.stringify(diagnostics, null, 2)}`,
      );
    });

  await Promise.race([success, failure]);
}

async function waitForTrustedRoot(request) {
  await expect
    .poll(
      async () => {
        const response = await request.get(`${devnetBaseUrl}/status`, {
          timeout: 5_000,
        });
        if (!response.ok()) return null;
        const status = await response.json();
        return status.utxo_root;
      },
      { timeout: 120_000 },
    )
    .toMatch(/^[0-9a-f]{64}$/);
}

async function awaitTx(request, txId) {
  const response = await request.get(`${devnetBaseUrl}/tx/${txId}?timeout=60`, {
    timeout: 65_000,
  });
  expect([200, 204]).toContain(response.status());
}

async function waitForSingleToken(request) {
  let tokenId = null;
  await expect
    .poll(
      async () => {
        const tokens = await getTokens(request);
        tokenId = tokens[0] || null;
        return tokens.length;
      },
      { timeout: 60_000 },
    )
    .toBe(1);
  return tokenId;
}

async function waitForTokenGone(request, tokenId) {
  await expect
    .poll(
      async () => {
        const tokens = await getTokens(request);
        return tokens.includes(tokenId);
      },
      { timeout: 60_000 },
    )
    .toBe(false);
}

async function getTokens(request) {
  const response = await request.get(`${devnetBaseUrl}/tokens`, {
    timeout: 5_000,
  });
  expect(response.ok()).toBe(true);
  return response.json();
}
