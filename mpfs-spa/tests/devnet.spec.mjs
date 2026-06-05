import { mkdir } from "node:fs/promises";
import path from "node:path";

import { expect, test } from "@playwright/test";

import {
  devnetGenesisAddressHex,
  signWitnessSet,
} from "../../scripts/devnet-cip30-signer.mjs";

test.setTimeout(300_000);

const devnetBaseUrl = process.env.MPFS_DEVNET_BASE_URL;
const shotsDir =
  process.env.MPFS_SHOTS_DIR || "/tmp/orch-spa-ux/shots-v2";
const walletBalance = "1b006a94d74f430000";

test("runs the full token facts lifecycle from the polished workbench", async ({
  page,
  request,
}) => {
  expect(devnetBaseUrl, "MPFS_DEVNET_BASE_URL is required").toBeTruthy();
  await mkdir(shotsDir, { recursive: true });

  const submittedTxIds = [];
  const proxiedFacts = {
    boot: [],
    insert: [],
    update: [],
    delete: [],
    process: [],
    end: [],
  };

  await waitForTrustedRoot(request);
  await installDevnetProxy(page, submittedTxIds, proxiedFacts);
  await installDevnetWallet(page);

  await page.goto("/");
  await expect(page.getByText("Connect an account, register a token")).toBeVisible();
  await expect(page.getByText("No tokens yet.")).toBeVisible();
  await shot(page, "00-empty-workbench-light");
  await page.getByRole("button", { name: "Toggle theme" }).click();
  await shot(page, "00-empty-workbench-dark");
  await page.getByRole("button", { name: "Toggle theme" }).click();

  await page.getByRole("button", { name: "Connect", exact: true }).click();
  await expect(page.getByRole("banner").getByText("Devnet Genesis")).toBeVisible();
  await expect(page.getByLabel("Mine only")).toBeChecked();
  await shot(page, "01-connected-light");
  await page.getByRole("button", { name: "Toggle theme" }).click();
  await shot(page, "01-connected-dark");

  await page.getByRole("button", { name: "Register token" }).first().click();
  await waitForSubmitCount(page, submittedTxIds, 1);
  await awaitTx(request, submittedTxIds[0]);

  const tokenId = await waitForSingleToken(request);
  await page.getByRole("button", { name: "Refresh tokens" }).click();
  await page
    .getByRole("button", { name: new RegExp(tokenId.slice(0, 12)) })
    .click();
  await expect(page.getByText("Mine", { exact: true })).toBeVisible();
  await expect(page.getByText("No facts for this token.")).toBeVisible();
  await shot(page, "02-token-selected");

  await requestInsert(page, submittedTxIds);
  await awaitTx(request, submittedTxIds[1]);
  await waitForRequest(request, tokenId, "insert", "start", "amaru");
  await refreshWorkbench(page);
  await expect(page.getByText("Insert", { exact: true })).toBeVisible();
  await shot(page, "03-insert-pending");

  await processRequests(page, request, submittedTxIds, 3);
  await waitForFact(request, tokenId, "start", "amaru");
  await refreshWorkbench(page);
  await expect(page.getByText("amaru").first()).toBeVisible();
  await shot(page, "04-insert-processed");

  await page.getByRole("button", { name: /Edit fact start/ }).click();
  await page.getByLabel("New value").fill("cardano");
  await page.getByRole("button", { name: "Request update" }).click();
  await waitForSubmitCount(page, submittedTxIds, 4);
  await awaitTx(request, submittedTxIds[3]);
  await waitForRequest(request, tokenId, "update", "start", "cardano");
  await refreshWorkbench(page);
  await expect(page.getByText("Update", { exact: true })).toBeVisible();
  await shot(page, "05-update-pending");

  await processRequests(page, request, submittedTxIds, 5);
  await waitForFact(request, tokenId, "start", "cardano");
  await refreshWorkbench(page);
  await expect(page.getByText("cardano").first()).toBeVisible();
  await shot(page, "06-update-processed");

  await page.getByRole("button", { name: /Delete fact start/ }).click();
  await page.getByRole("button", { name: "Request delete" }).click();
  await waitForSubmitCount(page, submittedTxIds, 6);
  await awaitTx(request, submittedTxIds[5]);
  await waitForRequest(request, tokenId, "delete", "start", null);
  await refreshWorkbench(page);
  await expect(page.getByText("Delete", { exact: true })).toBeVisible();
  await shot(page, "07-delete-pending");

  await processRequests(page, request, submittedTxIds, 7);
  await waitForNoFacts(request, tokenId);
  await refreshWorkbench(page);
  await expect(page.getByText("No facts for this token.")).toBeVisible();
  await shot(page, "08-delete-processed");

  await page.getByRole("button", { name: "End token" }).first().click();
  await page.getByRole("dialog").getByRole("button", { name: "End token" }).click();
  await waitForSubmitCount(page, submittedTxIds, 8);
  await awaitTx(request, submittedTxIds[7]);
  await waitForTokenGone(request, tokenId);
  await refreshWorkbench(page);
  await shot(page, "09-ended");

  expect(proxiedFacts.boot).toEqual([{ address: devnetGenesisAddressHex }]);
  expect(proxiedFacts.insert).toEqual([
    {
      token: tokenId,
      key: utf8Hex("start"),
      value: utf8Hex("amaru"),
      address: devnetGenesisAddressHex,
    },
  ]);
  expect(proxiedFacts.update).toEqual([
    {
      token: tokenId,
      key: utf8Hex("start"),
      old_value: utf8Hex("amaru"),
      new_value: utf8Hex("cardano"),
      address: devnetGenesisAddressHex,
    },
  ]);
  expect(proxiedFacts.delete).toEqual([
    {
      token: tokenId,
      key: utf8Hex("start"),
      value: utf8Hex("cardano"),
      address: devnetGenesisAddressHex,
    },
  ]);
  expect(proxiedFacts.process).toEqual([
    { token: tokenId, address: devnetGenesisAddressHex },
    { token: tokenId, address: devnetGenesisAddressHex },
    { token: tokenId, address: devnetGenesisAddressHex },
  ]);
  expect(proxiedFacts.end).toEqual([
    { token: tokenId, address: devnetGenesisAddressHex },
  ]);

  const signArgs = await page.evaluate(() => window.__signArgs);
  expect(signArgs).toHaveLength(8);
  expect(signArgs.every((arg) => arg.partial === true)).toBe(true);
  expect(signArgs.every((arg) => /^[0-9a-f]+$/.test(arg.tx))).toBe(true);

  const reactorCalls = await page.evaluate(() => window.__reactorCalls);
  expect(reactorCalls.map((call) => call.op)).toEqual([
    "boot",
    "assemble",
    "request_insert",
    "assemble",
    "update",
    "assemble",
    "request_update",
    "assemble",
    "update",
    "assemble",
    "request_delete",
    "assemble",
    "update",
    "assemble",
    "end",
    "assemble",
  ]);
  expect(reactorCalls.every((call) => call.exitOk)).toBe(true);
  expect(reactorCalls.filter((call) => call.op === "assemble")).toHaveLength(8);
});

async function requestInsert(page, submittedTxIds) {
  await page.getByRole("button", { name: "Add fact" }).first().click();
  await page.getByLabel("Key").fill("start");
  await page.getByLabel("Value").fill("amaru");
  await page.getByRole("button", { name: "Request insert" }).click();
  await waitForSubmitCount(page, submittedTxIds, 2);
}

async function processRequests(page, request, submittedTxIds, expectedSubmitCount) {
  await page.getByRole("button", { name: "Process requests" }).click();
  await waitForSubmitCount(page, submittedTxIds, expectedSubmitCount);
  await awaitTx(request, submittedTxIds[expectedSubmitCount - 1]);
}

async function refreshWorkbench(page) {
  await page.getByRole("button", { name: "Refresh", exact: true }).click();
}

async function shot(page, name) {
  await page.screenshot({
    path: path.join(shotsDir, `${name}.png`),
    fullPage: true,
  });
}

async function installDevnetWallet(page) {
  await page.exposeFunction("__mpfsSignTx", async (txHex) => signWitnessSet(txHex));
  await page.addInitScript(
    ({ address, balance, baseUrl }) => {
      window.MPFS_BASE_URL = baseUrl;
      try {
        window.localStorage.setItem("mpfs-spa-theme-mode", "light");
      } catch (_error) {
        // The app still falls back to its OS/default theme if storage is blocked.
      }
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
    if (req.method() === "POST" && url.pathname === "/facts/request/insert") {
      proxiedFacts.insert.push(JSON.parse(req.postData() || "{}"));
    }
    if (req.method() === "POST" && url.pathname === "/facts/request/update") {
      proxiedFacts.update.push(JSON.parse(req.postData() || "{}"));
    }
    if (req.method() === "POST" && url.pathname === "/facts/request/delete") {
      proxiedFacts.delete.push(JSON.parse(req.postData() || "{}"));
    }
    if (req.method() === "POST" && url.pathname === "/facts/update") {
      proxiedFacts.process.push(JSON.parse(req.postData() || "{}"));
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

async function waitForSubmitCount(page, submittedTxIds, expected) {
  const success = expect
    .poll(() => submittedTxIds.length, { timeout: 120_000 })
    .toBe(expected);
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

async function waitForFact(request, tokenId, key, value) {
  await expect
    .poll(
      async () => {
        const facts = await getFacts(request, tokenId);
        const fact = facts.find((entry) => entry.key === utf8Hex(key));
        return fact ? fact.value : null;
      },
      { timeout: 60_000 },
    )
    .toBe(utf8Hex(value));
}

async function waitForNoFacts(request, tokenId) {
  await expect
    .poll(
      async () => {
        const facts = await getFacts(request, tokenId);
        return facts.length;
      },
      { timeout: 60_000 },
    )
    .toBe(0);
}

async function waitForRequest(request, tokenId, operation, key, value) {
  await expect
    .poll(
      async () => {
        const requests = await getRequests(request, tokenId);
        return requests.some((entry) => {
          const req = entry.request || entry;
          return (
            req.operation === operation &&
            req.key === utf8Hex(key) &&
            (value === null ? req.value == null : req.value === utf8Hex(value))
          );
        });
      },
      { timeout: 60_000 },
    )
    .toBe(true);
}

async function getTokens(request) {
  const response = await request.get(`${devnetBaseUrl}/tokens`, {
    timeout: 5_000,
  });
  expect(response.ok()).toBe(true);
  const body = await response.json();
  if (Array.isArray(body)) return body;
  return body.tokens.entries.map((entry) => entry.token_id);
}

async function getFacts(request, tokenId) {
  const response = await request.get(`${devnetBaseUrl}/tokens/${tokenId}/facts`, {
    timeout: 5_000,
  });
  expect(response.ok()).toBe(true);
  const body = await response.json();
  return body.facts;
}

async function getRequests(request, tokenId) {
  const response = await request.get(
    `${devnetBaseUrl}/tokens/${tokenId}/requests`,
    { timeout: 5_000 },
  );
  expect(response.ok()).toBe(true);
  const body = await response.json();
  return body.requests || [];
}

function utf8Hex(text) {
  return Buffer.from(text, "utf8").toString("hex");
}
