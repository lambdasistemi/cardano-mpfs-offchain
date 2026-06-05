import { mkdir } from "node:fs/promises";

import { expect, test } from "@playwright/test";

import {
  preprodRequesterAddressHex,
  preprodRequesterOwner,
  signMoogWitnessSet,
} from "../../scripts/devnet-cip30-signer.mjs";

test.setTimeout(600_000);

const preprodBaseUrl = (process.env.MPFS_BASE_URL || "https://umpfs.plutimus.com")
  .replace(/\/+$/, "");
const walletPath = process.env.MPFS_SIGNER_WALLET;
const walletBalance = "1b006a94d74f430000";
const shotsDir = process.env.MPFS_SHOTS_DIR || "/tmp/orch-spa-ux/shots";
const factKey = "start";
const initialFactValue = "amaru";
const updatedFactValue = "cardano";

test("runs the full token fact lifecycle through the real browser reactor on preprod", async ({
  page,
  request,
}) => {
  expect(walletPath, "MPFS_SIGNER_WALLET is required").toBeTruthy();
  await mkdir(shotsDir, { recursive: true });

  const submittedTxIds = [];
  const proxiedFacts = {
    boot: [],
    requestInsert: [],
    requestUpdate: [],
    requestDelete: [],
    updateRoot: [],
    end: [],
  };

  await waitForTrustedRoot(request);
  const initialTokens = new Set(await getTokens(request));
  await installPreprodProxy(page, submittedTxIds, proxiedFacts);
  await installPreprodWallet(page);

  await page.goto("/");
  await page.getByRole("button", { name: "Connect" }).click();
  await expect(page.getByRole("banner").getByText("Moog Requester")).toBeVisible();
  await screenshot(page, "00-connected.png");

  await page.getByRole("tab", { name: "Tokens" }).click();
  await page.getByRole("button", { name: "Register new token" }).click();
  await waitForTxCountOrFail(page, submittedTxIds, 1);
  const bootTxId = submittedTxIds[0];
  await expectCardanoscanLink(page, bootTxId);
  console.log(`preprod boot tx: ${bootTxId}`);
  await screenshot(page, "01-register-submitted.png");
  await awaitTx(request, bootTxId);

  const tokenId = await waitForNewOwnedToken(request, initialTokens);
  const tokenState = unwrapTokenState(await getTokenState(request, tokenId));
  expect(tokenState?.process_time).toBe(1_800_000);
  expect(tokenState?.retract_time).toBe(1_800_000);
  await page.getByRole("button", { name: "Refresh" }).click();
  await page.getByText(tokenId, { exact: true }).click();
  await screenshot(page, "02-token-selected.png");

  await page.getByRole("tab", { name: "Facts" }).click();
  const insertForm = requestForm(page, "Request insert");
  await insertForm.getByLabel("Key").fill(factKey);
  await insertForm.getByLabel("Value").fill(initialFactValue);
  await expect(insertForm.getByLabel("Value")).toHaveValue(initialFactValue);
  await insertForm.getByRole("button", { name: "Request insert" }).click();
  await waitForTxCountOrFail(page, submittedTxIds, 2);
  const insertTxId = submittedTxIds[1];
  await expectCardanoscanLink(page, insertTxId);
  console.log(`preprod insert request tx: ${insertTxId}`);
  await screenshot(page, "03-insert-submitted.png");
  await awaitTx(request, insertTxId);
  await waitForRequestCount(request, tokenId, 1);
  await page.getByRole("button", { name: "Refresh" }).click();
  await expect(page.getByText("insert - start")).toBeVisible();
  await expect(page.getByText(/Value amaru/)).toBeVisible();
  await screenshot(page, "04-insert-pending.png");

  const rootBeforeInsertProcess = await getTokenRoot(request, tokenId);
  await processPendingRequests(page);
  await waitForTxCountOrFail(page, submittedTxIds, 3);
  const processInsertTxId = submittedTxIds[2];
  await expectCardanoscanLink(page, processInsertTxId);
  console.log(`preprod process insert tx: ${processInsertTxId}`);
  await screenshot(page, "05-process-insert-submitted.png");
  await awaitTx(request, processInsertTxId);
  await waitForRootChanged(request, tokenId, rootBeforeInsertProcess);
  await waitForRequestCount(request, tokenId, 0);
  await waitForFactValue(request, tokenId, factKey, initialFactValue);
  await page.getByRole("button", { name: "Refresh" }).click();
  await expectNoProcessableRequests(page);
  await lookupFact(page, factKey, initialFactValue);
  await screenshot(page, "06-insert-processed.png");

  const updateForm = requestForm(page, "Request update");
  await updateForm.getByLabel("Key").fill(factKey);
  await updateForm.getByLabel("Old value").fill(initialFactValue);
  await updateForm.getByLabel("New value").fill(updatedFactValue);
  await expect(updateForm.getByLabel("New value")).toHaveValue(updatedFactValue);
  await updateForm.getByRole("button", { name: "Request update" }).click();
  await waitForTxCountOrFail(page, submittedTxIds, 4);
  const updateTxId = submittedTxIds[3];
  await expectCardanoscanLink(page, updateTxId);
  console.log(`preprod update request tx: ${updateTxId}`);
  await screenshot(page, "07-update-submitted.png");
  await awaitTx(request, updateTxId);
  await waitForRequestCount(request, tokenId, 1);
  await page.getByRole("button", { name: "Refresh" }).click();
  await expect(page.getByText("update - start")).toBeVisible();
  await expect(page.getByText(/Value cardano/)).toBeVisible();
  await screenshot(page, "08-update-pending.png");

  const rootBeforeUpdateProcess = await getTokenRoot(request, tokenId);
  await processPendingRequests(page);
  await waitForTxCountOrFail(page, submittedTxIds, 5);
  const processUpdateTxId = submittedTxIds[4];
  await expectCardanoscanLink(page, processUpdateTxId);
  console.log(`preprod process update tx: ${processUpdateTxId}`);
  await screenshot(page, "09-process-update-submitted.png");
  await awaitTx(request, processUpdateTxId);
  await waitForRootChanged(request, tokenId, rootBeforeUpdateProcess);
  await waitForRequestCount(request, tokenId, 0);
  await waitForFactValue(request, tokenId, factKey, updatedFactValue);
  await page.getByRole("button", { name: "Refresh" }).click();
  await expectNoProcessableRequests(page);
  await lookupFact(page, factKey, updatedFactValue);
  await screenshot(page, "10-update-processed.png");

  const deleteForm = requestForm(page, "Request delete");
  await deleteForm.getByLabel("Key").fill(factKey);
  await deleteForm.getByLabel("Value").fill(updatedFactValue);
  await expect(deleteForm.getByLabel("Value")).toHaveValue(updatedFactValue);
  await deleteForm.getByRole("button", { name: "Request delete" }).click();
  await waitForTxCountOrFail(page, submittedTxIds, 6);
  const deleteTxId = submittedTxIds[5];
  await expectCardanoscanLink(page, deleteTxId);
  console.log(`preprod delete request tx: ${deleteTxId}`);
  await screenshot(page, "11-delete-submitted.png");
  await awaitTx(request, deleteTxId);
  await waitForRequestCount(request, tokenId, 1);
  await page.getByRole("button", { name: "Refresh" }).click();
  await expect(page.getByText("delete - start")).toBeVisible();
  await lookupFact(page, factKey, updatedFactValue);
  await screenshot(page, "12-delete-pending.png");

  const rootBeforeDeleteProcess = await getTokenRoot(request, tokenId);
  await processPendingRequests(page);
  await waitForTxCountOrFail(page, submittedTxIds, 7);
  const processDeleteTxId = submittedTxIds[6];
  await expectCardanoscanLink(page, processDeleteTxId);
  console.log(`preprod process delete tx: ${processDeleteTxId}`);
  await screenshot(page, "13-process-delete-submitted.png");
  await awaitTx(request, processDeleteTxId);
  await waitForRootChanged(request, tokenId, rootBeforeDeleteProcess);
  await waitForRequestCount(request, tokenId, 0);
  await waitForFactAbsent(request, tokenId, factKey);
  await page.getByRole("button", { name: "Refresh" }).click();
  await expectNoProcessableRequests(page);
  await screenshot(page, "14-delete-processed.png");

  await page.getByRole("tab", { name: "End" }).click();
  await page.getByRole("button", { name: "End this cage" }).click();
  await waitForTxCountOrFail(page, submittedTxIds, 8);
  const endTxId = submittedTxIds[7];
  await expectCardanoscanLink(page, endTxId);
  console.log(`preprod end tx: ${endTxId}`);
  await screenshot(page, "15-end-submitted.png");
  await awaitTx(request, endTxId);
  await waitForTokenGone(request, tokenId);
  await page.getByRole("tab", { name: "Tokens" }).click();
  await page.getByRole("button", { name: "Refresh" }).click();
  await expect(page.getByText(tokenId, { exact: true })).not.toBeVisible();
  await screenshot(page, "16-ended.png");

  expect(proxiedFacts.boot).toEqual([{ address: preprodRequesterAddressHex }]);
  expect(proxiedFacts.requestInsert).toEqual([
    {
      token: tokenId,
      key: utf8Hex(factKey),
      value: utf8Hex(initialFactValue),
      address: preprodRequesterAddressHex,
    },
  ]);
  expect(proxiedFacts.requestUpdate).toEqual([
    {
      token: tokenId,
      key: utf8Hex(factKey),
      old_value: utf8Hex(initialFactValue),
      new_value: utf8Hex(updatedFactValue),
      address: preprodRequesterAddressHex,
    },
  ]);
  expect(proxiedFacts.requestDelete).toEqual([
    {
      token: tokenId,
      key: utf8Hex(factKey),
      value: utf8Hex(updatedFactValue),
      address: preprodRequesterAddressHex,
    },
  ]);
  expect(proxiedFacts.updateRoot).toEqual([
    { token: tokenId, address: preprodRequesterAddressHex },
    { token: tokenId, address: preprodRequesterAddressHex },
    { token: tokenId, address: preprodRequesterAddressHex },
  ]);
  expect(proxiedFacts.end).toEqual([
    { token: tokenId, address: preprodRequesterAddressHex },
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
  for (const call of reactorCalls) {
    if (call.op === "assemble") {
      expect(call.stdout).toMatch(/^signed_tx: [0-9a-f]+$/);
    } else {
      expect(call.stdout).toMatch(/^cage_tx: [0-9a-f]+$/);
    }
  }
});

async function installPreprodWallet(page) {
  await page.exposeFunction("__mpfsSignTx", async (txHex) =>
    signMoogWitnessSet(txHex, { walletPath }),
  );
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
        moogpreprod: {
          name: "Moog Requester",
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
            submitTx: async () => "unused-preprod-submit",
          }),
        },
      };
    },
    {
      address: preprodRequesterAddressHex,
      balance: walletBalance,
      baseUrl: preprodBaseUrl,
    },
  );
}

async function installPreprodProxy(page, submittedTxIds, proxiedFacts) {
  await page.route(`${preprodBaseUrl}/**`, async (route) => {
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

    const response = await route.fetch({ timeout: 120_000 });
    const body = await response.body();
    const headers = {
      ...response.headers(),
      ...corsHeaders,
    };
    delete headers["content-encoding"];
    delete headers["content-length"];

    const url = new URL(req.url());
    if (req.method() === "POST") {
      const factKeyByPath = {
        "/facts/boot": "boot",
        "/facts/request/insert": "requestInsert",
        "/facts/request/update": "requestUpdate",
        "/facts/request/delete": "requestDelete",
        "/facts/update": "updateRoot",
        "/facts/end": "end",
      }[url.pathname];

      if (factKeyByPath) {
        proxiedFacts[factKeyByPath].push(JSON.parse(req.postData() || "{}"));
      }
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

async function waitForTxCountOrFail(
  page,
  submittedTxIds,
  expectedCount,
  timeout = 180_000,
) {
  const success = expect
    .poll(() => submittedTxIds.length, { timeout })
    .toBeGreaterThanOrEqual(expectedCount);
  const failure = page
    .locator('[role="alert"]')
    .filter({ hasText: /failed|declined|error|HTTP /i })
    .waitFor({ state: "visible", timeout: 180_000 })
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
  await expect(page.getByText("Submitted transaction")).toBeVisible();
}

async function expectCardanoscanLink(page, txId) {
  await expect(page.getByRole("link", { name: "View on preprod Cardanoscan" }))
    .toHaveAttribute("href", `https://preprod.cardanoscan.io/transaction/${txId}`);
}

function requestForm(page, buttonName) {
  return page
    .getByRole("button", { name: buttonName })
    .locator("xpath=ancestor::*[.//input][1]");
}

async function processPendingRequests(page) {
  const button = page.getByRole("button", {
    name: /^Process requests \([1-9][0-9]*\)$/,
  });
  await expect(button).toBeEnabled({ timeout: 60_000 });
  await button.click();
}

async function expectNoProcessableRequests(page) {
  await expect(
    page.getByRole("button", { name: "No processable requests" }),
  ).toBeDisabled();
}

async function lookupFact(page, key, expectedValue) {
  await page.getByLabel("Key").last().fill(key);
  await page.getByRole("button", { name: "Look up" }).click();
  await expect(
    page.locator('[role="alert"]').filter({ hasText: expectedValue }),
  ).toBeVisible();
}

async function screenshot(page, name) {
  await page.screenshot({ path: `${shotsDir}/${name}`, fullPage: true });
}

async function waitForTrustedRoot(request) {
  await expect
    .poll(
      async () => {
        const response = await request.get(`${preprodBaseUrl}/status`, {
          timeout: 10_000,
        });
        if (!response.ok()) return null;
        const status = await response.json();
        return status.utxo_root;
      },
      { timeout: 180_000 },
    )
    .toMatch(/^[0-9a-f]{64}$/);
}

async function awaitTx(request, txId) {
  const response = await request.get(`${preprodBaseUrl}/tx/${txId}?timeout=90`, {
    timeout: 95_000,
  });
  expect([200, 204]).toContain(response.status());
}

async function waitForRequestCount(request, tokenId, expectedCount) {
  await expect
    .poll(
      async () => {
        const requests = await getRequests(request, tokenId);
        return requests.length;
      },
      { timeout: 120_000 },
    )
    .toBe(expectedCount);
}

async function waitForRootChanged(request, tokenId, previousRoot) {
  let changedRoot = null;
  await expect
    .poll(
      async () => {
        const root = await getTokenRoot(request, tokenId);
        if (root && root !== previousRoot) {
          changedRoot = root;
          return true;
        }
        return false;
      },
      { timeout: 120_000 },
    )
    .toBe(true);
  return changedRoot;
}

async function waitForFactValue(request, tokenId, key, expectedValue) {
  const keyHex = utf8Hex(key);
  const expectedHex = utf8Hex(expectedValue);
  await expect
    .poll(
      async () => {
        const response = await request.get(
          `${preprodBaseUrl}/tokens/${tokenId}/facts/${keyHex}`,
          { timeout: 10_000 },
        );
        if (!response.ok()) return null;
        const body = await response.json();
        return body?.value || null;
      },
      { timeout: 120_000 },
    )
    .toBe(expectedHex);
}

async function waitForFactAbsent(request, tokenId, key) {
  const keyHex = utf8Hex(key);
  await expect
    .poll(
      async () => {
        const response = await request.get(
          `${preprodBaseUrl}/tokens/${tokenId}/facts/${keyHex}`,
          { timeout: 10_000 },
        );
        return response.status();
      },
      { timeout: 120_000 },
    )
    .toBe(404);
}

async function waitForNewOwnedToken(request, initialTokens) {
  let matched = null;
  await expect
    .poll(
      async () => {
        const tokens = await getTokens(request);
        for (const token of tokens) {
          if (initialTokens.has(token)) continue;
          const state = await getTokenState(request, token);
          if (unwrapTokenState(state)?.owner === preprodRequesterOwner) {
            matched = token;
            return token;
          }
        }
        return null;
      },
      { timeout: 120_000 },
    )
    .toMatch(/^[0-9a-f]+$/);
  return matched;
}

async function waitForTokenGone(request, tokenId) {
  await expect
    .poll(
      async () => {
        const tokens = await getTokens(request);
        return tokens.includes(tokenId);
      },
      { timeout: 120_000 },
    )
    .toBe(false);
}

async function getTokens(request) {
  const response = await request.get(`${preprodBaseUrl}/tokens`, {
    timeout: 10_000,
  });
  expect(response.ok()).toBe(true);
  return decodeTokens(await response.json());
}

async function getTokenState(request, tokenId) {
  const response = await request.get(`${preprodBaseUrl}/tokens/${tokenId}`, {
    timeout: 10_000,
  });
  if (!response.ok()) return null;
  return response.json();
}

async function getTokenRoot(request, tokenId) {
  const state = unwrapTokenState(await getTokenState(request, tokenId));
  return state?.root || null;
}

async function getRequests(request, tokenId) {
  const response = await request.get(`${preprodBaseUrl}/tokens/${tokenId}/requests`, {
    timeout: 10_000,
  });
  if (!response.ok()) return [];
  const body = await response.json();
  return Array.isArray(body) ? body : body?.requests || [];
}

function decodeTokens(body) {
  if (Array.isArray(body)) return body;
  if (Array.isArray(body?.tokens)) return body.tokens;
  throw new Error(`unexpected /tokens response: ${JSON.stringify(body)}`);
}

function unwrapTokenState(body) {
  return body?.state?.state || body?.state || body || null;
}

function utf8Hex(value) {
  return Buffer.from(value, "utf8").toString("hex");
}
