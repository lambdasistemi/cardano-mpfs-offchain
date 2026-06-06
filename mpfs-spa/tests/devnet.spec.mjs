import { mkdir } from "node:fs/promises";
import path from "node:path";

import { expect, test } from "@playwright/test";

import {
  devnetGenesisAddressHex,
  signWitnessSet,
} from "../../scripts/devnet-cip30-signer.mjs";

test.setTimeout(300_000);

const devnetBaseUrl = process.env.MPFS_DEVNET_BASE_URL;
const shotsDir = process.env.MPFS_SHOTS_DIR || "/tmp/orch-ui/shots-integrated";
const walletBalance = "1b006a94d74f430000";
const devnetProcessTimeMs = 45_000;
const devnetRetractTimeMs = 10_000;
const uiTokenId =
  "9999999999999999999999999999999999999999999999999999999999999999";
const uiOwnerHash = "11".repeat(28);
const uiRequesterHash = "22".repeat(28);
const uiOtherHash = "33".repeat(28);
const uiOwnerAddressHex = `00${uiOwnerHash}`;
const uiRequesterAddressHex = `00${uiRequesterHash}`;
const uiOtherAddressHex = `00${uiOtherHash}`;

test("separates facts and selectable requests with owner-gated actions", async ({
  page,
}) => {
  await mkdir(shotsDir, { recursive: true });

  const seen = await installUiContractServer(page);
  await installUiContractWallet(page, uiOwnerAddressHex);

  await page.goto("/");
  await installReactorStub(page);

  await page.getByRole("button", { name: "Connect", exact: true }).click();
  await expect(page.getByRole("heading", { name: "Facts" })).toBeVisible();
  await expect(page.getByRole("heading", { name: "Pending requests" })).toBeVisible();
  await expect(page.getByText("owner 11111111...111111 (you)").first()).toBeVisible();
  await expect(page.getByRole("button", { name: "Process selected" })).toBeVisible();
  await expect(page.getByRole("button", { name: "Reject selected" })).toBeVisible();
  await expect(page.getByRole("button", { name: "Add fact" })).toBeVisible();
  await expect(page.getByRole("button", { name: "End token" })).toBeVisible();
  await expect(page.getByText("amaru")).toBeVisible();
  await expect(page.getByText("Pending requests are selected individually")).toBeVisible();
  await shot(page, "ui-contract-light");

  await page.getByRole("checkbox", { name: "Select request aaa#0" }).check();
  await page.getByRole("button", { name: "Process selected" }).click();
  await expect.poll(() => seen.update.length).toBe(1);
  await expect.poll(() => seen.submit.length).toBe(1);
  expect(seen.update[0]).toEqual({
    token: uiTokenId,
    address: uiOwnerAddressHex,
    requests: ["aaa#0"],
  });

  await refreshWorkbench(page);
  await page.getByRole("checkbox", { name: "Select request aaa#0" }).check();
  await page.getByRole("checkbox", { name: "Select request bbb#1" }).check();
  await page.getByRole("button", { name: "Process selected" }).click();
  await expect.poll(() => seen.update.length).toBe(2);
  await expect.poll(() => seen.submit.length).toBe(2);
  expect(seen.update[1]).toEqual({
    token: uiTokenId,
    address: uiOwnerAddressHex,
    requests: ["aaa#0", "bbb#1"],
  });

  await refreshWorkbench(page);
  await page.getByRole("checkbox", { name: "Select request ccc#2" }).check();
  await page.getByRole("checkbox", { name: "Select request ddd#3" }).check();
  await page.getByRole("button", { name: "Reject selected" }).click();
  await expect.poll(() => seen.reject.length).toBe(1);
  await expect.poll(() => seen.submit.length).toBe(3);
  expect(seen.reject[0]).toEqual({
    token: uiTokenId,
    address: uiOwnerAddressHex,
    requests: ["ccc#2", "ddd#3"],
  });

  await refreshWorkbench(page);
  await page.getByLabel("Mine only").click();
  await page.evaluate((address) => {
    window.__mpfsWalletAddress = address;
  }, uiRequesterAddressHex);
  await page.getByRole("button", { name: "Refresh account" }).click();
  await expect(page.getByText("read-only for this token")).toBeVisible();
  await expect(page.getByRole("button", { name: "Process selected" })).toHaveCount(0);
  await expect(page.getByRole("button", { name: "Reject selected" })).toHaveCount(0);
  await expect(page.getByRole("button", { name: "Add fact" })).toHaveCount(0);
  await expect(page.getByRole("button", { name: "End token" })).toHaveCount(0);
  await page.getByRole("button", { name: "Retract request eee#4" }).click();
  await expect.poll(() => seen.retract.length).toBe(1);
  await expect.poll(() => seen.submit.length).toBe(4);
  expect(seen.retract[0]).toEqual({
    utxo: "eee#4",
    address: uiRequesterAddressHex,
  });

  await page.getByRole("button", { name: "Toggle theme" }).click();
  await shot(page, "ui-contract-dark");
});

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
    retract: [],
    reject: [],
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

  await page.getByRole("button", { name: "Register new token" }).first().click();
  await waitForSubmitCount(page, submittedTxIds, 1);
  await awaitTx(request, submittedTxIds[0]);

  const tokenId = await waitForSingleToken(request);
  await page.getByRole("button", { name: "Refresh tokens" }).click();
  await page
    .getByRole("button", { name: new RegExp(tokenId.slice(0, 12)) })
    .click();
  await expect(page.getByText("Mine", { exact: true })).toBeVisible();
  await expect(page.getByText("No committed facts for this token.")).toBeVisible();
  await expect(page.getByRole("heading", { name: "Facts" })).toBeVisible();
  await expect(page.getByRole("heading", { name: "Pending requests" })).toBeVisible();
  await expect(page.getByText("you can manage this token")).toBeVisible();
  await expect(page.getByRole("button", { name: "Add fact" })).toBeVisible();
  await expect(page.getByRole("button", { name: "End token" })).toBeVisible();
  await shot(page, "02-token-selected");

  await page.getByLabel("Mine only").click();
  await page.evaluate((address) => {
    window.__mpfsWalletAddress = address;
  }, uiOtherAddressHex);
  await page.getByRole("button", { name: "Refresh account" }).click();
  await expect(page.getByText("read-only for this token")).toBeVisible();
  await expect(page.getByRole("button", { name: "Add fact" })).toHaveCount(0);
  await expect(page.getByRole("button", { name: "End token" })).toHaveCount(0);
  await page.evaluate((address) => {
    window.__mpfsWalletAddress = address;
  }, devnetGenesisAddressHex);
  await page.getByRole("button", { name: "Refresh account" }).click();
  await expect(page.getByText("you can manage this token")).toBeVisible();

  await requestInsert(page, submittedTxIds);
  await awaitTx(request, submittedTxIds[1]);
  const insertRequestId =
    await waitForRequest(request, tokenId, "insert", "start", "amaru");
  await refreshWorkbench(page);
  await expect(page.getByText("Insert", { exact: true })).toBeVisible();
  await shot(page, "03-insert-pending");

  await processRequests(page, request, submittedTxIds, 3, [insertRequestId]);
  await waitForFact(request, tokenId, "start", "amaru");
  await refreshWorkbench(page);
  await expect(page.getByText("amaru").first()).toBeVisible();
  await shot(page, "04-insert-processed");

  await page.getByRole("button", { name: /Edit fact start/ }).click();
  await page.getByLabel("New value").fill("cardano");
  await page.getByRole("button", { name: "Request update" }).click();
  await waitForSubmitCount(page, submittedTxIds, 4);
  await awaitTx(request, submittedTxIds[3]);
  const updateRequestId =
    await waitForRequest(request, tokenId, "update", "start", "cardano");
  await refreshWorkbench(page);
  await expect(page.getByText("Update", { exact: true })).toBeVisible();
  await shot(page, "05-update-pending");

  await processRequests(page, request, submittedTxIds, 5, [updateRequestId]);
  await waitForFact(request, tokenId, "start", "cardano");
  await refreshWorkbench(page);
  await expect(page.getByText("cardano").first()).toBeVisible();
  await shot(page, "06-update-processed");

  await page.getByRole("button", { name: /Delete fact start/ }).click();
  await page.getByRole("button", { name: "Request delete" }).click();
  await waitForSubmitCount(page, submittedTxIds, 6);
  await awaitTx(request, submittedTxIds[5]);
  const deleteRequestId =
    await waitForRequest(request, tokenId, "delete", "start", null);
  await refreshWorkbench(page);
  await expect(page.getByText("Delete", { exact: true })).toBeVisible();
  await shot(page, "07-delete-pending");

  await processRequests(page, request, submittedTxIds, 7, [deleteRequestId]);
  await waitForNoFacts(request, tokenId);
  await refreshWorkbench(page);
  await expect(page.getByText("No committed facts for this token.")).toBeVisible();
  await shot(page, "08-delete-processed");

  await requestInsertWithKey(page, submittedTxIds, "subset-a", "one", 8);
  await awaitTx(request, submittedTxIds[7]);
  const subsetARequestId =
    await waitForRequest(request, tokenId, "insert", "subset-a", "one");
  await requestInsertWithKey(page, submittedTxIds, "subset-b", "two", 9);
  await awaitTx(request, submittedTxIds[8]);
  const subsetBRequestId =
    await waitForRequest(request, tokenId, "insert", "subset-b", "two");
  await requestInsertWithKey(page, submittedTxIds, "subset-c", "three", 10);
  await awaitTx(request, submittedTxIds[9]);
  const subsetCRequestId =
    await waitForRequest(request, tokenId, "insert", "subset-c", "three");
  await refreshWorkbench(page);
  await expect(page.getByText("subset-a")).toBeVisible();
  await shot(page, "09-process-subset-pending");

  await processRequests(page, request, submittedTxIds, 11, [
    subsetARequestId,
    subsetCRequestId,
  ]);
  await waitForFact(request, tokenId, "subset-a", "one");
  await waitForFact(request, tokenId, "subset-c", "three");
  await waitForFactAbsent(request, tokenId, "subset-b");
  await waitForRequestRefs(request, tokenId, [subsetBRequestId]);
  await refreshWorkbench(page);
  await shot(page, "10-process-subset-selected");

  await processRequests(page, request, submittedTxIds, 12, [subsetBRequestId]);
  await waitForFact(request, tokenId, "subset-b", "two");
  await waitForRequestRefs(request, tokenId, []);

  await requestInsertWithKey(page, submittedTxIds, "retract-me", "later", 13);
  await awaitTx(request, submittedTxIds[12]);
  const retractRequestId =
    await waitForRequest(request, tokenId, "insert", "retract-me", "later");
  await waitForRequestPhase(page, retractRequestId, "retractable");
  await retractRequest(page, request, submittedTxIds, 14, retractRequestId);
  await waitForRequestRefs(request, tokenId, []);
  await waitForFactAbsent(request, tokenId, "retract-me");
  await refreshWorkbench(page);
  await shot(page, "11-retract-owned");

  await requestInsertWithKey(page, submittedTxIds, "reject-a", "red", 15);
  await awaitTx(request, submittedTxIds[14]);
  const rejectARequestId =
    await waitForRequest(request, tokenId, "insert", "reject-a", "red");
  await requestInsertWithKey(page, submittedTxIds, "reject-b", "green", 16);
  await awaitTx(request, submittedTxIds[15]);
  const rejectBRequestId =
    await waitForRequest(request, tokenId, "insert", "reject-b", "green");
  await requestInsertWithKey(page, submittedTxIds, "reject-c", "blue", 17);
  await awaitTx(request, submittedTxIds[16]);
  const rejectCRequestId =
    await waitForRequest(request, tokenId, "insert", "reject-c", "blue");
  await waitForRequestPhase(page, rejectARequestId, "expired");
  await waitForRequestPhase(page, rejectBRequestId, "expired");
  await waitForRequestPhase(page, rejectCRequestId, "expired");
  await refreshWorkbench(page);
  await shot(page, "12-reject-subset-pending");

  await rejectRequests(page, request, submittedTxIds, 18, [
    rejectARequestId,
    rejectCRequestId,
  ]);
  await waitForRequestRefs(request, tokenId, [rejectBRequestId]);
  await waitForFactAbsent(request, tokenId, "reject-a");
  await waitForFactAbsent(request, tokenId, "reject-c");
  await refreshWorkbench(page);
  await shot(page, "13-reject-subset-selected");

  await rejectRequests(page, request, submittedTxIds, 19, [rejectBRequestId]);
  await waitForRequestRefs(request, tokenId, []);
  await waitForTokenReadable(request, tokenId);
  await refreshWorkbench(page);

  await page.getByRole("button", { name: "End token" }).first().click();
  await page.getByRole("dialog").getByRole("button", { name: "End token" }).click();
  await waitForSubmitCount(page, submittedTxIds, 20);
  await awaitTx(request, submittedTxIds[19]);
  await waitForTokenGone(request, tokenId);
  await refreshWorkbench(page);
  await shot(page, "14-ended");

  expect(proxiedFacts.boot).toEqual([{ address: devnetGenesisAddressHex }]);
  expect(proxiedFacts.insert).toEqual([
    {
      token: tokenId,
      key: utf8Hex("start"),
      value: utf8Hex("amaru"),
      address: devnetGenesisAddressHex,
    },
    {
      token: tokenId,
      key: utf8Hex("subset-a"),
      value: utf8Hex("one"),
      address: devnetGenesisAddressHex,
    },
    {
      token: tokenId,
      key: utf8Hex("subset-b"),
      value: utf8Hex("two"),
      address: devnetGenesisAddressHex,
    },
    {
      token: tokenId,
      key: utf8Hex("subset-c"),
      value: utf8Hex("three"),
      address: devnetGenesisAddressHex,
    },
    {
      token: tokenId,
      key: utf8Hex("retract-me"),
      value: utf8Hex("later"),
      address: devnetGenesisAddressHex,
    },
    {
      token: tokenId,
      key: utf8Hex("reject-a"),
      value: utf8Hex("red"),
      address: devnetGenesisAddressHex,
    },
    {
      token: tokenId,
      key: utf8Hex("reject-b"),
      value: utf8Hex("green"),
      address: devnetGenesisAddressHex,
    },
    {
      token: tokenId,
      key: utf8Hex("reject-c"),
      value: utf8Hex("blue"),
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
    { token: tokenId, address: devnetGenesisAddressHex, requests: [insertRequestId] },
    { token: tokenId, address: devnetGenesisAddressHex, requests: [updateRequestId] },
    { token: tokenId, address: devnetGenesisAddressHex, requests: [deleteRequestId] },
    {
      token: tokenId,
      address: devnetGenesisAddressHex,
      requests: [subsetARequestId, subsetCRequestId],
    },
    { token: tokenId, address: devnetGenesisAddressHex, requests: [subsetBRequestId] },
  ]);
  expect(proxiedFacts.retract).toEqual([
    { utxo: retractRequestId, address: devnetGenesisAddressHex },
  ]);
  expect(proxiedFacts.reject).toEqual([
    {
      token: tokenId,
      address: devnetGenesisAddressHex,
      requests: [rejectARequestId, rejectCRequestId],
    },
    { token: tokenId, address: devnetGenesisAddressHex, requests: [rejectBRequestId] },
  ]);
  expect(proxiedFacts.end).toEqual([
    { token: tokenId, address: devnetGenesisAddressHex },
  ]);

  const signArgs = await page.evaluate(() => window.__signArgs);
  expect(signArgs).toHaveLength(20);
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
    "request_insert",
    "assemble",
    "request_insert",
    "assemble",
    "request_insert",
    "assemble",
    "update",
    "assemble",
    "update",
    "assemble",
    "request_insert",
    "assemble",
    "retract",
    "assemble",
    "request_insert",
    "assemble",
    "request_insert",
    "assemble",
    "request_insert",
    "assemble",
    "reject",
    "assemble",
    "reject",
    "assemble",
    "end",
    "assemble",
  ]);
  expect(reactorCalls.every((call) => call.exitOk)).toBe(true);
  expect(reactorCalls.filter((call) => call.op === "assemble")).toHaveLength(20);
});

async function requestInsert(page, submittedTxIds) {
  await requestInsertWithKey(page, submittedTxIds, "start", "amaru", 2);
}

async function requestInsertWithKey(
  page,
  submittedTxIds,
  key,
  value,
  expectedSubmitCount,
) {
  await page.getByRole("button", { name: "Add fact" }).first().click();
  await page.getByLabel("Key").fill(key);
  await page.getByLabel("Value").fill(value);
  await page.getByRole("button", { name: "Request insert" }).click();
  await waitForSubmitCount(page, submittedTxIds, expectedSubmitCount);
}

async function processRequests(
  page,
  request,
  submittedTxIds,
  expectedSubmitCount,
  requestIds,
) {
  for (const requestId of requestIds) {
    await page.getByRole("checkbox", { name: `Select request ${requestId}` }).check();
  }
  await clickEnabledButton(page, "Process selected");
  await waitForSubmitCount(page, submittedTxIds, expectedSubmitCount);
  await awaitTx(request, submittedTxIds[expectedSubmitCount - 1]);
}

async function rejectRequests(
  page,
  request,
  submittedTxIds,
  expectedSubmitCount,
  requestIds,
) {
  for (const requestId of requestIds) {
    await page.getByRole("checkbox", { name: `Select request ${requestId}` }).check();
  }
  await clickEnabledButton(page, "Reject selected");
  await waitForSubmitCount(page, submittedTxIds, expectedSubmitCount);
  await awaitTx(request, submittedTxIds[expectedSubmitCount - 1]);
}

async function retractRequest(
  page,
  request,
  submittedTxIds,
  expectedSubmitCount,
  requestId,
) {
  await page.getByRole("button", { name: `Retract request ${requestId}` }).click();
  await waitForSubmitCount(page, submittedTxIds, expectedSubmitCount);
  await awaitTx(request, submittedTxIds[expectedSubmitCount - 1]);
}

async function waitForRequestPhase(page, requestId, phase) {
  const requestPattern = new RegExp(escapeRegExp(requestId));
  await expect
    .poll(
      async () => {
        await refreshWorkbench(page);
        const text = await page
          .getByRole("row", { name: requestPattern })
          .textContent()
          .catch(() => "");
        return text && text.includes(phase) ? phase : text || "";
      },
      { timeout: 90_000 },
    )
    .toBe(phase);
}

async function clickEnabledButton(page, name) {
  const button = page.getByRole("button", { name });
  await expect(button).toBeEnabled({ timeout: 15_000 });
  await button.evaluate((node) => node.click());
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
    ({ address, balance, baseUrl, processTime, retractTime }) => {
      window.MPFS_BASE_URL = baseUrl;
      window.MPFS_CAGE_DEFAULT_PROCESS_TIME = String(processTime);
      window.MPFS_CAGE_DEFAULT_RETRACT_TIME = String(retractTime);
      window.__mpfsWalletAddress = address;
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
            getUsedAddresses: async () => [window.__mpfsWalletAddress],
            getChangeAddress: async () => window.__mpfsWalletAddress,
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
      processTime: devnetProcessTimeMs,
      retractTime: devnetRetractTimeMs,
    },
  );
}

async function installUiContractWallet(page, initialAddress) {
  await page.addInitScript(
    ({ address, balance }) => {
      window.__mpfsWalletAddress = address;
      window.__signArgs = [];
      window.cardano = {
        uiwallet: {
          name: "UI Contract Wallet",
          icon: "",
          enable: async () => ({
            getNetworkId: async () => 0,
            getUsedAddresses: async () => [window.__mpfsWalletAddress],
            getChangeAddress: async () => window.__mpfsWalletAddress,
            getBalance: async () => balance,
            signTx: async (tx, partial) => {
              window.__signArgs.push({ tx, partial });
              return "bead";
            },
            submitTx: async () => "unused-ui-submit",
          }),
        },
      };
    },
    { address: initialAddress, balance: walletBalance },
  );
}

async function installUiContractServer(page) {
  const now = Date.now();
  const seen = {
    update: [],
    reject: [],
    retract: [],
    submit: [],
  };
  const requests = [
    uiRequest("aaa", 0, "insert", "alpha", "one", uiRequesterHash, now - 500),
    uiRequest("bbb", 1, "update", "beta", "two", uiOtherHash, now - 600),
    uiRequest("ccc", 2, "delete", "gamma", null, uiOtherHash, now - 300_000),
    uiRequest("ddd", 3, "insert", "delta", "four", uiRequesterHash, now - 310_000),
    uiRequest("eee", 4, "update", "epsilon", "five", uiRequesterHash, now - 180_000),
  ];

  await page.route("**/tokens", async (route) => {
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({ tokens: { entries: [{ token_id: uiTokenId }] } }),
    });
  });

  await page.route(`**/tokens/${uiTokenId}`, async (route) => {
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({
        state: {
          state: {
            owner: uiOwnerHash,
            root: "ab".repeat(32),
            tip: 2_000_000,
            process_time: 120_000,
            retract_time: 120_000,
          },
        },
      }),
    });
  });

  await page.route(`**/tokens/${uiTokenId}/facts`, async (route) => {
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({
        facts: [{ key: utf8Hex("start"), value: utf8Hex("amaru") }],
      }),
    });
  });

  await page.route(`**/tokens/${uiTokenId}/requests`, async (route) => {
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({ requests }),
    });
  });

  await page.route("**/status", async (route) => {
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({ utxo_root: "cd".repeat(32) }),
    });
  });

  await page.route("**/eval-context", async (route) => {
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({ era: "ui-contract" }),
    });
  });

  await page.route("**/facts/update", async (route) => {
    seen.update.push(JSON.parse(route.request().postData() || "{}"));
    await fulfillUiFacts(route);
  });

  await page.route("**/facts/reject", async (route) => {
    seen.reject.push(JSON.parse(route.request().postData() || "{}"));
    await fulfillUiFacts(route);
  });

  await page.route("**/facts/retract", async (route) => {
    seen.retract.push(JSON.parse(route.request().postData() || "{}"));
    await fulfillUiFacts(route);
  });

  await page.route("**/submit", async (route) => {
    seen.submit.push(JSON.parse(route.request().postData() || "{}"));
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({ txId: `ui-tx-${seen.submit.length}` }),
    });
  });

  return seen;
}

async function installReactorStub(page) {
  await page.evaluate(() => {
    globalThis.runCageReactor = async (stdin) => {
      const envelope = JSON.parse(stdin);
      if (envelope.op === "assemble") {
        return { stdout: "signed_tx: f00d", stderr: "", exitOk: true };
      }
      return { stdout: "cage_tx: cafe" + envelope.op.length, stderr: "", exitOk: true };
    };
  });
}

async function fulfillUiFacts(route) {
  await route.fulfill({
    status: 200,
    contentType: "application/json",
    body: JSON.stringify({
      snapshot: { utxo_root: "cd".repeat(32), chainpoint: "ui" },
      token: uiTokenId,
      owner: uiOwnerHash,
      requests: [],
      protocol_parameters: { cbor: "00", verified: false },
    }),
  });
}

function uiRequest(txId, txIx, operation, key, value, owner, submittedAt) {
  const request = {
    token: uiTokenId,
    owner,
    key: utf8Hex(key),
    operation,
    fee: 2_000_000,
    submitted_at: submittedAt,
  };
  if (value !== null) request.value = utf8Hex(value);
  return {
    request,
    utxo: {
      tx_in: { tx_id: txId, tx_ix: txIx },
      txout_cbor: "00",
      inclusion_proof: "proof",
    },
  };
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
    if (req.method() === "POST" && url.pathname === "/facts/retract") {
      proxiedFacts.retract.push(JSON.parse(req.postData() || "{}"));
    }
    if (req.method() === "POST" && url.pathname === "/facts/reject") {
      proxiedFacts.reject.push(JSON.parse(req.postData() || "{}"));
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
      if (submittedTxIds.length >= expected) return;
      const alertText = await page
        .locator('[role="alert"]')
        .filter({ hasText: /failed|declined|error|HTTP /i })
        .last()
        .textContent();
      if (submittedTxIds.length >= expected) return;
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

async function waitForTokenReadable(request, tokenId) {
  await expect
    .poll(
      async () => {
        const state = await request.get(`${devnetBaseUrl}/tokens/${tokenId}`, {
          timeout: 5_000,
        });
        const facts = await request.get(`${devnetBaseUrl}/tokens/${tokenId}/facts`, {
          timeout: 5_000,
        });
        return `${state.status()}/${facts.status()}`;
      },
      { timeout: 60_000 },
    )
    .toBe("200/200");
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

async function waitForFactAbsent(request, tokenId, key) {
  await expect
    .poll(
      async () => {
        const facts = await getFacts(request, tokenId);
        return facts.some((entry) => entry.key === utf8Hex(key));
      },
      { timeout: 60_000 },
    )
    .toBe(false);
}

async function waitForRequestRefs(request, tokenId, expectedRefs) {
  const expected = JSON.stringify([...expectedRefs].sort());
  await expect
    .poll(
      async () => {
        const refs = (await getRequests(request, tokenId)).map(requestRef).sort();
        return JSON.stringify(refs);
      },
      { timeout: 60_000 },
    )
    .toBe(expected);
}

async function waitForRequest(request, tokenId, operation, key, value) {
  let found = null;
  await expect
    .poll(
      async () => {
        const requests = await getRequests(request, tokenId);
        found = requests.find((entry) => {
          const req = entry.request || entry;
          return (
            req.operation === operation &&
            req.key === utf8Hex(key) &&
            (value === null ? req.value == null : req.value === utf8Hex(value))
          );
        });
        return found ? requestRef(found) : null;
      },
      { timeout: 60_000 },
    )
    .toMatch(/^[0-9a-f]+#\d+$/);
  return requestRef(found);
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

function requestRef(entry) {
  if (entry.requestId) return entry.requestId;
  if (entry.request_id) return entry.request_id;
  if (entry.utxo?.tx_in) {
    return `${entry.utxo.tx_in.tx_id}#${entry.utxo.tx_in.tx_ix}`;
  }
  if (entry.tx_id != null && entry.tx_ix != null) {
    return `${entry.tx_id}#${entry.tx_ix}`;
  }
  throw new Error(`request ref missing: ${JSON.stringify(entry)}`);
}

function escapeRegExp(text) {
  return String(text).replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}
