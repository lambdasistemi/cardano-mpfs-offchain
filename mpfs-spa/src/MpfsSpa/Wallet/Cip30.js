// CIP-30 dApp-connector FFI.
//
// All bindings read off `window.cardano.<key>`. The enable() / api.* methods
// return Promises, surfaced to PureScript as `Effect (Promise a)` and lifted
// with `toAffE` on the PureScript side. No MPFS protocol logic lives here —
// the wallet only enumerates, enables, reads addresses/balance/network, and
// signs/submits transactions the CageHelpers boundary produced.

export const _availableWallets = () => {
  const cardano =
    (typeof window !== "undefined" && window.cardano) || {};
  const out = [];
  for (const key of Object.keys(cardano)) {
    const w = cardano[key];
    if (w && typeof w.enable === "function" && typeof w.name === "string") {
      out.push({ key, name: w.name, icon: w.icon || "" });
    }
  }
  return out;
};

export const _enable = (key) => () => window.cardano[key].enable();

export const _getNetworkId = (api) => () => api.getNetworkId();
export const _getUsedAddresses = (api) => () => api.getUsedAddresses();
export const _getChangeAddress = (api) => () => api.getChangeAddress();
export const _getBalance = (api) => () => api.getBalance();
export const _subscribeAccountChanges = (api) => (handler) => () => {
  let cancelled = false;
  const safeHandler = () => {
    if (!cancelled) handler();
  };
  const cleanups = [
    subscribeWalletEvent(api, "accountChange", safeHandler),
    subscribeWalletEvent(api, "networkChange", safeHandler),
    subscribeWalletEvent(api, "accountsChanged", safeHandler),
    subscribeWalletEvent(api, "chainChanged", safeHandler),
  ].filter(Boolean);

  return () => {
    cancelled = true;
    for (const cleanup of cleanups) {
      try {
        cleanup();
      } catch (_e) {
        // Ignore wallet cleanup errors; the dApp is leaving this account.
      }
    }
  };
};
export const _signTx = (api) => (tx) => (partial) => () =>
  api.signTx(tx, partial);
export const _submitTx = (api) => (tx) => () => api.submitTx(tx);

function subscribeWalletEvent(api, eventName, handler) {
  const targets = [api && api.experimental, api].filter(Boolean);
  for (const target of targets) {
    const cleanup = trySubscribeTarget(target, eventName, handler);
    if (cleanup) return cleanup;
  }
  return null;
}

function trySubscribeTarget(target, eventName, handler) {
  try {
    if (typeof target.on === "function") {
      const result = target.on(eventName, handler);
      if (typeof result === "function") return result;
      if (typeof target.off === "function") {
        return () => target.off(eventName, handler);
      }
      if (typeof target.removeListener === "function") {
        return () => target.removeListener(eventName, handler);
      }
    }

    const method =
      "on" + eventName.charAt(0).toUpperCase() + eventName.slice(1);
    if (typeof target[method] === "function") {
      const result = target[method](handler);
      if (typeof result === "function") return result;
      return () => {};
    }
  } catch (_e) {
    return null;
  }
  return null;
}

// --- display-only balance decoding ------------------------------------------
// getBalance() returns a CBOR-encoded ledger Value. For display we only need
// the lovelace (coin) component. This is a deliberately small CBOR reader that
// handles the two shapes a balance takes — a bare unsigned int (coin only), or
// a 2-element array [coin, multiasset] — and returns the coin as a decimal
// string, or null if it cannot be read. This is presentational, not protocol.

function hexToBytes(hex) {
  const a = [];
  for (let i = 0; i + 1 < hex.length; i += 2) {
    a.push(parseInt(hex.substr(i, 2), 16));
  }
  return a;
}

function readUint(b, i) {
  const info = b[i] & 0x1f;
  if (info < 24) return BigInt(info);
  if (info === 24) return BigInt(b[i + 1]);
  if (info === 25) return (BigInt(b[i + 1]) << 8n) | BigInt(b[i + 2]);
  if (info === 26) {
    let v = 0n;
    for (let k = 1; k <= 4; k++) v = (v << 8n) | BigInt(b[i + k]);
    return v;
  }
  if (info === 27) {
    let v = 0n;
    for (let k = 1; k <= 8; k++) v = (v << 8n) | BigInt(b[i + k]);
    return v;
  }
  return null;
}

export const _coinOfBalance = (hex) => {
  try {
    const b = hexToBytes(hex);
    let i = 0;
    // Skip a CBOR array header (major type 4) to reach the coin element.
    if ((b[0] & 0xe0) === 0x80) i = 1;
    // The coin must be a CBOR unsigned int (major type 0).
    if ((b[i] & 0xe0) !== 0x00) return null;
    const v = readUint(b, i);
    return v === null ? null : v.toString();
  } catch (_e) {
    return null;
  }
};
