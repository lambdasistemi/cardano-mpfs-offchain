#!/usr/bin/env bash
set -euo pipefail

: "${MPFS_SPA_SITE_DIR:?set MPFS_SPA_SITE_DIR to the built SPA dist}"

pick_port() {
  node -e '
    const net = require("node:net");
    const srv = net.createServer();
    srv.listen(0, "127.0.0.1", () => {
      const port = srv.address().port;
      srv.close(() => console.log(port));
    });
  '
}

playwright_port="${PLAYWRIGHT_PORT:-$(pick_port)}"
export MPFS_BASE_URL="${MPFS_BASE_URL:-https://umpfs.plutimus.com}"
export MPFS_SIGNER_WALLET="${MPFS_SIGNER_WALLET:-/code/moog/tmp/requester.json}"
export PLAYWRIGHT_PORT="${playwright_port}"
export PLAYWRIGHT_BASE_URL="http://127.0.0.1:${playwright_port}/"

node --input-type=module - <<'NODE'
const baseUrl = process.env.MPFS_BASE_URL;
const address = process.env.MPFS_SIGNER_ADDRESS_HEX ||
  "60b42e31bb7a391052a1b51ee9264e22ebb74b5cee7e26a6e5c996644c";

const response = await fetch(`${baseUrl}/facts/boot`, {
  method: "POST",
  headers: { "content-type": "application/json" },
  body: JSON.stringify({ address }),
});
if (!response.ok) {
  console.error(`preprod funding check failed: HTTP ${response.status}`);
  process.exit(1);
}

const facts = await response.json();
const count = Array.isArray(facts.wallet_utxos) ? facts.wallet_utxos.length : 0;
if (count < 1) {
  console.error("preprod funding check failed: no wallet UTxOs");
  process.exit(1);
}
console.error(`preprod funding check: wallet_utxos=${count}`);
NODE

cd mpfs-spa
ln -sfn \
  "$(dirname "$(dirname "$(readlink -f "$(command -v playwright)")")")/lib/node_modules" \
  node_modules

playwright test tests/preprod.spec.mjs --reporter=list
