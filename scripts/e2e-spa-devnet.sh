#!/usr/bin/env bash
set -euo pipefail

: "${MPFS_DEVNET_SERVER:?set MPFS_DEVNET_SERVER to mpfs-devnet-server}"
: "${MPFS_SPA_SITE_DIR:?set MPFS_SPA_SITE_DIR to the built SPA dist}"
: "${MPFS_BLUEPRINT:?set MPFS_BLUEPRINT to the MPFS blueprint}"
: "${E2E_GENESIS_DIR:?set E2E_GENESIS_DIR to the devnet genesis directory}"

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

devnet_port="${MPFS_DEVNET_PORT:-$(pick_port)}"
playwright_port="${PLAYWRIGHT_PORT:-$(pick_port)}"
export MPFS_DEVNET_BASE_URL="http://127.0.0.1:${devnet_port}"
export PLAYWRIGHT_PORT="${playwright_port}"
export PLAYWRIGHT_BASE_URL="http://127.0.0.1:${playwright_port}/"

log_dir="${TMPDIR:-/tmp}/mpfs-spa-devnet.$RANDOM.$RANDOM"
mkdir -p "$log_dir"
devnet_log="$log_dir/mpfs-devnet-server.log"

cleanup() {
  local status=$?
  if [[ -n "${devnet_pid:-}" ]] && kill -0 "$devnet_pid" 2>/dev/null; then
    kill "$devnet_pid" 2>/dev/null || true
    wait "$devnet_pid" 2>/dev/null || true
  fi
  if [[ "$status" -ne 0 ]]; then
    echo "mpfs-devnet-server log: $devnet_log" >&2
    tail -n 200 "$devnet_log" >&2 || true
    echo "kept devnet log directory: $log_dir" >&2
  else
    rm -rf "$log_dir"
  fi
  exit "$status"
}
trap cleanup EXIT INT TERM

"$MPFS_DEVNET_SERVER" --port "$devnet_port" >"$devnet_log" 2>&1 &
devnet_pid=$!

for _ in $(seq 1 180); do
  if ! kill -0 "$devnet_pid" 2>/dev/null; then
    echo "mpfs-devnet-server exited before becoming ready" >&2
    exit 1
  fi
  if node -e '
    const url = `${process.env.MPFS_DEVNET_BASE_URL}/status`;
    fetch(url).then((r) => process.exit(r.ok ? 0 : 1)).catch(() => process.exit(1));
  '; then
    break
  fi
  sleep 1
done

if ! node -e '
  const url = `${process.env.MPFS_DEVNET_BASE_URL}/status`;
  fetch(url).then((r) => process.exit(r.ok ? 0 : 1)).catch(() => process.exit(1));
'; then
  echo "mpfs-devnet-server did not answer /status in time" >&2
  exit 1
fi

cd mpfs-spa
ln -sfn \
  "$(dirname "$(dirname "$(readlink -f "$(command -v playwright)")")")/lib/node_modules" \
  node_modules

playwright test tests/devnet.spec.mjs --reporter=list
