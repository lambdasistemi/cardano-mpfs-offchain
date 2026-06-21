# Installation

## Release tarballs

Each [GitHub release](https://github.com/lambdasistemi/cardano-mpfs-offchain/releases)
ships self-contained `mpfs-cli` bundles for `x86_64-linux` and
`aarch64-darwin`:

The tarball unpacks `bin/`, `lib/`, and `libexec/` into the current
directory:

```bash
mkdir mpfs-cli && tar -xzf mpfs-cli-<version>-x86_64-linux.tar.gz -C mpfs-cli
./mpfs-cli/bin/mpfs-cli --help
```

The bundle carries its own libraries and loader, so no Nix or Haskell
toolchain is needed on the target machine.

## Nix

The flake exposes the server and tools as packages:

```bash
nix build .#mpfs-serve   # production server
nix build .#mpfs-cli     # command-line client
nix run .#mpfs-devnet-server -- --port 3000   # devnet + API, for testing
```

## Docker

`just build-docker` builds the server image via Nix and loads it into
the local Docker daemon as
`ghcr.io/lambdasistemi/cardano-mpfs-offchain/mpfs-serve`, tagged with
the flake version.

## Running the server

`mpfs-serve` connects to a running cardano-node through its Unix
socket:

```bash
mpfs-serve \
  --socket /path/to/node.socket \
  --db /path/to/db \
  --port 3000 \
  --shelley-genesis /path/to/shelley-genesis.json \
  --blueprint /path/to/cage.json
```

| Flag | Required | Meaning |
|------|----------|---------|
| `--socket` | yes | cardano-node N2C Unix socket |
| `--db` | yes | RocksDB directory (created if missing) |
| `--port` | no (default 3000) | HTTP listen port |
| `--shelley-genesis` | yes | Shelley genesis JSON (network magic, stability window, security parameter) |
| `--blueprint` | yes | CIP-57 cage blueprint JSON (validator identity) |
| `--byron-genesis` | no | Byron genesis JSON |
| `--epoch-slots` | no (default 21600) | Byron epoch slots |
| `--mainnet` | no (default testnet) | Address network discriminator |

The service logs JSON lines on stderr and serves Swagger UI at
`/swagger-ui`.
