# WASM (wasm32-wasi) targets for the pure MPFS verifier, wired through
# self.lib.wasm.mkCardanoLedgerWasm (mirrored from cardano-ledger-inspector).
#
# wasm-mpfs-verify builds the mpfs-verify-reactor executable from the
# cardano-mpfs-verify package against the full wasm fork set + wasm32-built
# C libraries (libsodium / secp256k1 / blst, via cardano-crypto-class).
#
# Bump forks.json / cabal-wasm.project -> recompute dependenciesHash by
# setting it to pkgs.lib.fakeHash and replacing with the hash Nix prints.
{ pkgs
, libWasm
, ghcWasmMeta
, wasiSdk
, chap
, src
}:
let
  verifyForks = [
    "cborg"
    "plutus"
    "hs-memory"
    "foundation"
    "network"
    "double-conversion"
    "criterion-measurement"
    "haskell-lmdb-mock"
    "cardano-mpfs-onchain"
    "haskell-mts"
    "aiken-codegen"
    "cardano-tx-tools"
  ];
in
{
  wasm-mpfs-verify = libWasm.mkCardanoLedgerWasm {
    inherit pkgs ghcWasmMeta wasiSdk chap src;
    projectFile = "cabal-wasm.project";
    packages = [ "mpfs-verify-reactor" ];
    srpForks = verifyForks;
    withCLibs = true;
    # TODO(#258): compute the real FOD hash. The dependency-download phase
    # evaluates and bootstraps correctly at index-state 2025-12-07, but the
    # build host is at 99% disk; the hackage-index + ledger-closure download
    # needs headroom. Replace fakeHash with the sha256 Nix prints once the
    # FOD completes on a host with free space.
    dependenciesHash = pkgs.lib.fakeHash;
  };
}
