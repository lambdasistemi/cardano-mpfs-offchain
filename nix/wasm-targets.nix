# WASM (wasm32-wasi) targets for the pure MPFS verifier, wired through
# self.lib.wasm.mkCardanoLedgerWasm (mirrored from cardano-ledger-inspector).
#
# wasm-mpfs-verify builds the mpfs-verify-reactor executable from the
# cardano-mpfs-verify package against the full wasm fork set + wasm32-built
# C libraries (libsodium / secp256k1 / blst, via cardano-crypto-class).
#
# Bump forks.json / cabal-wasm.project -> recompute dependenciesHash by
# setting it to pkgs.lib.fakeHash and replacing with the hash Nix prints.
{ pkgs, libWasm, ghcWasmMeta, wasiSdk, chap, src }:
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
    "rocksdb-kv-transactions"
  ];
in {
  wasm-mpfs-verify = libWasm.mkCardanoLedgerWasm {
    inherit pkgs ghcWasmMeta wasiSdk chap src;
    projectFile = "cabal-wasm.project";
    # mpfs-verify-reactor exercises the verifier closure; building the
    # cardano-mpfs-cage-tx library forces every cage transaction builder
    # to cross-compile to wasm32-wasi alongside it (#258 cage extension).
    packages = [ "mpfs-verify-reactor" "cardano-mpfs-cage-tx" ];
    srpForks = verifyForks;
    withCLibs = true;
    # FOD hash of the wasm dependency-download phase. Recompute by setting
    # to pkgs.lib.fakeHash and replacing with the sha256 Nix prints.
    dependenciesHash = "sha256-IzbKKNVRPaKdXTsJ4nNSz1PkHSJRq0RDXSWh7BR15Ik=";
  };
}
