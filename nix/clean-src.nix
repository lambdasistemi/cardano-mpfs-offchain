# Filtered project source for the haskell.nix cabalProject and the wasm
# reactor build.
#
# haskell.nix hashes this `src` into every package's build plan, so an
# unfiltered `./.` makes *any* repo edit invalidate the whole Haskell (and
# wasm) build cache — a docs change, a CI-workflow edit, a flake lock prune,
# or release-please's per-release CHANGELOG/version bump all force a cold
# rebuild even though no Haskell input changed. Keep only the paths the cabal
# build actually reads.
#
# Excluded paths are NOT cabal build inputs (verified: no .cabal references
# them via data-files / extra-source-files). The swagger check reads docs/
# directly and is unaffected by filtering docs out of the Haskell source.
{ lib, src }:
let
  root = toString src;
  excludedTop = [
    "docs" # mkdocs site + swagger.json (own derivations)
    "specs" # speckit artifacts
    "lean" # Lean formal model
    "scripts" # helper scripts
    "flake.lock" # flake input metadata
    ".github" # CI workflows
    ".orch" # orchestration scratch
  ];
in
lib.cleanSourceWith {
  name = "cardano-mpfs-offchain-src";
  inherit src;
  filter = path: type:
    let
      rel = lib.removePrefix (root + "/") (toString path);
      parts = lib.splitString "/" rel;
      top = lib.head parts;
      isTopLevel = lib.length parts == 1;
    in
    lib.cleanSourceFilter path type
    && !(builtins.elem top excludedTop)
    # drop root-level *.md churn (CHANGELOG.md rewritten every release, etc.)
    && !(isTopLevel && lib.hasSuffix ".md" rel);
}
