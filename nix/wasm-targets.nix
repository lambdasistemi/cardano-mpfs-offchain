# WASM (wasm32-wasi) targets for the pure MPFS verifier, wired through the
# shared cardano-ledger-wasm builder. Ledger forks, flags, constraints,
# index-state, and C libraries come from cardano-ledger-wasm.lib.wasm; this
# file keeps only MPFS-specific package pins.
{ pkgs, libWasm, ghcWasmMeta, wasiSdk, chap, src }:
let
  lib = pkgs.lib;

  mpfsForks = {
    cardano-mpfs-onchain = {
      location = "https://github.com/cardano-foundation/cardano-mpfs-onchain";
      rev = "5c482af9289c6cbefe1062d04b828f3260d248ee";
      sha256 = "15aqmghgs9zrqsgx19kz1mx9j1a5n2ia486b93akm57lq8ikprph";
      subdirs = [ "haskell" ];
    };
    haskell-mts = {
      location = "https://github.com/lambdasistemi/haskell-mts";
      rev = "ab15f7b2dea73165b785c90333bbd09a36528a07";
      sha256 = "081wz3vq8d15wh1mziqnqhk2ai4i304gv7vxrhw9hqjr2sa9xy1a";
      subdirs = [ ];
    };
    aiken-codegen = {
      location = "https://github.com/paolino/aiken-codegen";
      rev = "74f364c10e930ce2bf47e64b755ede4424733325";
      sha256 = "0rg5hqjix9bs9radzib1ppy09qamxkkqr0hdvc8qm907c21cy2g8";
      subdirs = [ ];
    };
    cardano-tx-tools = {
      location = "https://github.com/lambdasistemi/cardano-tx-tools";
      rev = "631f1341fde6e4a11e94b058cf5f2925ffeb9eac";
      sha256 = "0asp3sx7hd8hip739kbz65986jlv8qx9342m9gqpqcxih6pxfgjj";
      subdirs = [ ];
    };
    rocksdb-kv-transactions = {
      location = "https://github.com/paolino/rocksdb-kv-transactions";
      rev = "0888387a5de81711273ea9b1e9d160decc33c231";
      sha256 = "0ywi4p744sk688p50f6n69llvxa1fws27wqciyhj4b57cqcpam4m";
      subdirs = [ ];
    };
  };

  mpfsForkNames = [
    "cardano-mpfs-onchain"
    "haskell-mts"
    "aiken-codegen"
    "cardano-tx-tools"
    "rocksdb-kv-transactions"
  ];

  fetchMpfsFork = name:
    let pin = mpfsForks.${name};
    in pkgs.fetchgit {
      url = pin.location;
      rev = pin.rev;
      hash = "sha256:${pin.sha256}";
    };

  fetchedMpfsForks = lib.genAttrs mpfsForkNames fetchMpfsFork;

  mpfsForkPackageLines = lib.concatLists (map (name:
    let pin = mpfsForks.${name};
    in if pin.subdirs == [ ] then
      [ "  ${fetchedMpfsForks.${name}}" ]
    else
      map (sub: "  ${fetchedMpfsForks.${name}}/${sub}") pin.subdirs)
    mpfsForkNames);

  mpfsForkPackagesBlock = ''
    packages:
  '' + lib.concatStringsSep "\n" mpfsForkPackageLines + "\n";

  renderSubdirs = subdirs:
    if subdirs == [ ] then
      ""
    else
      "  subdir:\n" + lib.concatMapStrings (subdir: "    ${subdir}\n") subdirs;

  renderPin = _name: pin: ''
    source-repository-package
      type: git
      location: ${pin.location}
      tag: ${pin.rev}
  '' + renderSubdirs pin.subdirs + ''
      --sha256: ${pin.sha256}
  '';

  ledgerForkProjectFragment =
    lib.concatStringsSep "\n" (lib.mapAttrsToList renderPin libWasm.forks.pins)
    + "\n"
    + lib.concatStringsSep "" (lib.mapAttrsToList (pkg: flags: ''
      package ${pkg}
        flags: ${flags}
    '') libWasm.forks.packageFlags)
    + "\n"
    + lib.concatStringsSep "" (lib.mapAttrsToList (pkg: opts: ''
      package ${pkg}
        ghc-options: ${opts}
    '') libWasm.forks.packageGhcOptions)
    + "\n"
    + lib.optionalString (libWasm.forks.constraints != [ ])
    ("constraints: " + lib.concatStringsSep ", " libWasm.forks.constraints
      + "\n")
    + lib.optionalString (libWasm.forks.allowNewer != [ ])
    ("allow-newer: " + lib.concatStringsSep ", " libWasm.forks.allowNewer
      + "\n");

  cabalWasmProject = pkgs.writeText "cabal-wasm.project" ''
    ${builtins.readFile ../cabal-wasm.project}

    ${ledgerForkProjectFragment}

    ${mpfsForkPackagesBlock}
  '';

  wasmSrc = pkgs.runCommand "cardano-mpfs-offchain-wasm-src" { } ''
    mkdir -p $out
    cp -rL ${src}/. $out/
    chmod -R u+w $out
    cp ${cabalWasmProject} $out/cabal-wasm.project
    substituteInPlace $out/cardano-mpfs-api/cardano-mpfs-api.cabal \
      --replace-fail "    Cardano.MPFS.API.Types.Facts" \
                     "    Cardano.MPFS.API.Types.Facts"$'\n\n'"  other-extensions:"
    substituteInPlace $out/cardano-mpfs-verify/cardano-mpfs-verify.cabal \
      --replace-fail "    Cardano.MPFS.Client.Verify.Write" \
                     "    Cardano.MPFS.Client.Verify.Write"$'\n\n'"  other-extensions:"
    substituteInPlace $out/cardano-mpfs-cage-tx/cardano-mpfs-cage-tx.cabal \
      --replace-fail "    Cardano.MPFS.Client.Cage.Update" \
                     "    Cardano.MPFS.Client.Cage.Update"$'\n\n'"  other-extensions:"
  '';
in {
  wasm-mpfs-verify = (libWasm.mkCardanoLedgerWasm {
    inherit pkgs ghcWasmMeta wasiSdk chap;
    projectFile = "cabal-wasm.project";
    # mpfs-verify-reactor exercises the verifier closure; building the
    # cardano-mpfs-cage-tx library forces every cage transaction builder
    # to cross-compile to wasm32-wasi alongside it (#258 cage extension).
    packages =
      [ "mpfs-verify-reactor" "mpfs-cage-reactor" "cardano-mpfs-cage-tx" ];
    src = wasmSrc;
    srpForks = [
      "plutus"
      "hs-memory"
      "criterion-measurement"
      "haskell-lmdb-mock"
      "double-conversion"
      "cborg"
      "foundation"
      "network"
    ];
    withCLibs = true;
    # FOD hash of the wasm dependency-download phase. Recompute by setting
    # to pkgs.lib.fakeHash and replacing with the sha256 Nix prints.
    dependenciesHash = "sha256-6/dXruvWUAtO5mL/0mmhqUwjb9M6baONdEJ48KxR2nY=";
  }).overrideAttrs (old: {
    configurePhase = old.configurePhase + ''
      for name in cardano-mpfs-api cardano-mpfs-verify cardano-mpfs-cage-tx; do
        for pkg in $(find dist-newstyle/build -mindepth 3 -maxdepth 3 -type d \
                       -path '*/wasm32-wasi/*' -name "$name-0.0.0"); do
          echo "purging MPFS local path-package dist entries: $pkg"
          rm -rf "$pkg"
        done
        find dist-newstyle -name 'package.conf.d' -type d | while read -r d; do
          for entry in "$d/$name"-0.0.0-inplace*.conf; do
            [ -e "$entry" ] && rm -f "$entry"
          done
        done
      done

      cat >> cabal-wasm.project <<'EOF'

      package cardano-mpfs-verify
        ghc-options: -Wno-unused-packages

      package cardano-mpfs-cage-tx
        ghc-options: -Wno-unused-packages
      EOF
    '';
  });
}
