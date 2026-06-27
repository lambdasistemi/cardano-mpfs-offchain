# Tasks: Retire `mpfs-spa`

## Slice S1 - Remove SPA Playwright e2e wiring

- [X] T372-S1 Remove `test-playwright-spa` shell apps, SPA e2e scripts, and
  `just` recipes while keeping the SPA package itself buildable until S2.
- [X] T372-S1 Prove the wasm keep-invariant, flake check, and absence of
  `test-playwright-spa` / `e2e-spa` references.

## Slice S2 - Remove SPA package and source

- [ ] T372-S2 Remove `mpfs-spa/`, `nix/mpfs-spa.nix`, package/dev-shell/CI
  wiring, clean-source inclusion, and stale docs references.
- [ ] T372-S2 Prove the full gate, including no live `mpfs-spa` references and
  the unchanged wasm keep-invariant.
