# Implementation Plan: CORS on the MPFS HTTP API

**Branch**: `feat/383-cors` | **Spec**: [spec.md](./spec.md)

## Technical Shape

The HTTP API already funnels through `mkApp :: Context IO -> Application` in `cardano-mpfs-offchain/lib/Cardano/MPFS/HTTP/Server.hs`. The slice should wrap the `serve (Proxy @FullAPI) ...` application there so direct callers and test harnesses all see the same middleware. `withApplication` should remain the lifecycle boundary for building `Context IO`; it does not need to know about CORS.

`wai-cors` is present in the current Nix dev shell as `wai-cors-0.2.7`, and `Network.Wai.Middleware.Cors` exports `cors`, `CorsResourcePolicy`, and `simpleCorsResourcePolicy`. Add the dependency to `cardano-mpfs-offchain/cardano-mpfs-offchain.cabal` and use a policy equivalent to:

- origin policy: permissive, with no credentials;
- methods: `GET`, `POST`, `OPTIONS`;
- request headers: `content-type`;
- ignored failures: false, so malformed CORS requests do not silently become allowed requests.

The tests should live in the existing offchain HTTP unit suite. A small `Cardano.MPFS.HTTP.CorsSpec` can reuse `StatusSpec.mkTestContext` and run the WAI application directly with `Network.Wai.Test`, avoiding a live node or Warp server. Wire it into `cardano-mpfs-offchain/test/main.hs`.

## Slice Plan

### Slice 1 - HTTP CORS Middleware

One bisect-safe behavior commit:

- add a RED HTTP unit spec for preflight and actual `GET /tokens` CORS headers;
- add `wai-cors` to the offchain package dependency list;
- wrap `mkApp` with the CORS policy;
- run focused unit proof, then the branch gate.

## Verification

Focused proof:

```bash
just unit "CORS"
```

Branch gate:

```bash
./gate.sh
```

The local gate covers the focused test, offchain builds, non-Docker CI mirror, full e2e suite, and Cabal version parity. The documentation dependency-graph drift check is implemented as a GitHub Actions action in `.github/workflows/deploy-docs.yaml`; treat it as a PR check before ready-for-review.
