# Feature Specification: CORS on the MPFS HTTP API

**Branch**: `feat/383-cors` | **Date**: 2026-06-30 | **Issue**: [#383](https://github.com/lambdasistemi/cardano-mpfs-offchain/issues/383)

## User Story

As the MPFS browser SPA running from a preview or deployed static host, I can call the MPFS HTTP API on `umpfs.plutimus.com` directly, including browser preflight checks, without routing through a same-origin reverse proxy.

## Functional Requirements

- The WAI `Application` built by `Cardano.MPFS.HTTP.Server.mkApp` must apply CORS handling to every API path.
- Browser `OPTIONS` preflight requests must return CORS headers without falling through to Servant route handling.
- CORS must allow `GET`, `POST`, and `OPTIONS`.
- CORS must allow the `content-type` request header.
- The response to an actual cross-origin request such as `GET /tokens` with an `Origin` header must include `access-control-allow-origin`.
- The policy must support the preview browser origin `https://preview.dev.plutimus.com`. A permissive origin policy is acceptable for this public read API.

## Acceptance

- A focused HTTP unit test proves `OPTIONS /tokens` with `Origin` and `Access-Control-Request-*` headers returns `access-control-allow-origin`, allowed methods, and allowed headers.
- A focused HTTP unit test proves `GET /tokens` with an `Origin` header returns `access-control-allow-origin` while preserving the endpoint response.
- The implementation stays inside the HTTP application boundary and does not change verifier logic, browser code, deploy scripts, or on-chain behavior.

## Non-Goals

- Deploying `umpfs.plutimus.com`.
- Changing browser-side base URL configuration.
- Adding credentialed browser sessions, authentication, or origin-specific authorization.
