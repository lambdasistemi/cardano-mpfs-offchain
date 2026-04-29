-- |
-- Module      : Cardano.MPFS.Client.Read
-- Description : Typed read-side response DTOs for the post-split API.
--
-- Carries the JSON shapes returned by @GET \/status@,
-- @GET \/tokens@, @GET \/tokens\/:id@,
-- @GET \/tokens\/:id\/facts\/:key@, and
-- @GET \/tx\/:txId@ under the redesign of feature #243.
--
-- Populated incrementally by tasks T030, T032, T071, T086 — see
-- @specs\/243-proof-redesign\/tasks.md@.
module Cardano.MPFS.Client.Read
    (
    ) where
