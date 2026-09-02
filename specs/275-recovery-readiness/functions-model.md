# Functions model — 275 recovery, liveness, readiness

New and changed signatures with explicit argument names. No bodies, no
algorithms, no helpers.

## M-1 `Cardano.MPFS.HTTP.Readiness`

```haskell
evalReadiness
    :: FollowerMode      -- ^ followerMode
    -> Word64            -- ^ stabilityWindowSlots
    -> BootStage         -- ^ bootStage
    -> IndexerPhase      -- ^ indexerPhase
    -> Bool              -- ^ proofsConsistent
    -> Maybe SlotNo      -- ^ checkpointSlot
    -> Maybe SlotNo      -- ^ observedTipSlot
    -> ReadyVerdict
```

Total, pure, and free of `IO`. `indexerPhase` is ignored when `bootStage` is
`Booting`, and that must be a stated property rather than an accident of
evaluation order.

```haskell
reasonCode :: ReadyReason -> Text
```

The wire codes of D-5. Must be exhaustive over the constructor set so a new
reason cannot silently serialize as an existing one.

## M-2 `Cardano.MPFS.HTTP.Gate`

```haskell
readinessAllowlist :: [[Text]]
```

The exhaustive set of always-available path segments: `/live`, `/ready` and
`/version`. The only place the allowlist is written down.

```haskell
mkGate
    :: IO ServerPhase        -- ^ readServerPhase
    -> (Context IO -> IO ReadinessObservation)
                             -- ^ observeContext
    -> Application           -- ^ inner
    -> Application
```

Superseded by the **M-2 addendum** below, which prepends `buildInfo`. Implement
the addendum signature; this block records the rest of the argument list.

Answers `/live`, `/ready` and `/version` itself; forwards to `inner` only when
the path is outside the allowlist **and** the verdict is `Ready`; otherwise 503s
with D-8.

```haskell
observeReadiness :: Context IO -> IO ReadinessObservation
```

Gathers the live observations `evalReadiness` consumes from a published
context. This is the only place observations are gathered, so the decision
cannot be fed different inputs from different call sites.

## M-3 `Cardano.MPFS.Server.Boot`

```haskell
data ServeConfig = ServeConfig
    { serveAppConfig   :: AppConfig
    , servePort        :: Int
    , serveOnListening :: Int -> IO ()
    }
```

`serveOnListening` receives the actually bound port and is invoked after the
socket is bound and before recovery starts. Production passes a no-op; the
recovery proof uses it to learn an ephemeral port and to assert ordering.

```haskell
runServer :: ServeConfig -> IO ()
```

Binds, serves the gate, runs the application concurrently, publishes the
context, and blocks until the listener or the application terminates.
Propagates any boot or linked-thread failure to its caller (INV-R7); it must
not catch and continue.

```haskell
withServer :: ServeConfig -> (ServerHandle -> IO a) -> IO a
```

Bracketed form used by the recovery proof: runs the same sequence as
`runServer` and hands back the bound port, so no test may construct its own
boot sequence (INV-R10).

## M-4 `Cardano.MPFS.HTTP.Server`

```haskell
mkApp :: Context IO -> Application
```

Unchanged signature. Its result becomes the `inner` argument of `mkGate`.

## M-7 `Cardano.MPFS.Context`

Three added fields on the existing record:

```haskell
    , indexerPhase         :: m IndexerPhase
    , stabilityWindowSlots :: Word64
    , followerMode         :: FollowerMode
```

`indexerProofsReady`, `state`, and `readMetrics` are unchanged and supply the
remaining observations.

## M-9 `exe/Serve.hs`

```haskell
main :: IO ()
```

Unchanged signature; its body may parse arguments, validate, load the
blueprint, create the database directory, assemble `ServeConfig`, and call
`runServer`. Any other sequencing there violates INV-R10.

## M-12 test surface

```haskell
spec :: Spec
```

## M-13 test surface

```haskell
spec :: Spec
```

```haskell
probeStatus :: Int -> Text -> IO Int
```

Issues one real HTTP request over TCP to the bound port and returns the status
code. Must surface a connection failure as a distinct, loud outcome rather
than as a status code, so "connect refused" can never be recorded as a pass.

```haskell
holdingReplay :: IO a -> (Tracer IO AppTrace -> IO () -> IO a) -> IO a
```

Supplies a tracer that blocks the replaying thread at `ReplayStart` and an
action that releases it. Built on the existing application tracer seam; no
production code exists solely to support it.

## M-14 `Cardano.MPFS.BuildInfo`

```haskell
data BuildInfo = BuildInfo
    { buildVersion     :: Text
    , buildGitCommit   :: Text
    , buildImageDigest :: Maybe Text
    }
```

```haskell
loadBuildInfo :: IO BuildInfo
```

Called once during startup, before the listener and recovery sequence. Reads
the optional `MPFS_IMAGE_DIGEST`. Must not be called per request and must not
be reached through `unsafePerformIO`.

```haskell
isCleanSourceCommit :: Text -> Bool
isImmutableImageDigest :: Text -> Bool
```

Pure predicates, exported for M2-E-PUBLISH's publication gate. `isCleanSourceCommit`
returns `False` for every development sentinel.

## M-2 addendum

```haskell
mkGate
    :: BuildInfo             -- ^ buildInfo
    -> IO ServerPhase        -- ^ readServerPhase
    -> (Context IO -> IO ReadinessObservation)
                             -- ^ observeContext
    -> Application           -- ^ inner
    -> Application
```

`buildInfo` is supplied at construction, which is what makes the
capture-once property structural rather than a convention.

## M-3 addendum

```haskell
data ServeConfig = ServeConfig
    { serveAppConfig   :: AppConfig
    , servePort        :: Int
    , serveBuildInfo   :: BuildInfo
    , serveOnListening :: Int -> IO ()
    }
```
