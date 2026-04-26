# Quickstart: Typed HTTP wrappers for MOOG

Run the client unit tests:

```bash
nix develop --quiet -c cabal test cardano-mpfs-client:unit-tests -O0 --test-show-details=direct
```

Expected MOOG-facing usage shape:

```haskell
import Cardano.MPFS.Client
import Network.HTTP.Client (newManager, defaultManagerSettings)

main :: IO ()
main = do
    manager <- newManager defaultManagerSettings
    let client =
            MpfsHttp
                { manager = manager
                , baseUrl = BaseUrl Http "127.0.0.1" 8080 ""
                , verifier = RunVerifier
                }
    result <- bootTx client (BootTxParams someAddressHex)
    case result of
        Left err -> print err
        Right response -> print (tx response)
```

Browser, WASM, WASI, and npm packaging are intentionally not part of
this milestone. They are tracked by the
`WASM/WASI MPFS API Client` milestone.
