-- | Mock `CageHelpers` implementation.
-- |
-- | Returns plausible, per-operation-distinct stub `UnsignedTxCbor` values
-- | after a short simulated delay so the UI exercises its loading/result
-- | states realistically. This lets the entire SPA be built and demoed before
-- | the real WASM cage-helper bridge (%351) lands; swapping to the real
-- | implementation touches only `Main` (the wiring), not any call site.
-- |
-- | The stub CBOR is NOT a valid transaction — it is a recognisable marker
-- | (`deadbeef` + a per-operation tag). No protocol logic lives here.
module MpfsSpa.CageHelpers.Mock
  ( mockCageHelpers
  ) where

import Prelude

import Data.Either (Either(..))
import Effect.Aff (Milliseconds(..), delay)

import MpfsSpa.CageHelpers (CageHelpers, CageResult)
import MpfsSpa.Types (UnsignedTxCbor(..))

-- | A mock that ignores its inputs and yields a marked stub transaction.
mockCageHelpers :: CageHelpers
mockCageHelpers =
  { registerToken: \_ _ -> stub "01"
  , insertFact: \_ _ _ _ _ -> stub "02"
  , updateFact: \_ _ _ _ _ _ -> stub "03"
  , deleteFact: \_ _ _ _ _ -> stub "04"
  , retractRequest: \_ _ _ _ -> stub "05"
  , rejectExpired: \_ _ _ -> stub "06"
  , endCage: \_ _ _ -> stub "07"
  }

-- | Produce a stub result after a simulated round-trip. The per-operation
-- | tag keeps each operation's stub distinct so the UI can show something.
stub :: String -> CageResult
stub tag = do
  delay (Milliseconds 200.0)
  pure (Right (UnsignedTxCbor ("deadbeef" <> tag)))
