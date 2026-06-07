-- | Runtime configuration for the preprod MPFS SPA.
module MpfsSpa.Config
  ( serverConfig
  , placeholderCageConfig
  , preprodCageConfig
  , walletPolicyJson
  ) where

import Prelude

import Data.Argonaut.Core (Json, fromNumber, fromObject)
import Data.String.Common as String
import Data.Tuple (Tuple(..))
import Effect (Effect)
import Foreign.Object as Object

import MpfsSpa.Http (Config)
import MpfsSpa.Types (CageConfig)

-- | The preprod MPFS server. `window.MPFS_BASE_URL` can override this for
-- | previews or local servers.
defaultMpfsBaseUrl :: String
defaultMpfsBaseUrl = "https://umpfs.plutimus.com"

-- | Read the server base URL from the page; defaults to the preprod server.
serverConfig :: Effect Config
serverConfig = do
  base <- _baseUrl
  pure
    { baseUrl:
        if String.trim base == "" then defaultMpfsBaseUrl
        else base
    }

foreign import _baseUrl :: Effect String

-- | Backward-compatible name used by the existing tab code.
placeholderCageConfig :: CageConfig
placeholderCageConfig = preprodCageConfig

-- | Preprod cage config. The Nix SPA build injects validator identity from
-- | the pinned cardano-mpfs-onchain blueprint, which is the canonical source
-- | for the cage/request validator bytes and cage script hash.
preprodCageConfig :: CageConfig
preprodCageConfig =
  { cageScriptBytes: preprodCageScriptBytes
  , requestScriptBytes: preprodRequestScriptBytes
  , cfgScriptHash: "__MPFS_CAGE_SCRIPT_HASH__"
  -- 30-minute process/retract windows (vs the 5-min default) so an owner
  -- has time to fold/reject requests by hand in the browser. Baked into a
  -- token's state datum at boot; only affects tokens newly registered here.
  , defaultProcessTime: 1800000
  , defaultRetractTime: 1800000
  , defaultTip: 2000000
  , network: "preprod"
  }

-- | Permissive wallet policy caps mirroring the Haskell tests' defaults.
walletPolicyJson :: Json
walletPolicyJson =
  obj
    [ Tuple "max_fee" (fromNumber 10000000.0)
    , Tuple "max_min_utxo_coin_per_byte" (fromNumber 10000.0)
    , Tuple "max_ex_unit_prices"
        ( obj
            [ Tuple "price_memory" (fromNumber 1000000000000.0)
            , Tuple "price_steps" (fromNumber 1000000000000.0)
            , Tuple "pr_mem" (fromNumber 1000000000000.0)
            , Tuple "pr_steps" (fromNumber 1000000000000.0)
            ]
        )
    ]

-- Filled by the pinned blueprint during the Nix SPA build. Kept separate so
-- the long byte strings are isolated from the operational config.
preprodRequestScriptBytes :: String
preprodRequestScriptBytes =
  "__MPFS_REQUEST_SCRIPT_BYTES__"

preprodCageScriptBytes :: String
preprodCageScriptBytes =
  "__MPFS_CAGE_SCRIPT_BYTES__"

obj :: Array (Tuple String Json) -> Json
obj = fromObject <<< Object.fromFoldable
