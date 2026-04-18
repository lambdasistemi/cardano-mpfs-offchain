{-# LANGUAGE DataKinds #-}

-- |
-- Module      : Cardano.MPFS.Core.Types
-- Description : Core domain types for the MPFS offchain service
-- License     : Apache-2.0
--
-- Central type vocabulary for the MPFS offchain service.
-- Re-exports ledger types (Conway era) and defines domain
-- types that bridge the gap between cardano-ledger
-- representations and the Aiken on-chain validator layout.
--
-- Every module in the @Cardano.MPFS@ hierarchy imports
-- from here rather than reaching into @cardano-ledger-*@
-- directly, ensuring a single point of control for era
-- transitions and type aliases.
module Cardano.MPFS.Core.Types
    ( -- * Ledger re-exports
      ConwayEra
    , Addr
    , TxId
    , TxIn
    , TxOut
    , Coin (..)
    , MaryValue
    , PolicyID (..)
    , AssetName (..)
    , SlotNo (..)
    , ScriptHash (..)
    , KeyHash
    , KeyRole (..)
    , ExUnits (..)
    , PParams

      -- * Token identification
    , TokenId (..)

      -- * Merkle Patricia Forestry
    , Root (..)
    , Operation (..)

      -- * Requests
    , Request (..)
    , LocatedRequest (..)

      -- * Token state
    , TokenState (..)
    , LocatedTokenState (..)

      -- * Facts
    , Fact (..)

      -- * Chain position
    , BlockId (..)
    ) where

import Data.ByteString (ByteString)

import Cardano.Ledger.Address (Addr)
import Cardano.Ledger.Api.Tx.Out (TxOut)
import Cardano.Ledger.Coin (Coin (..))
import Cardano.Ledger.Conway (ConwayEra)
import Cardano.Ledger.Core (PParams)
import Cardano.Ledger.Hashes (ScriptHash (..))
import Cardano.Ledger.Keys (KeyHash, KeyRole (..))
import Cardano.Ledger.Mary.Value
    ( AssetName (..)
    , MaryValue
    , PolicyID (..)
    )
import Cardano.Ledger.Plutus.ExUnits (ExUnits (..))
import Cardano.Ledger.Slot (SlotNo (..))
import Cardano.Ledger.TxIn (TxId, TxIn)

-- | Unique identifier for a token managed by the
-- MPFS service. Corresponds to the on-chain
-- asset name derived from SHA2-256(txId ++ index).
newtype TokenId = TokenId
    { unTokenId :: AssetName
    }
    deriving (Eq, Ord, Show)

-- | MPF root hash representing the current state
-- of a trie (32 bytes Blake2b-256).
newtype Root = Root
    { unRoot :: ByteString
    }
    deriving (Eq, Show)

-- | An operation to perform on a key in the trie.
data Operation
    = -- | Insert a new key-value pair
      Insert
        !ByteString
        -- ^ Value to insert
    | -- | Delete a key
      Delete
        !ByteString
        -- ^ Old value being deleted (needed for proof)
    | -- | Update an existing key with a new value
      Update
        !ByteString
        -- ^ Old value being replaced
        !ByteString
        -- ^ New value to store
    deriving (Eq, Show)

-- | A pending request together with the reference
-- of the UTxO currently carrying it on-chain. The
-- UTxO body ('TxOut') is recoverable via a
-- 'resolveUtxo' hook.
data LocatedRequest = LocatedRequest
    { requestRef :: !TxIn
    -- ^ Reference of the UTxO carrying this request
    , request :: !Request
    -- ^ The request payload
    }
    deriving (Eq, Show)

-- | A request to modify a token's trie.
data Request = Request
    { requestToken :: !TokenId
    -- ^ The token whose trie is being modified
    , requestOwner :: !(KeyHash 'Payment)
    -- ^ The owner's payment key hash
    , requestKey :: !ByteString
    -- ^ The key to operate on
    , requestValue :: !Operation
    -- ^ The operation to perform
    , requestFee :: !Coin
    -- ^ Fee the requester agrees to pay
    , requestSubmittedAt :: !Integer
    -- ^ POSIXTime (ms) when the request was created
    }
    deriving (Eq, Show)

-- | On-chain token state together with the
-- reference of the UTxO currently carrying it.
-- The UTxO body ('TxOut') is recoverable via a
-- 'resolveUtxo' hook.
data LocatedTokenState = LocatedTokenState
    { tokenStateRef :: !TxIn
    -- ^ Reference of the UTxO holding this state
    , tokenState :: !TokenState
    -- ^ The state payload
    }
    deriving (Eq, Show)

-- | Current on-chain state of a token.
data TokenState = TokenState
    { owner :: !(KeyHash 'Payment)
    -- ^ Owner's payment key hash
    , root :: !Root
    -- ^ Current root hash of the token's trie
    , tip :: !Coin
    -- ^ Maximum fee charged per request
    , processTime :: !Integer
    -- ^ Duration (ms) of the oracle processing window
    , retractTime :: !Integer
    -- ^ Duration (ms) of the requester retract window
    }
    deriving (Eq, Show)

-- | A key-value fact stored in a trie.
data Fact = Fact
    { key :: !ByteString
    -- ^ The fact's key
    , value :: !ByteString
    -- ^ The fact's value
    }
    deriving (Eq, Show)

-- | Block identifier (header hash).
newtype BlockId = BlockId
    { unBlockId :: ByteString
    }
    deriving (Eq, Show)
