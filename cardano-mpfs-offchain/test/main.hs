module Main (main) where

import Data.IORef (newIORef)
import Test.Hspec (hspec)

import Cardano.MPFS.Mock.State
    ( mkMockCheckpoints
    , mkMockRequests
    , mkMockTokens
    )
import Cardano.MPFS.Trie.Pure (mkPureTrie)
import Cardano.MPFS.Trie.PureManager
    ( mkPureTrieManager
    )

import Cardano.MPFS.BalanceSpec qualified as BalanceSpec
import Cardano.MPFS.Indexer.CodecsSpec qualified as CodecsSpec
import Cardano.MPFS.Indexer.EventSpec qualified as CageEventSpec
import Cardano.MPFS.Indexer.FollowerSpec qualified as CageFollowerSpec
import Cardano.MPFS.Indexer.InverseSpec qualified as InverseSpec
import Cardano.MPFS.Indexer.PersistentSpec qualified as PersistentStateSpec
import Cardano.MPFS.Indexer.RollbackSpec qualified as RollbackSpec
import Cardano.MPFS.OnChainSpec qualified as OnChainSpec
import Cardano.MPFS.ProofSpec qualified as ProofSpec
import Cardano.MPFS.StateSpec qualified as StateSpec
import Cardano.MPFS.Trie.PersistentSpec qualified as PersistentSpec
import Cardano.MPFS.TrieManagerSpec qualified as TrieManagerSpec
import Cardano.MPFS.TrieSpec qualified as TrieSpec
import Cardano.MPFS.TxBuilderSpec qualified as TxBuilderSpec

main :: IO ()
main =
    PersistentSpec.withTestDB
        $ \db nodesCF kvCF metaCF -> do
            counterRef <- newIORef (0 :: Int)
            hspec $ do
                BalanceSpec.spec
                CageEventSpec.spec
                CageFollowerSpec.spec
                CodecsSpec.spec
                InverseSpec.spec
                TrieSpec.spec mkPureTrie
                TrieManagerSpec.spec mkPureTrieManager
                PersistentSpec.spec
                    db
                    nodesCF
                    kvCF
                    metaCF
                    counterRef
                StateSpec.spec
                    mkMockTokens
                    mkMockRequests
                    mkMockCheckpoints
                PersistentStateSpec.spec
                ProofSpec.spec
                OnChainSpec.spec
                RollbackSpec.spec
                TxBuilderSpec.spec
