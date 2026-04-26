-- |
-- Module      : Cardano.MPFS.Client.Verify
-- Description : Offline verifiers for MPFS proof-bearing responses.
--
-- Snapshot well-formedness plus cryptographic replay for every
-- per-endpoint response envelope. Each verifier runs a structural
-- pass (hex decodes, 32-byte hashes, non-empty witnesses), then a
-- replay pass: every 'WitnessedUtxo' is cryptographically replayed
-- against @snapshot.utxo_root@ via
-- "Cardano.MPFS.Client.Verify.Replay", and every 'TrieFact' against
-- @UpdateProof.trie_root@. Error emission follows the order
-- structural → binding → root documented in
-- @contracts\/verify-error.md@.
module Cardano.MPFS.Client.Verify
    ( VerifyError (..)
    , verifyVerificationSnapshot

      -- * Per-endpoint write verifiers
    , verifyBootTxResponse
    , verifyRequestTxResponse
    , verifyRetractTxResponse
    , verifyRejectTxResponse
    , verifyEndTxResponse
    , verifyUpdateTxResponse

      -- * Per-endpoint read verifiers
    , verifyTokenResponse
    , verifyFactResponse
    , verifyProofResponse
    , verifyRequestsResponse
    ) where

import Data.ByteString qualified as BS
import Data.ByteString.Base16 qualified as Base16
import Data.Text (Text)
import Data.Text qualified as T
import Data.Text.Encoding qualified as T

import Cardano.MPFS.Client.Bundle
    ( BootProof (..)
    , BootTxResponse (..)
    , EndProof (..)
    , EndTxResponse (..)
    , RejectProof (..)
    , RejectTxResponse (..)
    , RequestProof (..)
    , RequestTxResponse (..)
    , RetractProof (..)
    , RetractTxResponse (..)
    , TrieFact (..)
    , TxIn (..)
    , UpdateProof (..)
    , UpdateTxResponse (..)
    , WitnessedUtxo (..)
    )
import Cardano.MPFS.Client.Read
    ( FactResponse (..)
    , FactWitness (..)
    , ProofResponse (..)
    , RequestsResponse (..)
    , TokenResponse (..)
    , TokenState (..)
    , WitnessedRequest (..)
    , WitnessedTokenState (..)
    )
import Cardano.MPFS.Client.Snapshot
    ( ChainPoint (..)
    , Hex (..)
    , VerificationSnapshot (..)
    )
import Cardano.MPFS.Client.Verify.Replay
    ( VerifyError (..)
    , replayTrieFact
    , replayWitnessedUtxo
    )
import Cardano.MPFS.Client.Verify.TxView
    ( verifyBootAssetBinding
    , verifyBootRedeemerBinding
    , verifyContinuingStateOutput
    , verifyEndAssetBinding
    , verifyEndRedeemerBinding
    , verifyNoMint
    , verifyNoRedeemers
    , verifyRejectRedeemerBinding
    , verifyRetractRedeemerBinding
    , verifyStateRootBinding
    , verifyTxInputBinding
    , verifyUpdateRedeemerBinding
    )

-- | Structural check for the snapshot that every proof-bearing response
-- must carry. Confirms the @utxo_root@ decodes as a 32-byte hash and
-- the chain-point block id decodes as a non-empty hash.
verifyVerificationSnapshot
    :: VerificationSnapshot -> Either VerifyError ()
verifyVerificationSnapshot VerificationSnapshot{..} = do
    checkHash32 "utxo_root" utxoRoot
    let ChainPoint{blockId} = chainpoint
    checkNonEmptyHash "chainpoint.block_id" blockId

-- | Verify a @POST \/tx\/boot@ response.
verifyBootTxResponse :: BootTxResponse -> Either VerifyError ()
verifyBootTxResponse (BootTxResponse t s (BootProof fs)) = do
    checkTxCbor "boot.tx" t
    verifyVerificationSnapshot s
    checkWitnessedUtxosStructural "boot.funding" fs
    rootBs <- decodeHex "boot.snapshot.utxo_root" (utxoRoot s)
    replayWitnessedUtxos "boot.funding" rootBs fs
    txView <- verifyTxInputBinding "boot" t (witnessInputs fs) []
    verifyBootAssetBinding "boot" txView
    verifyBootRedeemerBinding "boot" txView

-- | Verify a @POST \/tx\/request\/{insert,delete,update}@ response.
verifyRequestTxResponse :: RequestTxResponse -> Either VerifyError ()
verifyRequestTxResponse (RequestTxResponse t s (RequestProof fs)) = do
    checkTxCbor "request.tx" t
    verifyVerificationSnapshot s
    checkWitnessedUtxosStructural "request.funding" fs
    rootBs <- decodeHex "request.snapshot.utxo_root" (utxoRoot s)
    replayWitnessedUtxos "request.funding" rootBs fs
    txView <- verifyTxInputBinding "request" t (witnessInputs fs) []
    verifyNoMint "request" txView
    verifyNoRedeemers "request" txView

-- | Verify a @POST \/tx\/retract@ response.
verifyRetractTxResponse :: RetractTxResponse -> Either VerifyError ()
verifyRetractTxResponse
    (RetractTxResponse t s (RetractProof ri sr fs)) = do
        checkTxCbor "retract.tx" t
        verifyVerificationSnapshot s
        checkWitnessedUtxoStructural "retract.request_in" ri
        checkWitnessedUtxoStructural "retract.state_ref" sr
        checkWitnessedUtxosStructural "retract.funding" fs
        rootBs <-
            decodeHex "retract.snapshot.utxo_root" (utxoRoot s)
        replayWitnessedUtxo "retract.request_in" rootBs ri
        replayWitnessedUtxo "retract.state_ref" rootBs sr
        replayWitnessedUtxos "retract.funding" rootBs fs
        txView <-
            verifyTxInputBinding
                "retract"
                t
                (txIn ri : witnessInputs fs)
                [txIn sr]
        verifyNoMint "retract" txView
        verifyRetractRedeemerBinding
            "retract"
            txView
            (txIn ri)
            (txIn sr)

-- | Verify a @POST \/tx\/reject@ response.
verifyRejectTxResponse :: RejectTxResponse -> Either VerifyError ()
verifyRejectTxResponse
    (RejectTxResponse t s (RejectProof st ris fs)) = do
        checkTxCbor "reject.tx" t
        verifyVerificationSnapshot s
        checkWitnessedUtxoStructural "reject.state" st
        checkWitnessedUtxosStructural "reject.request_ins" ris
        checkWitnessedUtxosStructural "reject.funding" fs
        rootBs <-
            decodeHex "reject.snapshot.utxo_root" (utxoRoot s)
        replayWitnessedUtxo "reject.state" rootBs st
        replayWitnessedUtxos "reject.request_ins" rootBs ris
        replayWitnessedUtxos "reject.funding" rootBs fs
        txView <-
            verifyTxInputBinding
                "reject"
                t
                (txIn st : witnessInputs ris <> witnessInputs fs)
                []
        verifyContinuingStateOutput "reject" txView st
        verifyRejectRedeemerBinding
            "reject"
            txView
            (txIn st)
            (witnessInputs ris)

-- | Verify a @POST \/tx\/end@ response.
verifyEndTxResponse :: EndTxResponse -> Either VerifyError ()
verifyEndTxResponse (EndTxResponse t s (EndProof st fs)) = do
    checkTxCbor "end.tx" t
    verifyVerificationSnapshot s
    checkWitnessedUtxoStructural "end.state" st
    checkWitnessedUtxosStructural "end.funding" fs
    rootBs <- decodeHex "end.snapshot.utxo_root" (utxoRoot s)
    replayWitnessedUtxo "end.state" rootBs st
    replayWitnessedUtxos "end.funding" rootBs fs
    txView <- verifyTxInputBinding "end" t (txIn st : witnessInputs fs) []
    verifyEndAssetBinding "end" txView st
    verifyEndRedeemerBinding "end" txView (txIn st)

-- | Verify a @POST \/tx\/update@ response.
verifyUpdateTxResponse :: UpdateTxResponse -> Either VerifyError ()
verifyUpdateTxResponse
    (UpdateTxResponse t s (UpdateProof st rs fs tr tread)) = do
        checkTxCbor "update.tx" t
        verifyVerificationSnapshot s
        checkWitnessedUtxoStructural "update.state" st
        checkWitnessedUtxosStructural "update.requests" rs
        checkWitnessedUtxosStructural "update.funding" fs
        checkHash32 "update.trie_root" tr
        checkTrieFactsStructural "update.trie_read" tread
        rootBs <- decodeHex "update.snapshot.utxo_root" (utxoRoot s)
        replayWitnessedUtxo "update.state" rootBs st
        replayWitnessedUtxos "update.requests" rootBs rs
        replayWitnessedUtxos "update.funding" rootBs fs
        trieRootBs <- decodeHex "update.trie_root" tr
        replayTrieFacts "update.trie_read" trieRootBs tread
        verifyStateRootBinding "update" st tr
        txView <-
            verifyTxInputBinding
                "update"
                t
                (txIn st : witnessInputs rs <> witnessInputs fs)
                []
        verifyContinuingStateOutput "update" txView st
        verifyUpdateRedeemerBinding
            "update"
            txView
            (txIn st)
            (witnessInputs rs)
            tread

-- ---------------------------------------------------------------
-- Read endpoint verifiers
-- ---------------------------------------------------------------

-- | Verify a @GET \/tokens\/:id@ response.
--
-- Replays the state UTxO witness against the snapshot's
-- @utxo_root@. The decoded 'TokenState' inside the witness
-- (owner, trie root, tip, process and retract windows) is
-- passed through verbatim — its bytes already live inside
-- the cryptographically replayed state @tx_out@. The trie
-- root is checked structurally (32 bytes) so downstream
-- 'verifyFactResponse' and 'verifyProofResponse' calls
-- against it cannot silently malform.
verifyTokenResponse :: TokenResponse -> Either VerifyError ()
verifyTokenResponse (TokenResponse s wts) = do
    let WitnessedTokenState{utxo = stateUtxo} = wts
    verifyVerificationSnapshot s
    checkWitnessedTokenStateStructural "token.state" wts
    rootBs <- decodeHex "token.snapshot.utxo_root" (utxoRoot s)
    replayWitnessedUtxo "token.state" rootBs stateUtxo

-- | Verify a @GET \/tokens\/:id\/facts\/:key@ response.
--
-- The queried key is supplied by the caller (the URL path
-- component) and rebound to the proof at field path
-- @"fact.key"@. The verifier:
--
-- 1. runs structural checks on the snapshot, the state
--    witness, the trie root carried in the state, the value,
--    and the MPF proof;
-- 2. replays the state UTxO witness against
--    @snapshot.utxo_root@;
-- 3. replays the inclusion proof against the trie root
--    carried in the state, with the queried @key@ and
--    advertised @value@.
verifyFactResponse
    :: Hex
    -- ^ Queried key (URL path component).
    -> FactResponse
    -> Either VerifyError ()
verifyFactResponse qKey (FactResponse s v fw) = do
    let FactWitness{state = wts, mpfProof = pf} = fw
        WitnessedTokenState{utxo = stateUtxo, state = ts} = wts
        TokenState{root = trieRoot} = ts
    verifyVerificationSnapshot s
    checkWitnessedTokenStateStructural "fact.state" wts
    checkNonEmpty "fact.key" qKey
    checkNonEmpty "fact.value" v
    checkNonEmpty "fact.mpf_proof" pf
    rootBs <- decodeHex "fact.snapshot.utxo_root" (utxoRoot s)
    replayWitnessedUtxo "fact.state" rootBs stateUtxo
    trieRootBs <- decodeHex "fact.state.state.root" trieRoot
    replayTrieFact
        "fact"
        trieRootBs
        TrieFact{key = qKey, value = Just v, mpfProof = pf}

-- | Verify a @GET \/tokens\/:id\/proofs\/:key@ response.
--
-- The queried key (and an optional advertised value, when the
-- response is an inclusion claim) is supplied by the caller.
-- A 'Nothing' value means the response asserts absence of the
-- key from the trie. The verifier:
--
-- 1. runs structural checks on the snapshot, the state
--    witness, the trie root carried in the state, and the
--    MPF proof;
-- 2. replays the state UTxO witness against
--    @snapshot.utxo_root@;
-- 3. replays the inclusion or exclusion proof against the
--    trie root carried in the state.
verifyProofResponse
    :: Hex
    -- ^ Queried key (URL path component).
    -> Maybe Hex
    -- ^ Advertised value, if the caller knows the proof
    -- should witness an inclusion. Pass 'Nothing' to assert
    -- absence.
    -> ProofResponse
    -> Either VerifyError ()
verifyProofResponse qKey mValue (ProofResponse s fw) = do
    let FactWitness{state = wts, mpfProof = pf} = fw
        WitnessedTokenState{utxo = stateUtxo, state = ts} = wts
        TokenState{root = trieRoot} = ts
    verifyVerificationSnapshot s
    checkWitnessedTokenStateStructural "proof.state" wts
    checkNonEmpty "proof.key" qKey
    case mValue of
        Just v -> checkNonEmpty "proof.value" v
        Nothing -> Right ()
    checkNonEmpty "proof.mpf_proof" pf
    rootBs <- decodeHex "proof.snapshot.utxo_root" (utxoRoot s)
    replayWitnessedUtxo "proof.state" rootBs stateUtxo
    trieRootBs <- decodeHex "proof.state.state.root" trieRoot
    replayTrieFact
        "proof"
        trieRootBs
        TrieFact{key = qKey, value = mValue, mpfProof = pf}

-- | Verify a @GET \/tokens\/:id\/requests@ response.
--
-- Replays every pending-request UTxO witness against
-- @snapshot.utxo_root@. The decoded request payload is
-- opaque to the verifier — it flows through to the consumer
-- so MOOG can drive its own application logic on top of the
-- already-replayed @tx_out@ bytes.
verifyRequestsResponse
    :: RequestsResponse -> Either VerifyError ()
verifyRequestsResponse (RequestsResponse s wrs) = do
    verifyVerificationSnapshot s
    checkWitnessedRequestsStructural "requests.requests" wrs
    rootBs <- decodeHex "requests.snapshot.utxo_root" (utxoRoot s)
    replayWitnessedUtxos
        "requests.requests"
        rootBs
        (map (\WitnessedRequest{utxo = u} -> u) wrs)

-- ---------------------------------------------------------------
-- Structural pass (hex decode, 32-byte hashes, non-empty hex)
-- ---------------------------------------------------------------

checkWitnessedUtxosStructural
    :: Text -> [WitnessedUtxo] -> Either VerifyError ()
checkWitnessedUtxosStructural prefix =
    traverseIndexed prefix checkWitnessedUtxoStructural

checkWitnessedUtxoStructural
    :: Text -> WitnessedUtxo -> Either VerifyError ()
checkWitnessedUtxoStructural prefix WitnessedUtxo{..} = do
    checkTxIn (prefix <> ".tx_in") txIn
    checkNonEmpty (prefix <> ".tx_out") txOut
    checkNonEmpty (prefix <> ".utxo_proof") utxoProof

checkTxIn :: Text -> TxIn -> Either VerifyError ()
checkTxIn prefix TxIn{..} =
    checkHash32 (prefix <> ".tx_id") txId

checkWitnessedTokenStateStructural
    :: Text -> WitnessedTokenState -> Either VerifyError ()
checkWitnessedTokenStateStructural
    prefix
    WitnessedTokenState{utxo = u, state = ts} = do
        checkWitnessedUtxoStructural prefix u
        let TokenState{root = r} = ts
        checkHash32 (prefix <> ".state.root") r

checkWitnessedRequestsStructural
    :: Text -> [WitnessedRequest] -> Either VerifyError ()
checkWitnessedRequestsStructural prefix =
    traverseIndexed prefix checkWitnessedRequestStructural

checkWitnessedRequestStructural
    :: Text -> WitnessedRequest -> Either VerifyError ()
checkWitnessedRequestStructural prefix WitnessedRequest{utxo = u} =
    checkWitnessedUtxoStructural prefix u

checkTrieFactsStructural
    :: Text -> [TrieFact] -> Either VerifyError ()
checkTrieFactsStructural prefix =
    traverseIndexed prefix checkTrieFactStructural

checkTrieFactStructural
    :: Text -> TrieFact -> Either VerifyError ()
checkTrieFactStructural prefix TrieFact{..} = do
    checkNonEmpty (prefix <> ".key") key
    case value of
        Nothing -> Right ()
        Just v -> checkNonEmpty (prefix <> ".value") v
    checkNonEmpty (prefix <> ".mpf_proof") mpfProof

-- ---------------------------------------------------------------
-- Replay pass (binding + root)
-- ---------------------------------------------------------------

replayWitnessedUtxos
    :: Text
    -> BS.ByteString
    -> [WitnessedUtxo]
    -> Either VerifyError ()
replayWitnessedUtxos prefix rootBs =
    traverseIndexed prefix (`replayWitnessedUtxo` rootBs)

replayTrieFacts
    :: Text
    -> BS.ByteString
    -> [TrieFact]
    -> Either VerifyError ()
replayTrieFacts prefix rootBs =
    traverseIndexed prefix (`replayTrieFact` rootBs)

-- ---------------------------------------------------------------
-- Helpers
-- ---------------------------------------------------------------

traverseIndexed
    :: Text
    -> (Text -> a -> Either VerifyError ())
    -> [a]
    -> Either VerifyError ()
traverseIndexed prefix f = go (0 :: Int)
  where
    go _ [] = Right ()
    go i (x : xs) = do
        f (prefix <> "[" <> T.pack (show i) <> "]") x
        go (i + 1) xs

checkTxCbor :: Text -> Hex -> Either VerifyError ()
checkTxCbor field (Hex txt) =
    case Base16.decode (T.encodeUtf8 txt) of
        Right bs
            | BS.null bs -> Left (MalformedTxCbor field)
            | otherwise -> Right ()
        Left _ -> Left (MalformedTxCbor field)

checkNonEmpty :: Text -> Hex -> Either VerifyError ()
checkNonEmpty field h = do
    bs <- decodeHex field h
    if BS.null bs
        then Left (WrongHexLength field 1 0)
        else Right ()

checkHash32 :: Text -> Hex -> Either VerifyError ()
checkHash32 field h = do
    bs <- decodeHex field h
    let got = BS.length bs
    if got == 32
        then Right ()
        else Left (WrongHexLength field 32 got)

checkNonEmptyHash :: Text -> Hex -> Either VerifyError ()
checkNonEmptyHash field h = do
    bs <- decodeHex field h
    if BS.null bs then Left EmptyBlockId else Right ()

decodeHex :: Text -> Hex -> Either VerifyError BS.ByteString
decodeHex field (Hex txt) =
    case Base16.decode (T.encodeUtf8 txt) of
        Right bs -> Right bs
        Left _ -> Left (MalformedHex field txt)

witnessInputs :: [WitnessedUtxo] -> [TxIn]
witnessInputs = map txIn
