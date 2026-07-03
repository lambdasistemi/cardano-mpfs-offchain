-- |
-- Module      : Cardano.MPFS.Client.KERI.Ed25519
-- Description : Pure Ed25519 verify for KERI AID signature checking.
--
-- Thin wrapper over 'cardano-crypto-class' Ed25519DSIGN, which uses
-- the pure 'crypton' path (no libsodium FFI) when compiled with
-- @-external-libsodium-vrf@ disabled. Compiles unchanged to
-- wasm32-wasi and GHC-JS.
module Cardano.MPFS.Client.KERI.Ed25519
    ( verifyEd25519
    ) where

import Data.ByteString (ByteString)

import Cardano.Crypto.DSIGN
    ( SigDSIGN
    , VerKeyDSIGN
    , rawDeserialiseSigDSIGN
    , rawDeserialiseVerKeyDSIGN
    , verifyDSIGN
    )
import Cardano.Crypto.DSIGN.Ed25519 (Ed25519DSIGN)

-- | Verify a raw Ed25519 signature.
-- Returns 'True' iff the 32-byte public key authenticates the 64-byte
-- signature over the message.  Malformed key or signature bytes give 'False'.
verifyEd25519
    :: ByteString
    -- ^ 32-byte Ed25519 public key
    -> ByteString
    -- ^ signed message
    -> ByteString
    -- ^ 64-byte Ed25519 signature
    -> Bool
verifyEd25519 pubKeyBs msg sigBs =
    case ( rawDeserialiseVerKeyDSIGN pubKeyBs :: Maybe (VerKeyDSIGN Ed25519DSIGN)
         , rawDeserialiseSigDSIGN sigBs :: Maybe (SigDSIGN Ed25519DSIGN)
         ) of
        (Just pk, Just sig) ->
            case verifyDSIGN () pk msg sig of
                Right () -> True
                Left _ -> False
        _ -> False
