-- |
-- Module      : Cardano.MPFS.CLI.Options
-- Description : optparse-applicative command surface for mpfs-cli.
--
-- Defines the nine subcommands and their argument parsers. Every
-- subcommand and option carries @--help@ text. Hex arguments are
-- validated at parse time via "Cardano.MPFS.CLI.Hex".
module Cardano.MPFS.CLI.Options
    ( Command (..)
    , commandInfo
    ) where

import Cardano.MPFS.CLI.Hex (HexArg, hexReader)
import Data.Text (Text)
import Options.Applicative

-- | Every mpfs-cli subcommand, fully parsed.
data Command
    = -- | @register-token@ — boot a new token/cage for the owner key.
      RegisterToken
        { server :: String
        , ownerKey :: FilePath
        , cageConfig :: Maybe FilePath
        , trustedRoot :: Maybe HexArg
        }
    | -- | @fact insert@ — request insertion of a new key/value fact.
      FactInsert
        { server :: String
        , token :: Text
        , key :: HexArg
        , value :: HexArg
        , ownerKey :: FilePath
        }
    | -- | @fact update@ — request a change of an existing fact value.
      FactUpdate
        { server :: String
        , token :: Text
        , key :: HexArg
        , oldValue :: HexArg
        , newValue :: HexArg
        , ownerKey :: FilePath
        }
    | -- | @fact delete@ — request deletion of an existing fact.
      FactDelete
        { server :: String
        , token :: Text
        , key :: HexArg
        , ownerKey :: FilePath
        }
    | -- | @fact retract@ — retract a pending request by id.
      FactRetract
        { server :: String
        , token :: Text
        , requestId :: Text
        , ownerKey :: FilePath
        }
    | -- | @fact reject@ — reject pending requests past their deadline.
      FactReject
        { server :: String
        , token :: Text
        , ownerKey :: FilePath
        }
    | -- | @fact get@ — read-only fact lookup with proof.
      FactGet
        { server :: String
        , token :: Text
        , key :: HexArg
        }
    | -- | @token end@ — close out a token/cage.
      TokenEnd
        { server :: String
        , token :: Text
        , ownerKey :: FilePath
        }
    | -- | @token list@ — read-only listing of known token ids.
      TokenList
        {server :: String}
    deriving stock (Show)

-- | Top-level parser with @--help@ wiring.
commandInfo :: ParserInfo Command
commandInfo =
    info
        (commandsP <**> helper)
        ( fullDesc
            <> header "mpfs-cli - command-line front-end for the MPFS server"
            <> progDesc
                "Register tokens and manage facts end-to-end against an \
                \MPFS server using a local Bech32 .skey. JSON is written \
                \to stdout; logs go to stderr."
        )

commandsP :: Parser Command
commandsP =
    hsubparser
        ( command
            "register-token"
            ( info
                registerTokenP
                (progDesc "Register (boot) a new token/cage for the owner key")
            )
            <> command
                "fact"
                ( info
                    (hsubparser factCommands)
                    (progDesc "Manage facts (insert/update/delete/retract/reject/get)")
                )
            <> command
                "token"
                ( info
                    (hsubparser tokenCommands)
                    (progDesc "Manage tokens (end/list)")
                )
        )

factCommands :: Mod CommandFields Command
factCommands =
    command
        "insert"
        ( info
            factInsertP
            (progDesc "Request insertion of a new key/value fact")
        )
        <> command
            "update"
            ( info
                factUpdateP
                (progDesc "Request a change of an existing fact value")
            )
        <> command
            "delete"
            (info factDeleteP (progDesc "Request deletion of an existing fact"))
        <> command
            "retract"
            (info factRetractP (progDesc "Retract a pending request by id"))
        <> command
            "reject"
            ( info
                factRejectP
                (progDesc "Reject pending requests past their deadline")
            )
        <> command
            "get"
            (info factGetP (progDesc "Read-only: look up a fact with proof"))

tokenCommands :: Mod CommandFields Command
tokenCommands =
    command
        "end"
        (info tokenEndP (progDesc "Close out a token/cage"))
        <> command
            "list"
            (info tokenListP (progDesc "Read-only: list known token ids"))

-- Shared options ------------------------------------------------------

serverP :: Parser String
serverP =
    strOption
        ( long "server"
            <> metavar "URL"
            <> help "MPFS server base URL (e.g. http://localhost:3000)"
        )

ownerKeyP :: Parser FilePath
ownerKeyP =
    strOption
        ( long "owner-key"
            <> metavar "KEYFILE"
            <> help "Path to a Bech32-encoded .skey file"
        )

tokenP :: Parser Text
tokenP =
    strOption
        (long "token" <> metavar "TOKEN" <> help "Target token id (hex)")

keyP :: Parser HexArg
keyP =
    option
        hexReader
        (long "key" <> metavar "HEX" <> help "Fact key (hex)")

valueP :: Parser HexArg
valueP =
    option
        hexReader
        (long "value" <> metavar "HEX" <> help "Fact value (hex)")

-- Per-command parsers --------------------------------------------------

registerTokenP :: Parser Command
registerTokenP =
    RegisterToken
        <$> serverP
        <*> ownerKeyP
        <*> cageConfigP
        <*> trustedRootP

-- | @--cage-config FILE@: the cage blueprint JSON. Optional; defaults to
-- @$MPFS_BLUEPRINT@. One of the two must be set for write commands.
cageConfigP :: Parser (Maybe FilePath)
cageConfigP =
    optional
        ( strOption
            ( long "cage-config"
                <> metavar "FILE"
                <> help
                    "Cage blueprint JSON. Optional; defaults to \
                    \$MPFS_BLUEPRINT."
            )
        )

-- | @--trusted-root HEX@: independently-obtained UTxO-CSMT root.
-- Optional; without it the CLI trusts the server's /status root.
trustedRootP :: Parser (Maybe HexArg)
trustedRootP =
    optional
        ( option
            hexReader
            ( long "trusted-root"
                <> metavar "HEX"
                <> help
                    "Trusted UTxO root (hex). Optional; defaults to the \
                    \server's /status root."
            )
        )

factInsertP :: Parser Command
factInsertP =
    FactInsert <$> serverP <*> tokenP <*> keyP <*> valueP <*> ownerKeyP

factUpdateP :: Parser Command
factUpdateP =
    FactUpdate
        <$> serverP
        <*> tokenP
        <*> keyP
        <*> option
            hexReader
            (long "old-value" <> metavar "HEX" <> help "Current fact value (hex)")
        <*> option
            hexReader
            ( long "new-value"
                <> metavar "HEX"
                <> help "Replacement fact value (hex)"
            )
        <*> ownerKeyP

factDeleteP :: Parser Command
factDeleteP =
    FactDelete <$> serverP <*> tokenP <*> keyP <*> ownerKeyP

factRetractP :: Parser Command
factRetractP =
    FactRetract
        <$> serverP
        <*> tokenP
        <*> strOption
            ( long "request-id"
                <> metavar "REQ_ID"
                <> help "Identifier of the pending request to retract"
            )
        <*> ownerKeyP

factRejectP :: Parser Command
factRejectP =
    FactReject <$> serverP <*> tokenP <*> ownerKeyP

factGetP :: Parser Command
factGetP =
    FactGet <$> serverP <*> tokenP <*> keyP

tokenEndP :: Parser Command
tokenEndP =
    TokenEnd <$> serverP <*> tokenP <*> ownerKeyP

tokenListP :: Parser Command
tokenListP =
    TokenList <$> serverP
