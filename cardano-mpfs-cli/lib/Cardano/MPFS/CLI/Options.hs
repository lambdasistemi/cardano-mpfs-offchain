-- |
-- Module      : Cardano.MPFS.CLI.Options
-- Description : optparse-applicative command surface for mpfs-cli.
--
-- Defines the command surface and validates hex arguments at parse time.
-- Network, blueprint, and signing-key inputs may be passed explicitly or
-- resolved from the documented environment variables by the runner.
module Cardano.MPFS.CLI.Options
    ( App (..)
    , Command (..)
    , OutputFormat (..)
    , commandInfo
    ) where

import Cardano.MPFS.CLI.Hex (HexArg, hexReader)
import Data.Text (Text)
import Options.Applicative

-- | Complete parsed invocation.
data App = App
    { appOutput :: OutputFormat
    , appCommand :: Command
    }
    deriving stock (Show)

-- | Output mode selected by @--json@.
data OutputFormat
    = OutputHuman
    | OutputJson
    deriving stock (Eq, Show)

-- | Every mpfs-cli subcommand, fully parsed.
data Command
    = -- | @register-token@ -- boot a new token/cage for the owner key.
      RegisterToken
        { server :: Maybe String
        , ownerKey :: Maybe FilePath
        , cageConfig :: Maybe FilePath
        , processTimeMs :: Maybe Integer
        , retractTimeMs :: Maybe Integer
        , trustedRoot :: Maybe HexArg
        }
    | -- | @fact insert@ -- requester asks to insert a key/value fact.
      FactInsert
        { server :: Maybe String
        , token :: Text
        , key :: HexArg
        , value :: HexArg
        , ownerKey :: Maybe FilePath
        , cageConfig :: Maybe FilePath
        , trustedRoot :: Maybe HexArg
        }
    | -- | @fact update@ -- requester asks to change a fact value.
      FactUpdate
        { server :: Maybe String
        , token :: Text
        , key :: HexArg
        , oldValue :: HexArg
        , newValue :: HexArg
        , ownerKey :: Maybe FilePath
        , cageConfig :: Maybe FilePath
        , trustedRoot :: Maybe HexArg
        }
    | -- | @fact delete@ -- requester asks to delete a fact.
      FactDelete
        { server :: Maybe String
        , token :: Text
        , key :: HexArg
        , value :: HexArg
        , ownerKey :: Maybe FilePath
        , cageConfig :: Maybe FilePath
        , trustedRoot :: Maybe HexArg
        }
    | -- | @fact retract@ -- requester retracts a pending request.
      FactRetract
        { server :: Maybe String
        , token :: Text
        , requestId :: Text
        , ownerKey :: Maybe FilePath
        , cageConfig :: Maybe FilePath
        , trustedRoot :: Maybe HexArg
        }
    | -- | @fact reject@ -- owner rejects expired pending requests.
      FactReject
        { server :: Maybe String
        , token :: Text
        , requestIds :: [Text]
        , ownerKey :: Maybe FilePath
        , cageConfig :: Maybe FilePath
        , trustedRoot :: Maybe HexArg
        }
    | -- | @fact list TOKEN@ -- read-only complete fact enumeration.
      FactList
        { server :: Maybe String
        , token :: Text
        , trustedRoot :: Maybe HexArg
        }
    | -- | @fact get TOKEN KEY@ -- read-only present/absent fact lookup.
      FactGet
        { server :: Maybe String
        , token :: Text
        , key :: HexArg
        , trustedRoot :: Maybe HexArg
        }
    | -- | @token process TOKEN@ -- owner folds pending requests.
      TokenProcess
        { server :: Maybe String
        , token :: Text
        , requestIds :: [Text]
        , ownerKey :: Maybe FilePath
        , cageConfig :: Maybe FilePath
        , trustedRoot :: Maybe HexArg
        }
    | -- | @token end@ -- owner closes out a token/cage.
      TokenEnd
        { server :: Maybe String
        , token :: Text
        , ownerKey :: Maybe FilePath
        , cageConfig :: Maybe FilePath
        , trustedRoot :: Maybe HexArg
        }
    | -- | @token list@ -- read-only verified token-id listing.
      TokenList
        { server :: Maybe String
        , cageConfig :: Maybe FilePath
        , trustedRoot :: Maybe HexArg
        }
    | -- | @token get TOKEN@ -- read-only verified token state.
      TokenGet
        { server :: Maybe String
        , token :: Text
        , trustedRoot :: Maybe HexArg
        }
    | -- | @requests list TOKEN@ -- read-only verified pending requests.
      RequestsList
        { server :: Maybe String
        , token :: Text
        , cageConfig :: Maybe FilePath
        , trustedRoot :: Maybe HexArg
        }
    deriving stock (Show)

-- | Top-level parser with @--help@ wiring.
commandInfo :: ParserInfo App
commandInfo =
    info
        ((combineOutput <$> outputFlag <*> commandsP) <**> helper)
        ( fullDesc
            <> header "mpfs-cli - command-line front-end for the MPFS server"
            <> progDesc
                "Register tokens, request and process facts, and run \
                \verified read-only queries against an MPFS server. Human \
                \output is the default; pass --json for machine output."
            <> footer
                "Environment: MPFS_SERVER defaults --server; \
                \MPFS_BLUEPRINT defaults --cage-config; \
                \MPFS_SIGNER_WALLET defaults --owner-key and is a PATH to a \
                \Bech32 ed25519 signing key."
        )
  where
    combineOutput topJson (localJson, cmd) =
        App
            { appOutput =
                if topJson || localJson
                    then OutputJson
                    else OutputHuman
            , appCommand = cmd
            }

commandsP :: Parser (Bool, Command)
commandsP =
    hsubparser
        ( command
            "register-token"
            ( info
                (withOutput registerTokenP)
                (progDesc "Owner: register (boot) a new token/cage")
            )
            <> command
                "fact"
                ( info
                    (hsubparser factCommands)
                    ( progDesc
                        "Requester commands and verified fact queries"
                    )
                )
            <> command
                "token"
                ( info
                    (hsubparser tokenCommands)
                    (progDesc "Owner commands and verified token queries")
                )
            <> command
                "requests"
                ( info
                    (hsubparser requestCommands)
                    (progDesc "Verified pending-request queries")
                )
        )

factCommands :: Mod CommandFields (Bool, Command)
factCommands =
    command
        "insert"
        ( info
            (withOutput factInsertP)
            (progDesc "Requester: ask to insert a new key/value fact")
        )
        <> command
            "update"
            ( info
                (withOutput factUpdateP)
                (progDesc "Requester: ask to change an existing fact value")
            )
        <> command
            "delete"
            ( info
                (withOutput factDeleteP)
                (progDesc "Requester: ask to delete an existing fact")
            )
        <> command
            "retract"
            ( info
                (withOutput factRetractP)
                (progDesc "Requester: retract a pending request by id")
            )
        <> command
            "reject"
            ( info
                (withOutput factRejectP)
                (progDesc "Owner: reject pending requests past their deadline")
            )
        <> command
            "list"
            ( info
                (withOutput factListP)
                (progDesc "Read-only: enumerate all facts for a token")
            )
        <> command
            "get"
            ( info
                (withOutput factGetP)
                (progDesc "Read-only: look up a fact with proof")
            )

tokenCommands :: Mod CommandFields (Bool, Command)
tokenCommands =
    command
        "process"
        ( info
            (withOutput tokenProcessP)
            (progDesc "Owner: fold pending requests into the token trie")
        )
        <> command
            "end"
            (info (withOutput tokenEndP) (progDesc "Owner: close out a token"))
        <> command
            "list"
            ( info
                (withOutput tokenListP)
                (progDesc "Read-only: list known token ids with proof")
            )
        <> command
            "get"
            ( info
                (withOutput tokenGetP)
                (progDesc "Read-only: get token state with proof")
            )

requestCommands :: Mod CommandFields (Bool, Command)
requestCommands =
    command
        "list"
        ( info
            (withOutput requestsListP)
            ( progDesc
                "Read-only: list pending request ids and deadlines with proof"
            )
        )

withOutput :: Parser Command -> Parser (Bool, Command)
withOutput p = (,) <$> outputFlag <*> p

-- Shared options ------------------------------------------------------

outputFlag :: Parser Bool
outputFlag =
    switch
        ( long "json"
            <> help "Write a verified JSON object instead of human output"
        )

serverP :: Parser (Maybe String)
serverP =
    optional
        ( strOption
            ( long "server"
                <> metavar "URL"
                <> help
                    "MPFS server base URL. Optional; defaults to MPFS_SERVER."
            )
        )

ownerKeyP :: Parser (Maybe FilePath)
ownerKeyP =
    optional
        ( strOption
            ( long "owner-key"
                <> metavar "KEYFILE"
                <> help
                    "Path to a Bech32 ed25519 .skey file. Optional; defaults \
                    \to MPFS_SIGNER_WALLET."
            )
        )

tokenOptionP :: Parser Text
tokenOptionP =
    strOption
        (long "token" <> metavar "TOKEN" <> help "Target token id (hex)")

tokenArgP :: Parser Text
tokenArgP =
    argument str (metavar "TOKEN" <> help "Target token id (hex)")

tokenArgOrOptionP :: Parser Text
tokenArgOrOptionP = tokenArgP <|> tokenOptionP

keyOptionP :: Parser HexArg
keyOptionP =
    option
        hexReader
        (long "key" <> metavar "HEX" <> help "Fact key (hex)")

keyArgP :: Parser HexArg
keyArgP =
    argument hexReader (metavar "KEY" <> help "Fact key (hex)")

keyArgOrOptionP :: Parser HexArg
keyArgOrOptionP = keyArgP <|> keyOptionP

valueP :: Parser HexArg
valueP =
    option
        hexReader
        (long "value" <> metavar "HEX" <> help "Fact value (hex)")

-- | @--cage-config FILE@: the cage blueprint JSON. Optional; defaults to
-- @$MPFS_BLUEPRINT@. It is required for write commands, @token list@,
-- and @requests list@ because those commands derive verifier addresses
-- locally.
cageConfigP :: Parser (Maybe FilePath)
cageConfigP =
    optional
        ( strOption
            ( long "cage-config"
                <> metavar "FILE"
                <> help
                    "Cage blueprint JSON. Optional; defaults to MPFS_BLUEPRINT."
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

requestIdP :: Parser Text
requestIdP =
    strOption
        ( long "request-id"
            <> metavar "TXHASH#IX"
            <> help "Pending request UTxO reference"
        )

processRequestIdsP :: Parser [Text]
processRequestIdsP =
    many
        ( strOption
            ( long "request-id"
                <> metavar "TXHASH#IX"
                <> help
                    "Only process this pending request. Repeat to select a \
                    \batch; omit to process all pending requests."
            )
        )

rejectRequestIdsP :: Parser [Text]
rejectRequestIdsP =
    many
        ( strOption
            ( long "request-id"
                <> metavar "TXHASH#IX"
                <> help
                    "Only reject this pending request. Repeat to select a \
                    \batch; omit to reject all expired pending requests."
            )
        )

-- Per-command parsers --------------------------------------------------

registerTokenP :: Parser Command
registerTokenP =
    RegisterToken
        <$> serverP
        <*> ownerKeyP
        <*> cageConfigP
        <*> processTimeP
        <*> retractTimeP
        <*> trustedRootP

processTimeP :: Parser (Maybe Integer)
processTimeP =
    optional
        ( option
            auto
            ( long "process-time-ms"
                <> metavar "MS"
                <> help
                    "Token request processing window in milliseconds. \
                    \Optional; defaults to the CLI devnet/testnet profile."
            )
        )

retractTimeP :: Parser (Maybe Integer)
retractTimeP =
    optional
        ( option
            auto
            ( long "retract-time-ms"
                <> metavar "MS"
                <> help
                    "Token request retract window in milliseconds. Optional; \
                    \defaults to the CLI devnet/testnet profile."
            )
        )

factInsertP :: Parser Command
factInsertP =
    FactInsert
        <$> serverP
        <*> tokenOptionP
        <*> keyOptionP
        <*> valueP
        <*> ownerKeyP
        <*> cageConfigP
        <*> trustedRootP

factUpdateP :: Parser Command
factUpdateP =
    FactUpdate
        <$> serverP
        <*> tokenOptionP
        <*> keyOptionP
        <*> option
            hexReader
            (long "old-value" <> metavar "HEX" <> help "Current fact value")
        <*> option
            hexReader
            (long "new-value" <> metavar "HEX" <> help "Replacement value")
        <*> ownerKeyP
        <*> cageConfigP
        <*> trustedRootP

factDeleteP :: Parser Command
factDeleteP =
    FactDelete
        <$> serverP
        <*> tokenOptionP
        <*> keyOptionP
        <*> valueP
        <*> ownerKeyP
        <*> cageConfigP
        <*> trustedRootP

factRetractP :: Parser Command
factRetractP =
    FactRetract
        <$> serverP
        <*> tokenOptionP
        <*> requestIdP
        <*> ownerKeyP
        <*> cageConfigP
        <*> trustedRootP

factRejectP :: Parser Command
factRejectP =
    FactReject
        <$> serverP
        <*> tokenOptionP
        <*> rejectRequestIdsP
        <*> ownerKeyP
        <*> cageConfigP
        <*> trustedRootP

factListP :: Parser Command
factListP =
    FactList
        <$> serverP
        <*> tokenArgOrOptionP
        <*> trustedRootP

factGetP :: Parser Command
factGetP =
    FactGet
        <$> serverP
        <*> tokenArgOrOptionP
        <*> keyArgOrOptionP
        <*> trustedRootP

tokenProcessP :: Parser Command
tokenProcessP =
    TokenProcess
        <$> serverP
        <*> tokenArgOrOptionP
        <*> processRequestIdsP
        <*> ownerKeyP
        <*> cageConfigP
        <*> trustedRootP

tokenEndP :: Parser Command
tokenEndP =
    TokenEnd
        <$> serverP
        <*> tokenOptionP
        <*> ownerKeyP
        <*> cageConfigP
        <*> trustedRootP

tokenListP :: Parser Command
tokenListP =
    TokenList
        <$> serverP
        <*> cageConfigP
        <*> trustedRootP

tokenGetP :: Parser Command
tokenGetP =
    TokenGet
        <$> serverP
        <*> tokenArgOrOptionP
        <*> trustedRootP

requestsListP :: Parser Command
requestsListP =
    RequestsList
        <$> serverP
        <*> tokenArgOrOptionP
        <*> cageConfigP
        <*> trustedRootP
