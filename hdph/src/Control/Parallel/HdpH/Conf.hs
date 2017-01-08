-- HpdH runtime configuration parameters
--
-- Author: Patrick Maier
-----------------------------------------------------------------------------

module Control.Parallel.HdpH.Conf
  ( -- * HdpH runtime system configuration parameters
    RTSConf(..),
    StartupBackend(..),
    defaultRTSConf,      -- :: RTSConf

    -- * reading and updating HdpH RTS config parameters
    updateConfFromFile,  -- :: String -> RTSConf -> IO (Either String RTSConf)
    updateConfFromArgs,  -- :: [String] -> RTSConf
                         -- -> IO (Either String (RTSConf, [String]))
    updateConf           -- :: [String] -> RTSConf
                         -- -> IO (Either String (RTSConf, [String]))
  ) where

import Prelude
import Control.Monad (foldM)
import Data.Char (isDigit, isSpace)
import Data.List (intercalate, stripPrefix)
import GHC.Conc (getNumCapabilities)
import qualified Network.BSD (getHostName)
import qualified System.Posix.Process (getProcessID)
import Text.ParserCombinators.ReadP
       (ReadP, readP_to_S, (<++), (+++), pfail, eof, skipSpaces, string, munch,
        between, sepBy, many, option, optional)
import Network.Socket (HostName, ServiceName)

import Control.Parallel.HdpH.Internal.Location (dbgNone)

-----------------------------------------------------------------------------
-- Runtime configuration parameters (for RTS monad stack)

-- | 'RTSConf' is a record data type collecting a number of parameter
-- governing the behaviour of the HdpH runtime system.
--
-- TODO: Describe format of config file and format of parameter/value pairs.
data RTSConf =
  RTSConf {
    debugLvl :: Int,
        -- ^ Debug level, a number defined in module
        -- "Control.Parallel.HdpH.Internal.Location".
        -- Default is 0 (corresponding to no debug output).
        --
        -- TODO: Describe switch and possible values.

    scheds :: Int,
        -- ^ Number of concurrent schedulers per node. Must be positive and 
        -- should be @<=@ to the number of HECs (as set by GHC RTS option 
        -- @-N@). Default is 1.
        --
        -- TODO: Describe switch and possible (negative) values.

    selSparkFIFO :: Bool,
        -- ^ Select sparks always according to FIFO policy, regardless of
        -- whether selection for local execution or remote scheduling.
        -- Default is False, which means that local execution selects
        -- youngest sparks; remote scheduling selects oldest.

    wakeupDly :: Int,
        -- ^ Interval in microseconds to wake up sleeping schedulers
        -- (which is necessary to recover from a race condition between
        -- concurrent schedulers). Must be positive. 
        -- Default is 1000 (corresponding to 1 millisecond).

    maxHops :: Int,
        -- ^ Number of hops a FISH message may travel at a certain distance
        -- from the thief before having to ripple out.
        -- Must be non-negative. Default is 3.

    maxFish :: Int,
        -- ^ Low sparkpool watermark for fishing.
        -- If positive or zero, RTS will send FISH messages whenever the size 
        -- of the spark pool drops to 'maxFish' or lower.
        -- If negative, RTS will not send FISH messages.
        -- Should be @<@ 'minSched'. Default is 1.

    minSched :: Int,
        -- ^ Low sparkpool watermark for scheduling. RTS will respond to FISH 
        -- messages by SCHEDULEing sparks unless size of spark pool is less
        -- than 'minSched'. Must be non-negative; should be @>@ 'maxFish'.
        -- Default is 2.

    minFishDly :: Int,
        -- ^ After a failed FISH, minimal delay in microseconds before
        -- sending another FISH message; the actual delay is chosen randomly
        -- between 'minFishDly' and 'maxFishDly'. Must be non-negative; should
        -- be @<=@ 'maxFishDly'.
        -- Default is 10000 (corresponding to 10 milliseconds).

    maxFishDly :: Int,
        -- ^ After a failed FISH, maximal delay in microseconds before
        -- sending another FISH message; the actual delay is chosen randomly
        -- between 'minFishDly' and 'maxFishDly'. Must be non-negative; should
        -- be @>=@ 'minFishDly'.
        -- Default is 1000000 (corresponding to 1 second).

    numProcs :: Int,
        -- ^ Number of nodes constituting the distributed runtime system.
        -- Only relevant to UDP startup.
        -- Must be positive. Default is 0 (ie. must be overridden manually).

    numConns :: Int,
        -- ^ Upper bound on the number of TCP connections cached by this node;
        -- a negative number means there is no upper bound (in which case
        -- the number of connections is bounded by the total number of nodes).
        -- Default is -1 (ie. cache all connections).

    interface :: String,
        -- ^ Network interface, required to autodetect a node's
        -- IP address. The string must be one of the interface names
        -- returned by the POSIX command @ifconfig@.
        -- Default is @eth0@ (corresponding to the first Ethernet interface).

    confFile :: String,
        -- ^ Name of configuration file, or empty string.
        -- Default is empty string (corresponding to no configuration file).

    path :: [String],
        -- ^ Path from root of network topology to this node.
        -- Default is empty list (corresponding to the trivial topology).

    -- Startup Options

    startupBackend :: StartupBackend,
        -- ^ Which backend we want to use for Node discovery.

    startupHost :: HostName,
        -- ^ TCP node discovery: Which address should nodes look for to.

    startupPort :: ServiceName,
        -- ^ TCP node discovery: Which port should nodes look for to.

    startupTimeout :: Int,
        -- ^ Timeout (in seconds) for the TCP node discovery to trigger an
        -- error.

    -- Optimisation Options

    useLastStealOptimisation :: Bool,
        -- ^ Should HdpH track the location of the last sucessful steal and
        -- use this as the first steal candidate as it is likely to be a source
        -- of work?

    useLowWatermarkOptimisation :: Bool
        -- ^ Should HdpH try to fish when there are less than 'maxFish'
        --   sparks left or should it wait until it finds no work before
        --   performing a fish?

    }
    deriving (Show)  -- for testing

-- | Default runtime system configuration parameters.
defaultRTSConf :: RTSConf
defaultRTSConf =
  RTSConf {
    debugLvl       = dbgNone,  -- no debug information
    scheds         = 1,        -- only 1 scheduler by default
    selSparkFIFO   = False,    -- don't observe strict FIFO scheduling
    wakeupDly      = 1000,     -- wake up one sleeping scheduler every millisecond
    maxHops        = 3,        -- no more than 3 hops per FISH
    maxFish        = 1,        -- send FISH when <= 1 spark in pool
    minSched       = 2,        -- reply with SCHEDULE when >= 2 sparks in pool
    minFishDly     = 10000,    -- delay at least 10 milliseconds after failed FISH
    maxFishDly     = 1000000,  -- delay up to 1 second after failed FISH
    numProcs       = 1,        -- override this default for UDP node discovery
    numConns       = -1,       -- default: cache all TCP connections
    interface      = "eth0",   -- default network interface: 1st Ethernet adapter
    confFile       = "",       -- default config file: none
    path           = [],       -- default path: empty list = no path
    startupBackend = UDP,      -- default to udp discovery since this doesn't need extra params
    startupHost    = "",
    startupPort    = "",
    startupTimeout = 10,        -- default (TCP) startup timeout
    useLastStealOptimisation    = True, -- Optimisations on by default.
    useLowWatermarkOptimisation = True
    }

-- StartupBackends
data StartupBackend = TCP | UDP deriving (Show)

-----------------------------------------------------------------------------
-- parsing configuration parameters

-- | Update the given configuration with the given list of command line
-- arguments; see '@updateConfFromArgs@' for a description of the argument
-- format.
-- The given configuration is updated according to a configuration file
-- first if one of the arguments specifies a config file.
-- Returns either the updated configuration and a list of remaining command
-- line arguments, or a string indicating an error that occured while reading
-- the configuration file (see '@updateConfFromFile@') or parsing the command
-- line arguments.
updateConf :: [String] -> RTSConf -> IO (Either String (RTSConf, [String]))
updateConf args conf0 = do
  parse1 <- updateConfFromArgs args conf0
  case parse1 of
    Left err_msg              -> return $ Left err_msg
    Right (conf1, other_args) -> do
      let conf_file_name = confFile conf1
      if null conf_file_name
        then return $ Right (conf1, other_args)
        else do
          parse2 <- updateConfFromFile conf_file_name conf0
          case parse2 of
            Left err_msg -> return $ Left err_msg
            Right conf2  -> updateConfFromArgs args conf2


-----------------------------------------------------------------------------
-- parsing configuration parameters from command line

-- | Update the given configuration with the given list of parameter/value
-- pairs. Returns the updated configuration plus a list of strings that could
-- not be parsed as parameter/value pairs.
-- TODO: Update doc
updateConfFromArgs :: [String] -> RTSConf
                   -> IO (Either String (RTSConf, [String]))
updateConfFromArgs args conf = do
  let pref = "Control.Parallel.HdpH.Conf.updateConfFromArgs: "
  hostname <- getHOSTNAME
  pid      <- getPID
  caps     <- getNumCapabilities
  let (opts, args') = bracket (== "+HdpH") (== "-HdpH") args
  case updConfWith hostname pid caps opts conf of
    Left err_msg -> return $ Left $ pref ++ "parse error on " ++ err_msg
    Right conf'  -> return $ Right (conf', args')


-- update the given configuration with the given list of parameter/value pairs.
-- Returns the updated configuration or the option string causing a parse error.
updConfWith :: String -> String -> Int -> [String] -> RTSConf
            -> Either String RTSConf
updConfWith hostname pid caps opts conf = foldM readEntry conf opts
  where
    readEntry :: RTSConf -> String -> Either String RTSConf
    readEntry conf' opt =
      case readP_to_S (parseEntry conf') opt of
        [(conf'', _)] -> conf'' `seq` Right conf''
        _             -> Left opt
    parseEntry :: RTSConf -> ReadP RTSConf
    parseEntry conf' = do
      optional $ string "-"
      parseConfEntry hostname pid caps conf'


-- parse a single configuration parameter/value pair and update given conf
parseConfEntry :: String -> String -> Int -> RTSConf -> ReadP RTSConf
parseConfEntry hostname pid caps conf =
      (string "d" >> parseNat >>= \ n -> eof >>
       return conf { debugLvl = n })
  <++ (string "debug" >> skipEqual >> parseNat >>= \ n -> eof >>
       return conf { debugLvl = n })
  <++ (string "scheds" >> skipEqual >> parseInt >>= \ i -> eof >>
       let n = if i > 0 then i else max 1 (caps + i) in
       return conf { scheds = n })
  <++ (string "selSparkFIFO" >> skipEqual >> parseBool >>= \ b -> eof >>
       return conf { selSparkFIFO = b })
  <++ (string "wakeup" >> skipEqual >> parseNat >>= \ n -> eof >>
       return conf { wakeupDly = n })
  <++ (string "hops" >> skipEqual >> parseNat >>= \ n -> eof >>
       return conf { maxHops = n })
  <++ (string "maxFish" >> skipEqual >> parseInt >>= \ i -> eof >>
       return conf { maxFish = i })
  <++ (string "minSched" >> skipEqual >> parseNat >>= \ n -> eof >>
       return conf { minSched = n })
  <++ (string "minNoWork" >> skipEqual >> parseNat >>= \ n -> eof >>
       return conf { minFishDly = n })
  <++ (string "maxNoWork" >> skipEqual >> parseNat >>= \ n -> eof >>
       return conf { maxFishDly = n })
  <++ (string "numProcs" >> skipEqual >> parseNat >>= \ n -> eof >>
       return conf { numProcs = n })
  <++ (string "conns" >> skipEqual >> parseInt >>= \ i -> eof >>
       return conf { numConns = i })
  <++ (string "nic" >> skipEqual >> parseWord >>= \ w -> eof >>
       return conf { interface = w })
  <++ (string "config" >> skipEqual >> parseWord >>= \ w -> eof >>
       return conf { confFile = w })
  <++ (string "path" >> skipEqual >> parsePath >>= \ p -> eof >>
       return conf { path = expandPath hostname pid p })
  <++ (string "startupBackend" >> skipEqual >> parseBackend >>= \b -> eof >>
       return conf { startupBackend = b })
  <++ (string "startupHost" >> skipEqual >> parseWord >>= \h -> eof >>
       return conf { startupHost = h })
  <++ (string "startupPort" >> skipEqual >> parseWord >>= \p -> eof >>
       return conf { startupPort = p })
  <++ (string "startupTimeout" >> skipEqual >> parseInt >>= \t -> eof >>
         return conf { startupTimeout = t })
  <++ (string "useLastStealOptimisation" >> skipEqual >> parseBool >>= \b -> eof >>
         return conf { useLastStealOptimisation = b })
  <++ (string "useLowWatermarkOptimisation" >> skipEqual >> parseBool >>= \b -> eof >>
         return conf { useLowWatermarkOptimisation = b })
  <++ pfail

-- consume a single equals sign, including surrounding space
skipEqual :: ReadP ()
skipEqual = skipSpaces >> string "=" >> skipSpaces

-- parse a maximal string of digits, returning a non-negative integer;
-- consume trailing space
parseNat :: ReadP Int
parseNat = do
  digits <- munch isDigit 
  skipSpaces
  case reads digits of
    [(n, [])] -> return n
    _         -> pfail

-- parse an optional "-" followed by a  maximal string of digits,
-- returning an integer; consume trailing space
parseInt :: ReadP Int
parseInt = do
  sign   <- option "" $ string "-"
  digits <- munch isDigit
  skipSpaces
  case reads (sign ++ digits) of
    [(n, [])] -> return n
    _         -> pfail

-- parse and return a maximal string of non-space characters;
-- consume trailing space
parseWord :: ReadP String
parseWord = do
  word <- munch (not . isSpace)
  skipSpaces
  return word

-- Parse a boolean value; Consume trailing spaces.
parseBool :: ReadP Bool
parseBool = do
    v <- parseTrue +++ parseFalse
    skipSpaces
    return v
  where parseTrue  = string "true"  +++ string "TRUE"  +++ string "True"  >>
                     return True
        parseFalse = string "false" +++ string "FALSE" +++ string "False" >>
                     return False

-- parse and return an absolute path, ie. a list of strings separated by
-- forward slashes (and without spaces); consume trailing space
parsePath :: ReadP [String]
parsePath = do
  p <- many $ do { _ <- string "/"; munch $ \ c -> c /= '/' && not (isSpace c) }
  skipSpaces
  return p

-- expand the unknowns %HOSTNAME and %PID in the given path to the values
-- of hostname and pid, respectively
expandPath :: String -> String -> [String] -> [String]
expandPath hostname pid =
  map (replace "%HOSTNAME" hostname . replace "%PID" pid)

parseBackend :: ReadP StartupBackend
parseBackend = parseUDP +++ parseTCP
  where parseUDP = string "udp" +++ string "UDP" >> return UDP
        parseTCP = string "tcp" +++ string "TCP" >> return TCP

-----------------------------------------------------------------------------
-- parsing configuration file

-- | Update the given configuration by reading the given configuration file.
-- Returns either the updated configuration, or a string indicating a error 
-- (which may be a parse error, or a no-entry-found error).
-- For a description of the available parameters and the admissible values
-- see the documentation of @RTSConf@.
updateConfFromFile :: String -> RTSConf -> IO (Either String RTSConf)
updateConfFromFile filename conf = do
  let pref = "Control.Parallel.HdpH.Conf.updateConfFromFile: "
  let parse_error = pref ++ "parse error"   
  let no_entry = pref ++ "no entry"
  hostname  <- getHOSTNAME
  pid       <- getPID
  caps      <- getNumCapabilities
  conf_data <- readFile filename
  case readP_to_S (parseConfFile hostname pid caps conf) conf_data of
    [(Just (Right conf'), _)] -> return $ Right $ conf'
    [(Just (Left err),    _)] -> return $ Left $ parse_error ++ " on " ++ err
    [(Nothing,            _)] -> return $ Left $ no_entry ++ " for " ++ hostname
    _                         -> return $ Left $ parse_error

-- parse config file and update given configuration; returns Nothing if
-- the given hostname was not found in the config file
parseConfFile :: String -> String -> Int -> RTSConf
              -> ReadP (Maybe (Either String RTSConf))
parseConfFile hostname pid caps conf = skipComments >> parse
  where
    parse :: ReadP (Maybe (Either String RTSConf))
    parse =     (eof >> return Nothing)
            <++ (do maybe_conf <- parseConfFileEntry hostname pid caps conf
                    case maybe_conf of
                      Nothing -> parse
                      Just _  -> return maybe_conf)

-- parse config file entry and update given if the entry's hostname matches
-- the given hostname; returns Nothing otherwise
parseConfFileEntry :: String -> String -> Int -> RTSConf
                   -> ReadP (Maybe (Either String RTSConf))
parseConfFileEntry hostname pid caps conf = do
  w <- parseWord
  skipComments
  settings <- parseList
  if w /= hostname
    then return Nothing
    else return $ Just $ updConfWith hostname pid caps settings conf


-- parse and return a comma-separated list of strings enclosed in braces;
-- consume space and comments trailing commas and braces
parseList :: ReadP [String]
parseList =
  between skipLBrace skipRBrace $ sepBy (munch (`notElem` "{,}")) skipComma

-- parse a single left brace; consume trailing space and comments
skipLBrace :: ReadP ()
skipLBrace = string "{" >> skipComments

-- parse a single right brace; consume trailing space and comments
skipRBrace :: ReadP ()
skipRBrace = string "}" >> skipComments

-- parse a single comma; consume trailing space and comments
skipComma :: ReadP ()
skipComma = string "," >> skipComments

-- consume white space and comments, where comments start with a hash and
-- extend to the end of the line
skipComments :: ReadP ()
skipComments =
  (skipSpaces >> string "#" >> munch (/= '\n') >> skipComments) <++ skipSpaces


-----------------------------------------------------------------------------
-- auxiliary functions

-- Return the host name of the current node. Ideally should return a fully
-- qualified host name but currently likely returns an unqualified one.
getHOSTNAME :: IO String
getHOSTNAME = Network.BSD.getHostName

-- Return the PID of the current node.
getPID :: IO String
getPID = show <$> System.Posix.Process.getProcessID


-- In the source list (third argument) replace every non-overlapping occurrence
-- of the pattern list with the substitute list.
replace :: (Eq a) => [a] -> [a] -> [a] -> [a]
replace pat subst = intercalate subst . splitOn pat

-- Split source list into pieces separated by the non-empty pattern list,
-- consuming the pattern. See Data.Text.splitOn for relevant properties.
splitOn :: (Eq a) => [a] -> [a] -> [[a]]
splitOn []  _   = error "Control.Parallel.HdpH.Conf.splitOn: empty pattern"
splitOn pat src = case splitOnFst pat src of
                    Nothing          -> [src]
                    Just (pref, suf) -> pref : splitOn pat suf

-- Split source list into a prefix and a suffix separated by first occurence
-- of the pattern list. Returns Nothing if the pattern does not occur.
splitOnFst :: (Eq a) => [a] -> [a] -> Maybe ([a], [a])
splitOnFst pat src =
  case src of
    []   -> Nothing
    x:xs -> case stripPrefix pat src of
              Just suf -> Just ([], suf)
              Nothing  -> case splitOnFst pat xs of
                            Just (pref, suf) -> Just (x:pref, suf)
                            Nothing          -> Nothing


-- Split list @xs@ into items appearing in between the @open@ and @close@
-- predicates (1st component) and items outwith (2nd component).
-- Preserves the relative order of items on the list.
bracket :: (a -> Bool) -> (a -> Bool) -> [a] -> ([a], [a])
bracket open close xs =
  case break open xs of
    (prefix, [])    -> ([], prefix)
    (prefix, _:xs') -> (bracketed, prefix ++ rest)
                         where
                           (rest, bracketed) = bracket close open xs'
