-- Locations
--   includes API for error and debug messages
--
-- Author: Rob Stewart, Patrick Maier
-----------------------------------------------------------------------------

{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}

{-# OPTIONS_GHC -fno-warn-orphans #-}

module Control.Parallel.HdpH.Internal.Location
  ( -- * node IDs (and their constitutent parts)
    Node,                 -- instances: Eq, Ord, Show, NFData, Serialize
    path,                 -- :: Node -> [String]
    address,              -- :: Node -> NT.EndPointAddress
    hash,                 -- :: Node -> Word32
    mkNode,               -- :: [String] -> NT.EndPointAddress -> Node

    -- * reading all node IDs and this node's own node ID
    allNodes,             -- :: IO [Node]
    myNode,               -- :: IO Node
    myNode',              -- :: IO (Maybe Node)
    MyNodeException(..),  -- instances: Exception, Show, Typeable

    -- * error messages tagged by emitting node
    error,         -- :: String -> a

    -- * debug messages tagged by emitting node
    debug,         -- :: Int -> String -> IO ()

    -- * debug levels
    dbgNone,       -- :: Int
    dbgStats,      -- :: Int
    dbgStaticTab,  -- :: Int
    dbgFailure,    -- :: Int
    dbgSpark,      -- :: Int
    dbgMsgSend,    -- :: Int
    dbgMsgRcvd,    -- :: Int
    dbgGIVar,      -- :: Int
    dbgIVar,       -- :: Int
    dbgGRef        -- :: Int
  ) where

import Prelude hiding (error)
import qualified Prelude (error)
import Control.DeepSeq (NFData, rnf)
import Control.Exception (catch, evaluate)
import Control.Monad (when)
import Data.Functor ((<$>))
import qualified Data.Hashable (hash)
import Data.IORef (readIORef)
import Data.Serialize (Serialize, put, get)
import qualified Network.Transport as NT (EndPointAddress(..))
import System.IO (stderr, hPutStrLn)
import System.IO.Unsafe (unsafePerformIO)

import Control.Parallel.HdpH.Internal.State.Location
       (myNodeRef, allNodesRef, debugRef)
import Control.Parallel.HdpH.Internal.Type.Location
       (Node(Node, address, path, hash), MyNodeException(MyNodeUnset))


-----------------------------------------------------------------------------
-- node IDs (abstract outwith this module)
-- NOTE: Node IDs are hyperstrict.

-- Smart constructor for node IDs; ensures resulting node ID is hyperstrict;
-- this constructor is only to be exported to module HdpH.Internal.Comm.
mkNode :: [String] -> NT.EndPointAddress -> Node
mkNode p addr =
  h `seq` Node { address = addr, path = p, hash = h }  -- computing h forces
    where                                              -- addr and p
      h = fromIntegral $ Data.Hashable.hash (addr, p)


-- orphan instance
instance Eq Node where
  n1 == n2 = hash n1    == hash n2 &&    -- compare hash first (for speed)
             address n1 == address n2

-- orphan instance
instance Ord Node where
  compare n1 n2 =
    case compare (hash n1) (hash n2) of  -- compare hash first (for speed)
      EQ       -> compare (address n1) (address n2)
      lt_or_gt -> lt_or_gt

-- orphan instance
instance Show Node where
  showsPrec _ n = showChar '<' . shows (path n) . showChar ',' .
                                 shows (address n) . showChar '>'

-- orphan instance
instance NFData Node  -- default instance suffices (due to hyperstrictness)

-- orphan instance
-- NOTE: Can't derive this instance because 'get' must ensure hyperstrictness
instance Serialize Node where
  put n = put (hash n) >>
          put (address n) >>
          put (path n)
  get = do
    h    <- get
    addr <- get
    p    <- get
    let n = Node { address = addr, path = p, hash = h }
    -- force h, addr and p to make Node n hyperstrict
    h `seq` rnf addr `seq` rnf p `seq` return n


-----------------------------------------------------------------------------
-- reading this node's own node ID

-- Return this node's node ID;
-- raises 'NodeUnset :: MyNodeException' if node ID has not yet been set
-- (by module HdpH.Internal.Comm).
myNode :: IO Node
myNode = readIORef myNodeRef


-- Return 'Just' this node's node ID, or 'Nothing' if ID has not yet been set.
myNode' :: IO (Maybe Node)
myNode' =
  catch (Just <$> (evaluate =<< myNode))
        (const $ return Nothing :: MyNodeException -> IO (Maybe Node))


-- Return list of all nodes (with main node being head of the list),
-- provided the list has been initialised (by module HdpH.Internal.Comm);
-- otherwise returns the empty list.
allNodes :: IO [Node]
allNodes = readIORef allNodesRef


-----------------------------------------------------------------------------
-- error messages tagged by emitting node

-- Abort with error 'message'.
error :: String -> a
error message = case unsafePerformIO myNode' of
                  Just node -> Prelude.error (show node ++ " " ++ message)
                  Nothing   -> Prelude.error message


-----------------------------------------------------------------------------
-- debug messages tagged by emitting node

-- Output a debug 'message' to 'stderr' if the given 'level' is less than
-- or equal to the system level; 'level' should be positive.
debug :: Int -> String -> IO ()
debug level message = do
  sysLevel <- readIORef debugRef
  when (level <= sysLevel) $ do
    maybe_this <- myNode'
    case maybe_this of
      Just this -> hPutStrLn stderr $ show this ++ " " ++ message
      Nothing   -> hPutStrLn stderr $ "<unknown> " ++ message


-- debug levels
dbgNone, dbgStats, dbgStaticTab, dbgFailure, dbgSpark :: Int
dbgMsgSend, dbgMsgRcvd, dbgGIVar, dbgIVar, dbgGRef :: Int
dbgNone      = 0  -- no debug output
dbgStats     = 1  -- print final stats
dbgStaticTab = 2  -- on main node, print Static table
dbgFailure   = 3  -- detect node failure
dbgSpark     = 4  -- sparks created or converted
dbgMsgSend   = 5  -- messages being sent
dbgMsgRcvd   = 6  -- messages being handled
dbgGIVar     = 7  -- op on a GIVar (globalising or writing to)
dbgIVar      = 8  -- blocking/unblocking on IVar (only log event type)
dbgGRef      = 9  -- registry update


-----------------------------------------------------------------------------
-- missing (orphan) instances for NT.EndPointAddress

deriving instance NFData NT.EndPointAddress
deriving instance Serialize NT.EndPointAddress
