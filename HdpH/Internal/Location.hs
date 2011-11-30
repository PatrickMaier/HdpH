-- Locations; wrapper module
--   includes API for error and debug messages
--
-- Visibility: HdpH, HdpH.Internal
-- Author: Patrick Maier <P.Maier@hw.ac.uk>
-- Created: 22 Sep 2011
--
-----------------------------------------------------------------------------

module HdpH.Internal.Location
  ( -- re-exports from HdpH.Internal.Location_MPI

    -- * node IDs (and their constitutent parts)
    NodeId,           -- instances: Eq, Ord, Show, NFData, Serialize
    rank,             -- :: NodeId -> MP.MPI.Rank
    host,             -- :: NodeId -> IPv4Addr
    mkMyNodeId,       -- :: IO NodeId
    NetworkAddr,      -- instances: Eq, Ord, Show, NFData, Serialize
    validNetworkAddr, -- :: NetworkAddr -> Bool
    fromNetworkAddr,  -- :: NetworkAddr -> [Word8]

    -- end of re-exports

    -- * reading all node IDs and this node's own node ID
    allNodes,             -- :: IO [NodeId]
    myNode,               -- :: IO NodeId
    myNode',              -- :: IO (Maybe NodeId)
    MyNodeException(..),  -- instances: Exception, Show, Typeable

    -- * error messages tagged by emitting node
    error,  -- :: String -> a

    -- * debug messages tagged by emitting node
    debug,  -- :: Int -> String -> IO ()

    -- * debug levels
    dbgNone,     -- :: Int
    dbgStats,    -- :: Int
    dbgSpark,    -- :: Int
    dbgMsgSend,  -- :: Int
    dbgMsgRcvd,  -- :: Int
    dbgGIVar,    -- :: Int
    dbgIVar,     -- :: Int
    dbgGRef      -- :: Int
  ) where

import Prelude hiding (catch, error)
import qualified Prelude (error)
import Control.Exception (catch, evaluate)
import Control.Monad (when)
import Data.Functor ((<$>))
import Data.IORef (readIORef)
import System.IO (stderr, hPutStrLn)
import System.IO.Unsafe (unsafePerformIO)

import HdpH.Internal.State.Location (myNodeRef, allNodesRef, debugRef)
import HdpH.Internal.Type.Location (MyNodeException(NodeIdUnset))

-- import what's re-exported above
import HdpH.Internal.Location_MPI


-----------------------------------------------------------------------------
-- reading this node's own node ID

-- Return this node's node ID;
-- raises 'NodeIdUnset :: MyNodeException' if node ID has not yet been set
-- (by module HdpH.Internal.Comm).
myNode :: IO NodeId
myNode = readIORef myNodeRef


-- Return 'Just' this node's node ID, or 'Nothing' if ID has not yet been set.
myNode' :: IO (Maybe NodeId)
myNode' =
  catch (Just <$> (evaluate =<< myNode))
        (const $ return Nothing :: MyNodeException -> IO (Maybe NodeId))


-- Return list of all nodes (with main node being head of the list),
-- provided the list has been initialised (by module HdpH.Internal.Comm);
-- otherwise returns the empty list.
allNodes :: IO [NodeId]
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
dbgNone    = 0 :: Int  -- no debug output
dbgStats   = 1 :: Int  -- print final stats
dbgSpark   = 2 :: Int  -- spark created or converted
dbgMsgSend = 3 :: Int  -- message to be sent
dbgMsgRcvd = 4 :: Int  -- message being handled
dbgGIVar   = 5 :: Int  -- op on a GIVar (globalising or writing to)
dbgIVar    = 6 :: Int  -- blocking/unblocking on an IVar (only log event type)
dbgGRef    = 7 :: Int  -- registry update (globalise or free)
