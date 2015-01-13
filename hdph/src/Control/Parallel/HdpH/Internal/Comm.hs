-- Node to node communication (via TCP)
--
-- Uses the transport layer abstraction for distributed Haskell communication
-- Hackage: http://hackage.haskell.org/package/distributed-process
-- GitHub:  https://github.com/haskell-distributed/distributed-process
--
-- Author: Rob Stewart, Patrick Maier
-----------------------------------------------------------------------------

{-# LANGUAGE CPP #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE MultiParamTypeClasses #-}

{-# OPTIONS_GHC -fno-warn-orphans #-}

module Control.Parallel.HdpH.Internal.Comm
  ( -- * initialise comms layer
    withCommDo,    -- :: RTSConf -> IO () -> IO ()

    -- * information about the comms layer of the virtual machine
    conf,          -- :: IO RTSConf
    nodes,         -- :: IO Int
    allNodes,      -- :: IO [Node]
    equiDistBases, -- :: IO Bases
    myNode,        -- :: IO Node
    rootNode,      -- :: IO Node
    isRoot,        -- :: IO Bool

    -- * sending and receiving payload messages
    Payload,       -- synomyn: Data.ByteString.Lazy.ByteString
    send,          -- :: Node -> Payload -> IO ()
    receive        -- :: IO Payload
  ) where

import Prelude
import Control.Concurrent (forkIO, killThread)
import Control.Concurrent.Chan (newChan, readChan, writeChan)
import Control.DeepSeq (($!!))
import Control.Exception (finally)
import Control.Monad (when, unless)
import Data.ByteString as Strict (ByteString)
import qualified Data.ByteString.Lazy as LBS (toChunks, fromChunks)
import Data.Functor ((<$>))
import Data.IORef (readIORef, writeIORef, atomicModifyIORef)
import Data.Serialize (encode, decode)
import Data.List (delete)

import Network.Info (IPv4, ipv4, name, getNetworkInterfaces)
import qualified Network.Transport as NT
import qualified Network.Transport.TCP as TCP

import Control.Parallel.HdpH.Conf (RTSConf(..), StartupBackend(..))
import Control.Parallel.HdpH.Dist (zero)
import Control.Parallel.HdpH.Internal.Location 
       (Node, address, mkNode, debug, dbgNone, dbgFailure, dbgMsgRcvd)
import Control.Parallel.HdpH.Internal.State.Location
       (myNodeRef, allNodesRef, debugRef)
import qualified Control.Parallel.HdpH.Internal.Data.DistMap as DistMap
import Control.Parallel.HdpH.Internal.Data.CacheMap.Strict
       (CacheMappable, create, destroy)
import qualified Control.Parallel.HdpH.Internal.Data.CacheMap.Strict as CacheMap
import Control.Parallel.HdpH.Internal.Topology (Bases, equiDistMap)
import Control.Parallel.HdpH.Internal.Type.Comm
       (State(..), Payload, PayloadQ, ConnCache)
import Control.Parallel.HdpH.Internal.State.Comm (stateRef)
#if defined(STARTUP_MPI)
import Control.Parallel.HdpH.Internal.CommStartupMPI
       (defaultAllgatherByteStrings)
#elif defined(STARTUP_UDP)
import Control.Parallel.HdpH.Internal.CommStartupUDP (defaultAllgatherByteStrings)
#endif
import Control.Parallel.HdpH.Internal.CommStartupUDP (startupUDP)
import Control.Parallel.HdpH.Internal.CommStartupTCP (startupTCP)

import Control.Parallel.HdpH.Internal.Misc (shuffle)

--import System.IO (hPutStrLn, hFlush, stderr)  -- DEBUG


thisModule :: String
thisModule = "Control.Parallel.HdpH.Internal.Comm"

candidateSockets :: [Int]
candidateSockets = take 128 $ [8001, 8097 .. 40000]


-----------------------------------------------------------------------------
-- access to individual bits of state

-- RTS configuration
conf :: IO RTSConf
conf = s_conf <$> readIORef stateRef

-- Number of nodes in the parallel machine;
-- 0 indicates that initialisation isn't completed yet
nodes :: IO Int
nodes = s_nodes <$> readIORef stateRef

-- List of all nodes in the parallel machine; head is this node;
-- an empty list indicates that initialisation isn't completed yet
allNodes :: IO [Node]
allNodes = s_allNodes <$> readIORef stateRef

-- Distance-indexed map of equidistant bases around 'myNode'
equiDistBases :: IO Bases
equiDistBases = s_bases <$> readIORef stateRef

-- The currently executing node.
-- Version relies on equidistant bases; it would be simpler to just store
-- the current node in a dedicated field in the state of the Comm module.
myNode :: IO Node
myNode = do
  bases <- equiDistBases
  case DistMap.lookup zero bases of
    [(this,1)] -> return this
    _          -> error $ thisModule ++ "myNode: panic - impossible case"
{-
-- Old version; relying on the list of all nodes.
-- Phased out because the list of all nodes is to be phased out.
myNode :: IO Node
myNode = do
  ns <- allNodes
  case ns of
    this:_ -> return this
    []     -> error $ thisModule ++ ".myNode: not initialised"
-}

-- The root node;
-- should not be called before initialisation has been completed
rootNode :: IO Node
rootNode = do
  maybe_root <- s_root <$> readIORef stateRef
  case maybe_root of
    Just root -> return root
    Nothing   -> myNode  -- this node must be the root

-- Returns True if this node is the root node;
-- should not be called before initialisation has been completed
isRoot :: IO Bool
isRoot = do
  maybe_root <- s_root <$> readIORef stateRef
  case maybe_root of
    Just _  -> return False
    Nothing -> return True

-- Remaining pieces of state are internal use only
msgQ :: IO PayloadQ
msgQ = s_msgQ <$> readIORef stateRef

-- defined-not-used, maybe useful in future
transport :: IO NT.Transport
transport = s_tp <$> readIORef stateRef

myEP :: IO NT.EndPoint
myEP = s_ep <$> readIORef stateRef

-- Connection cache
connections :: IO ConnCache
connections = s_connCache <$> readIORef stateRef

-- Debug level; defined-not-used, maybe useful in future
debugLevel :: IO Int
debugLevel = debugLvl <$> conf


-----------------------------------------------------------------------------
-- initialise comms layer

withCommDo :: RTSConf -> IO () -> IO ()
withCommDo conf0 action = do
  -- check debug level
  let debug_level = debugLvl conf0
  unless (debug_level >= dbgNone) $
    error $ thisModule ++ ".withCommDo: debug level < none"

  -- create a transport
  myIP <- discoverMyIP conf0
  tp <- createTransport myIP candidateSockets

  -- create a local end point (and this node)
  ep <- createEndPoint tp
  let !me = mkNode (path conf0) (NT.address ep)
  let !me_enc = encode me

--  hPutStrLn stderr ("DEBUG.withCommDo.1: " ++ show me) >> hFlush stderr

  -- set up an isolated pre-state (not fully init, knows only this node) 
  msg_queue <- newChan
  atomicModifyIORef stateRef $ \ s ->
    (s { s_conf = conf0, s_allNodes = [me],
         s_msgQ = msg_queue, s_tp = tp, s_ep = ep {- , s_shutdown=shutdown -} }, ())

  -- start up a listener
  listener_tid <- forkIO listener

--  hPutStrLn stderr ("DEBUG.withCommDo.2: " ++ show me ++ ", numProcs=" ++ show (numProcs conf0)) >> hFlush stderr

  (do
    universe <- doStartup conf0 me_enc
    when (null universe) $
      error $ thisModule ++ ".withCommDo: node discovery failed"

--    hPutStrLn stderr ("DEBUG.withCommDo.3: " ++ show me ++ ", all=" ++ show universe) >> hFlush stderr

    -- elect root node
    let !root_node = minimum universe
    let !root | me == root_node = Nothing
              | otherwise       = Just root_node

    -- compute equidistant bases around me (after randomising universe)
    others <- shuffle $ delete me universe
    let all_nodes = [me] ++ others
    let !n = length all_nodes
    let !bases = equiDistMap all_nodes

    -- setup connection cache, sized according to RTSConf
    connCache <- CacheMap.empty
    CacheMap.resize (numConns conf0) connCache

--    hPutStrLn stderr ("DEBUG.withCommDo.4: " ++ show me ++ ", conns=" ++ show (numConns conf0)) >> hFlush stderr

    -- set up fully initialised state
    atomicModifyIORef stateRef $ \ s ->
      (s { s_nodes = n, s_allNodes = all_nodes, s_bases = bases, s_root = root,
           s_connCache = connCache }, ())

    -- set current node in Location module
    -- NOTE: HdpH still relies on state in Location module;
    --       this should be changed in the future.
    writeIORef myNodeRef me
    writeIORef allNodesRef all_nodes
    writeIORef debugRef (debugLvl conf0)

--    hPutStrLn stderr ("DEBUG.withCommDo.5: " ++ show me ++ ", isRoot=" ++ show root_node) >> hFlush stderr

    -- run action
    action)

    -- shutdown
    `finally` killThread listener_tid
    -- TODO: reset state and free resources to reflect shutdown of comms layer
  where doStartup cfg thisNode = do
          let backend = startupBackend cfg
          case backend of
            UDP -> startupUDP (numProcs cfg) thisNode >>= return . map decodeNode
            TCP -> startupTCP cfg >>= return . map decodeNode

-- Return the IP address associated with the interface named in RTS config.
discoverMyIP :: RTSConf -> IO IPv4
discoverMyIP config = do
  nis <- getNetworkInterfaces
  case filter (\ ni -> name ni == interface config) nis of
    []   -> error $ thisModule ++ ".discoverMyIP: no matching interface"
    ni:_ -> return $ ipv4 ni


-- Return a transport for the given IP address and one of the given candidate
-- sockets.
createTransport :: IPv4 -> [Int] -> IO NT.Transport
createTransport myIP candidate_sockets = do
  let thisFun = thisModule ++ ".createTransport"
  case candidate_sockets of
    []         -> error $ thisFun ++ ": no socket"
    sock:socks -> do
      t <- TCP.createTransport (show myIP) (show sock) TCP.defaultTCPParameters
      case t of
        Right tp -> return tp
        Left e -> if null socks
                    then error $ thisFun ++ ": " ++ show e
                    else createTransport myIP socks


-- Return a local endpoint for the given transport.
createEndPoint :: NT.Transport -> IO NT.EndPoint
createEndPoint tp = do
  ep <- NT.newEndPoint tp
  case ep of
    Right my_ep -> return my_ep
    Left  e     -> error $ thisModule ++ ".createEndpoint: " ++ show e


-- Deserialize node; abort on error.
decodeNode :: Strict.ByteString -> Node
decodeNode bs =
  case decode bs of
    Right node -> node
    Left e     -> error $ thisModule ++ ".decodeNode: " ++ show e


-- Instantiate CacheMappable class to make connection cache work;
-- orphan instance
instance CacheMappable Node NT.Connection where

  -- Create a new (heavyweight) connection to the given node.
  create node = do
    ep <- myEP
    c <- NT.connect ep (address node) NT.ReliableOrdered NT.defaultConnectHints
    case c of
      Right conn -> return $ Just conn
      Left err   -> do debug dbgFailure $
                         "Comm.connect to " ++ show node ++ ": " ++ show err
                       return Nothing

  -- Close the given connection.
  destroy _ conn = NT.close conn


-----------------------------------------------------------------------------
-- sending and receiving payload messages

-- | Block to receive a payload message;
--   the sender must be encoded into the message, otherwise it is unknown.
receive :: IO Payload
receive = msgQ >>= readChan


-- | Send a payload message.
--   Errors are ignored (they are assumed to occur only during shutdown).
send :: Node -> Payload -> IO ()
send dest message = do
  -- lookup connection; create and cache if non-existent
  maybe_conn <- CacheMap.lookup dest =<< connections
  case maybe_conn of
    Nothing   -> return ()  -- ignoring connection error
    Just conn -> do
      ok <- NT.send conn $ LBS.toChunks message
      case ok of
        Right () -> return ()
        Left err -> debug dbgFailure $
                      "Comm.send to " ++ show dest ++ ": " ++ show err


-----------------------------------------------------------------------------
-- async listening to messages (internal)

-- Non-terminating computation, to be run in a separate thread.
-- Continually receives messages sent to the given end point, which it puts
-- into the given message queue.
-- * Listener relies on Comm state being partially initialised.
listener :: IO ()
listener = do
  ep <- myEP
  q <- msgQ
  let loop = recv ep >>= writeChan q >> loop
  loop


-- Called by the listener to actually receive a message.
recv :: NT.EndPoint -> IO Payload
recv ep = do
  event <- NT.receive ep
  case event of
    NT.Received _ msg -> return $!! LBS.fromChunks msg  -- Q: ($!!) or ($!)?
    NT.ErrorEvent err -> do
      debug dbgFailure $ "Comm.recv: " ++ show err
      case err of
        NT.TransportError (NT.EventConnectionLost addr) _ -> do
          root <- rootNode  -- DODGY: Could we reach this before root is init'd?
          if addr == address root
            then -- Fatal: lost connection to root node.
                 error $ thisModule ++ ".recv: lost connection to root node"
            else recv ep -- ignore other lost connections
        _ -> recv ep     -- ignore ofter error events
    _ -> do
      debug dbgMsgRcvd $ "Comm.recv: " ++ show event
      recv ep            -- ignore other events
