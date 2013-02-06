-- Node to node communication (via TCP)
--
-- Uses the transport layer abstraction for distributed Haskell communication
-- Hackage: http://hackage.haskell.org/package/distributed-process
-- GitHub:  https://github.com/haskell-distributed/distributed-process
--
-- Author: Rob Stewart, Patrick Maier
-----------------------------------------------------------------------------

{-# LANGUAGE CPP #-}
{-# LANGUAGE ScopedTypeVariables #-}         -- for 'IOException'

module Control.Parallel.HdpH.Internal.Comm
  ( -- * CommM monad
    CommM,           -- synonym: Control.Monad.Reader.ReaderT <State> IO
    run_,            -- :: RTSConf -> CommM () -> IO ()
    liftIO,          -- :: IO a -> CommM a

    -- * information about the virtual machine
    nodes,           -- :: CommM Int
    allNodes,        -- :: CommM [NodeId]
    myNode,          -- :: CommM NodeId
    isMain,          -- :: CommM Bool

    -- * sending and receiving messages
    Message,         -- synomyn: MPI.Msg (= Data.ByteString.Lazy.ByteString)
    send,            -- :: NodeId -> Message -> CommM ()
    receive,         -- :: CommM Message
    shutdown,        -- :: CommM ()
    waitShutdown     -- :: CommM ()
  ) where

import Prelude hiding (error)
import qualified Prelude (error)
import Control.DeepSeq (NFData(rnf),force)
import Control.Exception (throw)
import Control.Monad (unless,void,when,forever)
import Control.Monad.Reader (ReaderT, runReaderT, ask)
import Control.Monad.Trans (lift)
import Data.Functor ((<$>))
import Data.IORef (writeIORef,atomicModifyIORef)
import qualified Data.Serialize (Serialize, put, get)
import Data.Word (Word8)

import Control.Parallel.HdpH.Internal.Misc (encodeLazy, decodeLazy)
import Control.Parallel.HdpH.Conf 
       (RTSConf(debugLvl),numProcs, networkInterface)
import Control.Parallel.HdpH.Internal.Location 
       (NodeId, MyNodeException(NodeIdUnset), error, dbgNone)
import Control.Parallel.HdpH.Internal.State.Location (myNodeRef, debugRef)
import Control.Concurrent
import System.IO (hPutStrLn,stderr) -- Used by HDPH_DEBUG
import System.Exit (ExitCode(..),exitWith)
import System.Timeout (timeout)


import qualified Data.ByteString.Lazy as Lazy (ByteString,toChunks,fromChunks)
import qualified Network.Transport as NT
import qualified Network.Transport.TCP as TCP
import Data.IORef (IORef, newIORef, readIORef)
import System.IO.Unsafe (unsafePerformIO)
import Data.Maybe
import qualified Data.Map as Map
import Data.List ((\\))
import Control.Exception (SomeException,try)

import Network.Multicast
import Network.Socket.ByteString (sendTo, recvFrom)
import Network.Socket (Socket)
import Network.Info
import Data.List (sort)
import System.Random (randomRs, newStdGen)

-----------------------------------------------------------------------------
-- state representation

data State =
  State { s_conf     :: RTSConf,  -- config data
          s_nodes    :: Int,      -- # nodes of virtual machine
          s_allNodes :: [NodeId], -- all nodes of virtual machine
          s_myNode   :: NodeId,   -- currently executing node
          s_isMain   :: Bool,     -- True iff currently executing is main node
          s_msgQ     :: MessageQ, -- queue holding received payload messages
          s_shutdown :: MVar () }  -- shutdown signal

-- concurrent message queue (storing payload messages)
type MessageQ = Chan Message

-- a message is an MPI message (here: a lazy byte string)
type Message = Lazy.ByteString

-----------------------------------------------------------------------------
-- CommM monad

-- CommM is a reader monad on top of the IO monad; mutable parts of the state 
-- (namely the message queue) are implemented via mutable references.
type CommM = ReaderT State IO

-- lifting lower layers
liftIO :: IO a -> CommM a
liftIO = lift


-----------------------------------------------------------------------------
-- access to individual bits of state

-- Number of nodes in the virtual machine.
nodes :: CommM Int
nodes = s_nodes <$> ask

-- List of all nodes in the virtual machine; head should be main node.
allNodes :: CommM [NodeId]
allNodes = s_allNodes <$> ask

-- The currently executing node.
myNode :: CommM NodeId
myNode = s_myNode <$> ask

-- True iff the currently executing node is the main node.
isMain :: CommM Bool
isMain = s_isMain <$> ask

-- internal use only: queue of received payload messages
msgQ :: CommM MessageQ
msgQ = s_msgQ <$> ask

-- |Internal use only: debug level
debug :: CommM Int
debug = debugLvl <$> s_conf <$> ask

-- |Block until receiving a shutdown signal.
waitShutdown :: CommM ()
waitShutdown = do
  mvar <- s_shutdown <$> ask
  liftIO $ takeMVar mvar
  shutdownTransport

-- |Called from 'waitShutdown', this closes
--  all connections, the local endpoint, and
--  then the transport layer, in that order
shutdownTransport :: CommM ()
shutdownTransport = do
{-
  -- Kill all connections
  liftIO $ killConnections <$> connectionLookup
  -- Kill my endpoint
  liftIO $ NT.closeEndPoint <$> myEndPoint
  -- Kill the transport layer
  trans <- liftIO lclTransport
  liftIO $ NT.closeTransport trans
-}
  liftIO shutdownTransportIO

-- |Used in an unclean shutdown when
--  the connection with the master node
--  has been unexpectedly closed.
shutdownTransportIO :: IO ()
shutdownTransportIO = do
  -- Kill all connections
--  remoteConnections <- connectionLookup
--  killConnections remoteConnections
  killConnections =<< connectionLookup
  -- Kill my endpoint
--  myEP <- myEndPoint
--  NT.closeEndPoint myEP
  NT.closeEndPoint =<< myEndPoint
  -- Kill the transport layer
--  trans <- lclTransport
--  NT.closeTransport trans
  NT.closeTransport =<< lclTransport

-- |Used during shutdown, to close
--  connections with all processes safely
killConnections :: Map.Map NodeId NT.Connection -> IO ()
killConnections remoteConnections = do
  let nodes = Map.keys remoteConnections
  mapM_ killConn nodes
  where
   killConn node = do
    let remoteConnection = fromJust $ Map.lookup node remoteConnections
    NT.close remoteConnection

-- |Initiate a shutdown from master process
shutdown :: CommM ()
shutdown = do
  targets <- allNodes
  -- broadcast Shutdown message to all nodes but sender
  liftIO $ broadcastMsg targets Shutdown

-- |Connection with the master process has been closed
--  unexpectedly, close transport layer
uncleanShutdown :: IO ()
uncleanShutdown = do
#ifdef HDPH_DEBUG
  dbg "Shutting down as main process died"
#endif
  -- The main process has terminated. Let's clean up.
  shutdownTransportIO
  exitWith (ExitFailure 9) -- force process termination

-----------------------------------------------------------------------------
-- running the CommM monad

-- Run the given 'CommM' action in the IO monad with the given config data
-- (which determines the debug level, see module HdpH.Internal.Location).
-- The 'action' must call 'waitShutdown' before it terminates on all nodes,
-- and at least one node must call 'shutdown'.
run_ :: RTSConf -> CommM () -> IO ()
run_ conf action = do

#ifdef HDPH_DEBUG
  dbg "run_.1"
#endif

  -- check debug level
  let debugLevel = debugLvl conf
  unless (debugLevel >= dbgNone) $
    Prelude.error "HdpH.Internal.Comm_MPI.run_: debug level < none"

  -- set debug level in HdpH.Internal.State.Location
  writeIORef debugRef debugLevel

#ifdef HDPH_DEBUG
  dbg "run_.2"
#endif


#ifdef HDPH_DEBUG
  dbg "run_.3"
#endif

  myIP <- discoverMyIP conf -- uses network-info for local IP address identification

  -- Networking step 1: Create a transport
  transport <- tryCreateTransport myIP conf
  atomicModifyIORef lclTransportRef (\r -> (transport,r))

  -- Networking step 2: Create a local endpoint; write to myEndPointRef IORef
  Right myEP <- NT.newEndPoint transport
  let me = NT.address myEP
  -- Sets 'myEndPointRef'. Receive events from endpoint in transport layer
  atomicModifyIORef myEndPointRef (\r -> (myEP,r))
  (allNodes,main) <- nodeInfo conf
  let iAmMain = me == main

  -- set node ID in HdpH.Internal.State.Location
  atomicModifyIORef myNodeRef (\r -> (me,r))

#ifdef HDPH_DEBUG
  dbg "run_.4"
#endif

#ifdef HDPH_DEBUG
  dbg "run_.5"
#endif

  -- create initial state
  q <- newChan
  startBarrier <- newEmptyMVar -- (not used in TCP backend)
  stopBarrier  <- newEmptyMVar

#ifdef HDPH_DEBUG
  dbg "run_.6"
#endif

#ifdef HDPH_DEBUG
  dbg $ "run_.7 receiveServerTid = "
#endif

  if iAmMain
         then do
#ifdef HDPH_DEBUG
                 dbg "run_.7.root"
#endif
                 -- Networking step 4: Create NodeId's for all nodes
                 -- including endpoint address info. Also, create
                 -- connections between local endpoint and endpoint addresses
                 -- of all nodes. Write connections map to connectionlookupRef
                 -- and all NodeId's to allNodesRef
                 nodeConnections <- remoteEndPointAddrMap allNodes
                 atomicModifyIORef connectionLookupRef (\r -> (nodeConnections,r))
                 
                 recvAllReady (length allNodes - 1 ) -- blocking
                 broadcastMsg (allNodes \\ [me]) Booted
                 atomicModifyIORef mainEndpointAddrRef (const (myEP, ()))

         else do
#ifdef HDPH_DEBUG
                 dbg "run_.7.other"
#endif
                 -- See networking step 4, above.
                 let mainEP = main
                 nodeConnections <- remoteEndPointAddrMap allNodes
                 atomicModifyIORef connectionLookupRef (const (nodeConnections, ()))
                 atomicModifyIORef mainEndpointAddrRef (const (mainEP, ()))

                 -- Tells master that this node is ready
                 broadcastMsg [main] Ready
                 
                 -- waits for `Booted', means that main is connected to all nodes
                 waitForBootstrapConfirmation

  let s0 = State { s_conf     = conf,
                   s_nodes    = length allNodes,
                   s_allNodes = allNodes,
                   s_myNode   = me,
                   s_isMain   = iAmMain,
                   s_msgQ     = q,
                   s_shutdown = stopBarrier }

#ifdef HDPH_DEBUG
  dbg "run_.8"
#endif
  forkIO $ receiveServer q startBarrier stopBarrier
#ifdef HDPH_DEBUG
  dbg "run_.8b"
#endif
  -- run monad
  runReaderT action s0

#ifdef HDPH_DEBUG
  dbg "run_.9"
#endif

  -- reset HdpH.Internal.State.Location
  atomicModifyIORef myNodeRef (\r -> (throw NodeIdUnset,r))
  writeIORef debugRef dbgNone
  
#ifdef HDPH_DEBUG
  dbg "run_.10"
#endif

-----------------------------------------------------------------------------
-- internal messages

data Msg = Startup          -- startup completed message (main -> other)
         | Shutdown         -- shutdown system message (main -> other)
         | Booted
         | Ready
         | Payload Message  -- non-system message; arg (payload) to be queued
           deriving (Eq, Ord, Show)  -- Show inst only for debugging


instance NFData Msg where
  rnf Startup         = ()
  rnf (Booted)        = ()
  rnf (Ready)         = ()
  rnf (Shutdown)      = ()
  rnf (Payload work)     = rnf work


instance Data.Serialize.Serialize Msg where
  put Startup         = Data.Serialize.put (0 :: Word8)
  put (Booted)        = Data.Serialize.put (1 :: Word8)
  put (Ready)         = Data.Serialize.put (2 :: Word8)
  put (Shutdown)      = Data.Serialize.put (3 :: Word8)
  put (Payload work)  = Data.Serialize.put (4 :: Word8) >>
                        Data.Serialize.put work

  get = do tag <- Data.Serialize.get
           case tag :: Word8 of
             0 -> do return $ Startup
             1 -> do return $ Booted
             2 -> do return $ Ready
             3 -> do return $ Shutdown
             4 -> do work  <- Data.Serialize.get
                     return $ Payload work


-----------------------------------------------------------------------------
-- sending messages (incl system messages)

-- Send a payload message.
send :: NodeId -> Message -> CommM ()
send dest message = lift $ send_ dest message

send_ :: NodeId -> Message -> IO ()
send_ dest message = do
  result <- try $ do
   remoteConnections <- connectionLookup
   let conn = fromJust $ Map.lookup dest remoteConnections
   -- Actual type of 'send' is Either (NT.FailedWith NT.SendErrorCode) ()
   NT.send conn (Lazy.toChunks (encodeLazy (Payload message)))
  case result of
   Left (e::SomeException) -> void (print e)
   Right _ -> return ()


-- This needs to be separate because in `send_' (Payload _)
-- constructor is automatically added.
broadcastMsg :: [NodeId] -> Msg -> IO ()
broadcastMsg dests msg =
  mapM_ broadcastMsg' dests
   where
    serialized_msg = encodeLazy msg
    broadcastMsg' dest = do
     result <- try $ do
      remoteConnections <- connectionLookup
      let conn = fromJust $ Map.lookup dest remoteConnections
      -- Actual type of 'send' is Either (NT.FailedWith NT.SendErrorCode) ()
      _ <- NT.send conn (Lazy.toChunks serialized_msg)
      return ()
     case result of
      Left (e::SomeException) -> void (print e)
      Right _ -> return ()


-----------------------------------------------------------------------------
-- receiving messages (incl system messages and message queue server)

-- Block to receive a message;
-- the sender must be encoded into the message, otherwise it is unknown.
receive :: CommM Message
receive = do q <- msgQ
             liftIO $ readChan q


recv :: IO Msg
recv = do
     ep <- myEndPoint
     event <- NT.receive ep
     case event of
       NT.Received _ msg ->  return ((force . decodeLazy . Lazy.fromChunks) msg)
       NT.ErrorEvent (NT.TransportError e _) ->
         case e of
           NT.EventConnectionLost ep -> do
               mainEP <- mainEndpointAddr
               -- Let's check if the main node has died.
               -- If it has, we should give up.
               if mainEP == ep then do
                 -- Main process has terminated prematurely. Fatal.
                 uncleanShutdown
                 return Shutdown
                else do
 {- Enabled in ft-scheduler branch
                 -- Send a message to scheduler
                 remoteConnections <- connectionLookup
                 let x = Map.filterWithKey (\node _ -> ep == node) remoteConnections
                     deadNode = head $ Map.keys x -- should only be one
                     msg = Payload $ encodeLazy (Payload.DEADNODE deadNode)
                 return msg
-}
                 recv
           _ -> recv  -- links to "case e of"
       _ -> do        -- links to "case event of"
          -- ignore remaining NT.Event constructors for now
          -- i.e. [ConnectionClosed,ConnectionOpened,ReceivedMulticast,EndPointClosed]
          recv

-- Non-terminating computation, to be run in a separate thread.
-- Continually receives message, which it puts into the given
-- message queue or handles immediately (in the case of system messages).
-- * 'Shutdown' unblocks the shutdown barrier (thus terminating all actions).
receiveServer :: MessageQ -> MVar () -> MVar () -> IO ()
receiveServer q startBarrier stopBarrier = do
  hdl <- recv
  handleMsg hdl  -- NOTE: Changed from previous 'forkIO $ handleMsg hdl'
    -- Rationale: 'handleMsg' should be sufficiently lazy to run sequentially',
    -- plus it poses less danger of corruption on shutdown
  where 
    handleMsg hdl =
      -- receive message and dispatch on constructor
      case hdl of
        -- 'Startup' not used in TCP backend            
        Startup ->        receiveServer q startBarrier stopBarrier
        Shutdown ->        -- lift shutdown barrier
                          putMVar stopBarrier ()
        Payload message -> -- queue the payload
                          do  writeChan q message
                              receiveServer q startBarrier stopBarrier
        _ -> error $ "Unexpected message in `receiveServer' " ++ show hdl


-----------------------------------------------------------------------------
-- debugging

#ifdef HDPH_DEBUG
dbg :: String -> IO ()
dbg s = do
  hPutStrLn stderr $ ": HdpH.Internal.Comm_TCP." ++ s
#endif

-- |Used to setup and store a Map of NodeId -> NT.Connection
--  And also, creates a list of [NodeId] that is written
--  to the allNodesRef IORef
remoteEndPointAddrMap :: [NodeId] -> IO (Map.Map NodeId NT.Connection)
remoteEndPointAddrMap nodes = do
  mvar <- newMVar Map.empty -- to store connections
  mapM_ (connectToAllNodes mvar) nodes
  takeMVar mvar

------
-- Start up utilities

-- |Before the 'receiveServer' function is forked,
--  each node must receive a Booted payload from the master process.
--  This indicates that all nodes have sent an ALIVE payload to the master process.
waitForBootstrapConfirmation ::  IO ()
waitForBootstrapConfirmation = do
  msg <- recv
  case msg of
   Booted -> return ()
   _ -> waitForBootstrapConfirmation -- Hangover from UDP broadcasts, still wait for `Booted'

-- |Writing into an MVar the connection that has been
-- made with the remote node, to be written into
-- the connectionLookupRef IORef
connectToAllNodes :: MVar (Map.Map NodeId NT.Connection) -> NodeId -> IO ()
connectToAllNodes mvar remoteNode = do
   myEP <- myEndPoint
   x <- NT.connect myEP remoteNode NT.ReliableOrdered NT.defaultConnectHints
   case x of
    (Right newConnection) ->
      modifyMVar_ mvar $ \m ->
       return $ Map.insert remoteNode newConnection m
    (Left _) ->  connectToAllNodes mvar remoteNode -- keep retrying

-- |'action' is only executed once the master node
--  has receve NT.ConnectionOpened from all other nodes
recvAllReady :: Int -> IO ()
recvAllReady i =
  when (i > 0) $ do
   msg <- recv
   case msg of
     Ready -> recvAllReady (i-1)
     _ -> putStrLn $ "unexpected msg in recvAllReady: " ++ show msg


---------------------------
-- Transport layer creation

tryCreateTransport :: IPv4 -> RTSConf -> IO NT.Transport
tryCreateTransport myIP conf =
  createTrans myIP (numProcs conf) 0

createTrans :: IPv4 -> Int -> Int -> IO NT.Transport
createTrans myIP tasks attempts = do
  rndsock <- genRandomSocket
  t <- TCP.createTransport (show myIP) (show rndsock) TCP.defaultTCPParameters
  case t of
   Right transport -> return transport
   Left e -> do
    let attempts' = attempts+1
    if attempts' == tasks then error ("Error creating transport: " ++ show e)
     else do 
      createTrans myIP tasks attempts'

-----------------------
-- UDP based Node discovery

nodeInfo :: RTSConf ->  IO (SlaveNodes, MainNode)
nodeInfo conf = do
  _ <- forkIO $ broadcastTimeout 10000000
  all <- findSlaves (numProcs conf)
  let mainNode = head (sort all) -- election protocol
  return (all,mainNode)
   where
    broadcastTimeout i = do
     -- broadcast endpoint address via UDP for 10 seconds
     _ <- timeout i broadcastMyNode
     return ()

discoverMyIP :: RTSConf -> IO IPv4
discoverMyIP conf = do
  ns <- getNetworkInterfaces
  return $ myIP ns (networkInterface conf)

myIP :: [NetworkInterface] -> String -> IPv4
myIP interfaces interfaceName =
  let eth = filter (\x -> name x == interfaceName) interfaces
  in ipv4 $ head eth

type MainNode = NodeId
type SlaveNodes = [NodeId]

broadcastMyNode :: IO ()
broadcastMyNode = do
  myEP <- myEndPoint
  forever $ do
   (sock, addr) <- multicastSender "224.0.0.99" 9999
   sendTo sock (NT.endPointAddressToByteString (NT.address myEP)) addr
   threadDelay 100000

findSlaves :: Int -> IO SlaveNodes
findSlaves numNodesExpected = do
  sock <- multicastReceiver "224.0.0.99" 9999
  listenForNodes sock [] numNodesExpected
  
listenForNodes :: Socket -> SlaveNodes -> Int -> IO SlaveNodes
listenForNodes sock ns expected = do
    (msg, _) <- recvFrom sock 1024
    let remoteEndPointAddr = NT.EndPointAddress msg
    let n = if remoteEndPointAddr `elem` ns then [] else [remoteEndPointAddr]
        ns' = n ++ ns
    if length ns' == expected then return ns'
     else listenForNodes sock ns' expected

genRandomSocket :: IO Int
genRandomSocket = do
  gen <- newStdGen
  return $ head (randomRs (8000,40000) gen)

------
-- Enpoint and connection lookup IORefs

myEndPoint :: IO NT.EndPoint
myEndPoint = readIORef myEndPointRef

myEndPointRef :: IORef NT.EndPoint
myEndPointRef = unsafePerformIO $ newIORef $ throw NodeIdUnset
{-# NOINLINE myEndPointRef #-}    -- required to protect unsafePerformIO hack

connectionLookup :: IO (Map.Map NodeId NT.Connection)
connectionLookup = readIORef connectionLookupRef

connectionLookupRef :: forall k a. IORef (Map.Map k a)
connectionLookupRef = unsafePerformIO $ newIORef Map.empty
{-# NOINLINE connectionLookupRef #-}    -- required to protect unsafePerformIO hack

-- Used to watch when main node has failed.
-- If main node fails, shutdown transport layer and terminate.
mainEndpointAddr :: forall a. IO a
mainEndpointAddr = readIORef mainEndpointAddrRef

mainEndpointAddrRef :: forall a. IORef a
mainEndpointAddrRef = unsafePerformIO $ newIORef $ throw NodeIdUnset
{-# NOINLINE mainEndpointAddrRef #-}    -- required to protect unsafePerformIO hack

lclTransport :: IO NT.Transport
lclTransport = readIORef lclTransportRef

lclTransportRef :: IORef NT.Transport
lclTransportRef = unsafePerformIO $ newIORef $ throw NodeIdUnset
{-# NOINLINE lclTransportRef #-}    -- required to protect unsafePerformIO hack
