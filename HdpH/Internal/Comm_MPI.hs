-- Node to node communication (via MPI)
--
-- Visibility: HdpH, HdpH.Internal
-- Author: Patrick Maier <P.Maier@hw.ac.uk>
-- Created: 06 May 2011
--
-----------------------------------------------------------------------------

{-# LANGUAGE CPP #-}  -- for #ifdef

module HdpH.Internal.Comm_MPI
  ( -- * CommM monad
    CommM,           -- synonym: Control.Monad.Reader.ReaderT <State> IO
    run_,            -- :: RTSConf -> CommM () -> IO ()
    runWithMPI_,     -- :: CommM () -> IO ()
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
import Control.Concurrent (forkIO, killThread)
import Control.Concurrent.Chan (Chan, newChan, readChan, writeChan)
import Control.Concurrent.MVar (MVar, newEmptyMVar, takeMVar, putMVar)
import Control.DeepSeq (NFData(rnf))
import Control.Exception (throw, bracket)
import Control.Monad (when, unless)
import Control.Monad.Reader (ReaderT, runReaderT, ask)
import Control.Monad.Trans (lift)
import Data.Functor ((<$>))
import Data.IORef (writeIORef)
import Data.Maybe (fromMaybe)
import Data.Serialize (Serialize)
import qualified Data.Serialize (put, get)
import Data.Word (Word8)
import System.IO (stderr, hPutStrLn)
import System.Posix.Env (getEnv)

import qualified MP.MPI_ByteString  -- Has to be MPI_ByteString, I'm afraid
       as MPI (Rank, rank0, myRank, allRanks, procs, 
               Msg, MsgHandle, send, recv, getMsg,
               withMPI, initialized, defaultBufferSize)

import HdpH.Conf (RTSConf(debugLvl), defaultRTSConf)
import HdpH.Internal.Misc (encodeLazy, decodeLazy)
import HdpH.Internal.Location (MyNodeException(NodeIdUnset), error, dbgNone)
import HdpH.Internal.Location_MPI (NodeId, rank, mkMyNodeId)
import HdpH.Internal.State.Location (myNodeRef, debugRef)


-----------------------------------------------------------------------------
-- state representation

data State =
  State { s_conf     :: RTSConf,  -- config data
          s_nodes    :: Int,      -- # nodes of virtual machine
          s_allNodes :: [NodeId], -- all nodes of virtual machine
          s_myNode   :: NodeId,   -- currently executing node
          s_isMain   :: Bool,     -- True iff currently executing is main node
          s_msgQ     :: MessageQ, -- queue holding received payload messages
          s_shutdown :: MVar () } -- shutdown signal

-- concurrent message queue (storing payload messages)
type MessageQ = Chan Message

-- a message is an MPI message (here: a lazy byte string)
type Message = MPI.Msg


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

-- internal use only: debug level
debug :: CommM Int
debug = debugLvl <$> s_conf <$> ask

-- Block until receiving a shutdown signal.
waitShutdown :: CommM ()
waitShutdown = liftIO . takeMVar =<< s_shutdown <$> ask

-- Initiate a shutdown.
shutdown :: CommM ()
shutdown = do
  sender  <- myNode
  targets <- allNodes
  -- broadcast Shutdown message to all nodes but sender
  liftIO $ broadcastMsg sender targets Shutdown
  -- lift shutdown barrier on sender
  stopBarrier <- s_shutdown <$> ask
  liftIO $ putMVar stopBarrier ()


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

  -- check whether MPI is initialised
  mpiInit <- MPI.initialized
  unless mpiInit $
    Prelude.error "HdpH.Internal.Comm_MPI.run_: MPI not initialised"

#ifdef HDPH_DEBUG
  dbg "run_.3"
#endif

  -- create own node ID and extract info about ranks from MPI
  myNode <- mkMyNodeId
  let iAmMain = rank myNode == MPI.rank0
  nodes <- MPI.procs
  ranks <- MPI.allRanks

  -- set node ID in HdpH.Internal.State.Location
  writeIORef myNodeRef myNode

#ifdef HDPH_DEBUG
  dbg "run_.4"
#endif

  -- allgather node IDs
  let prev = head $ tail $ dropWhile (/= rank myNode) $ cycle (reverse ranks)
  allNodes <- allgatherNodes myNode prev iAmMain

#ifdef HDPH_DEBUG
  dbg "run_.5"
#endif

  -- create initial state
  q <- newChan
  startBarrier <- newEmptyMVar
  stopBarrier  <- newEmptyMVar
  let s0 = State { s_conf     = conf,
                   s_nodes    = nodes,
                   s_allNodes = allNodes,
                   s_myNode   = myNode,
                   s_isMain   = iAmMain,
                   s_msgQ     = q,
                   s_shutdown = stopBarrier }

#ifdef HDPH_DEBUG
  dbg "run_.6"
#endif

  bracket

    -- fork receiver thread feeding msgQ
    (forkIO $ receiveServer q startBarrier stopBarrier)

    -- kill receiver thread
    killThread $

    -- actual computation
    \ receiveServerTid -> do

#ifdef HDPH_DEBUG
      dbg $ "run_.7 receiveServerTid = " ++ show receiveServerTid
#endif

      if iAmMain
         then do
#ifdef HDPH_DEBUG
                 dbg "run_.7.root"
#endif
                 -- broadcast Startup message (root node)
                 broadcastMsg myNode allNodes Startup
         else do
#ifdef HDPH_DEBUG
                 dbg "run_.7.other"
#endif
                 -- wait for the startup barrier (other nodes)
                 takeMVar startBarrier

#ifdef HDPH_DEBUG
      dbg "run_.8"
#endif

      -- run monad
      runReaderT action s0

#ifdef HDPH_DEBUG
      dbg "run_.9"
#endif

      -- reset HdpH.Internal.State.Location
      writeIORef myNodeRef $ throw NodeIdUnset
      writeIORef debugRef dbgNone

#ifdef HDPH_DEBUG
      dbg "run_.10"
#endif


-- Token ring barrier gathering node IDs of all nodes in the virtual machine;
-- Token is list of node IDs and travels round twice, starting and ending
-- at the main node. The second time the token is received, a node knows
-- all node IDs in the system.
allgatherNodes :: NodeId -> MPI.Rank -> Bool -> IO [NodeId]
allgatherNodes myNode prev iAmMain = do
  -- main node starts off first round by sending empty list
  when iAmMain $
    MPI.send prev $ encodeLazy ([] :: [NodeId])

  -- 1st round: receive a list, cons myNode on and pass it to previous rank
  someNodes <- decodeLazy <$> (MPI.getMsg =<< MPI.recv)
  MPI.send prev $ encodeLazy (myNode:someNodes)

  -- 2nd round: receive a message and pass it on (except for main node)
  msg <- MPI.getMsg =<< MPI.recv
  unless iAmMain $
    MPI.send prev msg

  -- decode message and return list of all nodes
  return $ decodeLazy msg


-----------------------------------------------------------------------------
-- internal messages

data Msg = Startup          -- startup completed message (main -> other)
         | Shutdown         -- shutdown system message (main -> other)
         | Payload Message  -- non-system message; arg (payload) to be queued
           deriving (Eq, Ord, Show)  -- Show inst only for debugging

instance NFData Msg where
  rnf Startup           = ()
  rnf Shutdown          = ()
  rnf (Payload message) = rnf message

instance Serialize Msg where
  put Startup           = Data.Serialize.put (0 :: Word8)
  put Shutdown          = Data.Serialize.put (1 :: Word8)
  put (Payload message) = Data.Serialize.put (2 :: Word8) >>
                          Data.Serialize.put message
  get = do tag <- Data.Serialize.get
           case tag :: Word8 of
             0 -> do return $ Startup
             1 -> do return $ Shutdown
             2 -> do message <- Data.Serialize.get
                     return $ Payload message

-- TODO: Abolish type Msg. Instead have 'Message' for payload messages
--       and 'SysMsg' for system messages. On the MPI layer, use MPI_TAGs to
--       distinguish between them.


-----------------------------------------------------------------------------
-- sending messages (incl system messages)

-- Send a payload message.
send :: NodeId -> Message -> CommM ()
send dest message =
  liftIO $ MPI.send (rank dest) (encodeLazy $ Payload message)


-- Send 'msg' to nodes in 'dests' except 'sender'.
broadcastMsg :: NodeId -> [NodeId] -> Msg -> IO ()
broadcastMsg sender dests msg = mapM_ sendMsg dests where
  serialized_msg = encodeLazy msg
  sendMsg :: NodeId -> IO ()
  sendMsg node | node == sender = return ()  -- skip sender
               | otherwise      = MPI.send (rank node) serialized_msg


-----------------------------------------------------------------------------
-- receiving messages (incl system messages and message queue server)

-- Block to receive a message;
-- the sender must be encoded into the message, otherwise it is unknown.
receive :: CommM Message
receive = do q <- msgQ
             liftIO $ readChan q


-- Non-terminating computation, to be run in a separate thread.
-- Continually receives message (via MPI), which it puts into the given
-- message queue or handles immediately (in the case of system messages).
-- * 'Startup' unblocks the startup barrier (thus enabling all actions);
-- * 'Shutdown' unblocks the shutdown barrier (thus terminating all actions).
receiveServer :: MessageQ -> MVar () -> MVar () -> IO ()
receiveServer q startBarrier stopBarrier = do
  hdl <- MPI.recv
  handleMsg hdl  -- NOTE: Changed from previous 'forkIO $ handleMsg hdl'
    -- Rationale: 'handleMsg' should be sufficiently lazy to run sequentially',
    -- plus it poses less danger of corruption on shutdown
  receiveServer q startBarrier stopBarrier where
    -- Receive and handle a message
    handleMsg :: MPI.MsgHandle -> IO ()
    handleMsg hdl = do
      -- receive message and dispatch on constructor
      msg <- decodeLazy <$> MPI.getMsg hdl
      case msg of
        Startup ->         -- lift startup barrier
                           putMVar startBarrier ()
        Shutdown ->        -- lift shutdown barrier
                           putMVar stopBarrier ()
        Payload message -> -- queue the payload
                           writeChan q message


-----------------------------------------------------------------------------
-- convenience

-- Initialise MPI and run the given 'CommM' action;
-- the restrictions on 'action' mentioned at 'run_' apply verbatim.
-- MPI buffer size and debug level can be set via environment variables
-- COMM_MPI_BUFSIZE and COMM_MPI_DEBUG, respectively.
runWithMPI_ :: CommM () -> IO ()
runWithMPI_ action = do
  let error_pref = "HdpH.Internal.Comm_MPI.runWithMPI_: no parse for "

  -- extracting MPI buffer size from env var COMM_MPI_BUFSIZE
  bufferSize <- readEnv "COMM_MPI_BUFSIZE" (error_pref ++ "COMM_MPI_BUFSIZE")
  let myBufferSize = fromMaybe MPI.defaultBufferSize bufferSize

  -- extracting debug level from env var COMM_MPI_DEBUG
  debugLevel <- readEnv "COMM_MPI_DEBUG" (error_pref ++ "COMM_MPI_DEBUG")
  let myDebugLevel = fromMaybe dbgNone debugLevel

  -- setup config data
  let conf = defaultRTSConf { debugLvl = myDebugLevel }

  -- initialise MPI and run action
  MPI.withMPI myBufferSize $ run_ conf action


-- 'readEnv var error_msg' returns 'Just x' if the environment variable 'var'
-- is defined and its value parses to 'x'; 'Nothing' is returned if 'var' is
-- not defined; if the parse fails then the function aborts with 'error_msg'.
readEnv :: Read a => String -> String -> IO (Maybe a)
readEnv var error_msg = do
  s <- getEnv var
  case s of
    Nothing -> return Nothing
    Just s  -> case reads s of
                 [(x,"")] -> return (Just x)
                 _        -> Prelude.error error_msg


-----------------------------------------------------------------------------
-- debugging

#ifdef HDPH_DEBUG
dbg :: String -> IO ()
dbg s = do
  i <- MPI.myRank
  hPutStrLn stderr $ show i ++ ": HdpH.Internal.Comm_MPI." ++ s
#endif
