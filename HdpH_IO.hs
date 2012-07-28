-- HdpH_IO programming interface
--
-- This interface reproduces HdpH functionality in the IO monad rather
-- than in Par monad. Currently, only explicit work placement and local
-- and global IVars are supported; work distribution through sparks and
-- fishing should be added later. In the current version, functionality
-- is similar to Eden's EDI language, except HdpH_IO does not require a
-- bespoke runtime system.
--
-- Visibility: public
-- Author: Patrick Maier <P.Maier@hw.ac.uk>
-- Created: 31 Oct 2011
--
-----------------------------------------------------------------------------

{-# LANGUAGE GeneralizedNewtypeDeriving #-}  -- for 'GIVar'
{-# LANGUAGE TemplateHaskell #-}             -- for mkClosure, etc

module HdpH_IO
  ( -- * invoking the system
    withHdpH,  -- :: IO a -> IO (Maybe a)
    withHdpH_, -- :: IO () -> IO ()

    -- * locations
    NodeId,    -- :: *; insts: Eq, Ord, Show, NFData, Serialize
    myNode,    -- :: IO NodeId
    allNodes,  -- :: IO [NodeId]

    -- * explicit work placement
    pushTo,    -- :: Closure (IO ()) -> NodeId -> IO ()
    delay,     -- :: IO () -> IO ()

    -- * local IVars
    IVar,      -- :: * -> *; instances: none
    new,       -- :: IO (IVar a)
    put,       -- :: IVar a -> a -> IO ()
    get,       -- :: IVar a -> IO a
    poll,      -- :: IVar a -> IO (Maybe a)
    probe,     -- :: IVar a -> IO Bool

    -- * global IVars
    GIVar,     -- :: * -> *; insts: Eq, Ord, Show, NFData, Serialize
    at,        -- :: GIVar a -> NodeId
    glob,      -- :: IVar (Closure a) -> IO (GIVar (Closure a))
    rput,      -- :: GIVar (Closure a) -> Closure a -> IO ()

    -- * explicit Closures
    module HdpH.Closure,

    -- * Static declaration for internal Closures in HdpH_IO and HdpH.Closure
    declareStatic   -- :: StaticDecl
  ) where

import Prelude hiding (error)
import Control.Concurrent (forkIO, killThread)
import Control.Concurrent.MVar
       (MVar, newMVar, newEmptyMVar, putMVar, takeMVar, readMVar)
import Control.DeepSeq (NFData, rnf)
import Control.Exception
       (throw, bracketOnError, block) -- TODO: rep 'block' by 'mask_' in GHC 7
import Control.Monad (unless, when, join)
import Control.Parallel (par)
import Data.Functor ((<$>))
import Data.IORef (IORef, newIORef, readIORef, writeIORef, atomicModifyIORef)
import Data.List ((\\))
import Data.Maybe (isJust, fromJust)
import Data.Monoid (mconcat)
import Data.Serialize (Serialize)
import qualified Data.Serialize (put, get)
import Data.Word (Word8)
import System.IO.Unsafe (unsafePerformIO)

import qualified MP.MPI as MPI
       (Rank, rank0, myRank, allRanks, send, recv, getMsg)

import HdpH.Closure hiding (declareStatic)  -- re-export almost whole module
import qualified HdpH.Closure (declareStatic)
import HdpH.Internal.GRef (GRef)
import qualified HdpH.Internal.GRef as GRef (at, globalise, free, withGRef)
import HdpH.Internal.Location
       (NodeId, rank, mkMyNodeId, myNode, allNodes, error,
        MyNodeException(NodeIdUnset))
import HdpH.Internal.Misc (encodeLazy, decodeLazy)
import HdpH.Internal.State.Location (myNodeRef, allNodesRef)


-----------------------------------------------------------------------------
-- Static declaration

declareStatic :: StaticDecl
declareStatic = mconcat
  [HdpH.Closure.declareStatic,
   declare $(static 'rput_abs)]


-----------------------------------------------------------------------------
-- node IDs (most stuff imported from HdpH.Internal.Location)

isMain :: IO Bool
isMain = do
  myRank <- MPI.myRank
  return $ myRank == MPI.rank0


-----------------------------------------------------------------------------
-- IVars (realised via MVars)

data IVar a = IVar { ready :: MVar (),          -- signal availability of value
                     value :: MVar (Maybe a) }  -- MVar serves as write lock

-- Invariants:
-- * If 'ready' is full then 'value' stores 'Just x' for some 'x'.
-- * If 'ready' is full it stays full (ie. no one blocks taking 'ready').
-- * Likewise, if 'value' stores 'Just x' it stays like that.
-- * 'value' is always full (ie. no one blocks taking 'value').


-- IVar creation
new :: IO (IVar a)
new = do sig <- newEmptyMVar
         val <- newMVar Nothing
         return $ IVar { ready = sig, value = val }


-- blocking read from IVar
get :: IVar a -> IO a
get v = do readMVar (ready v)               -- block waiting for 'ready'
           fromJust <$> readMVar (value v)  -- read and extract value


-- polling for the value of an IVar; non-blocking
poll :: IVar a -> IO (Maybe a)
poll v = readMVar (value v)  -- non-blocking because 'value' is always full


-- probing whether an IVar is full; non-blocking
probe :: IVar a -> IO Bool
probe v = isJust <$> poll v


-- write to IVar (but don't force any evaluation)
put :: IVar a -> a -> IO ()
put v x =
  block $ do
    maybe_val <- takeMVar (value v)
    -- due to 'block' above, uninterruptible once 'takeMVar' has succeeded
    case maybe_val of
      Nothing -> do putMVar (value v) (Just x)   -- 1st write: put value
                    putMVar (ready v) ()         --   then signal 'ready'
      Just _  -> do putMVar (value v) maybe_val  -- 2nd write: put value back


{-
-----------------------------------------------------------------------------
-- IVars (realised via MVars and IORefs)

-- NOTE: This implementation should be more efficient (only 1 MVar operation
--       per put or get) but relies on an implicitly provided memory barrier
--       (which could be problematic in multicores).

data IVar a = IVar { wlock :: MVar (),          -- lock for write operations
                     ready :: MVar (),          -- signal availability of value
                     value :: IORef (Maybe a) }

-- Invariants:
-- * If 'ready' is full then 'value' stores 'Just x' for some 'x'.
-- * If 'ready' is full it stays full (ie. no one blocks taking 'ready').
-- * Likewise, if 'value' stores 'Just x' it stays like that.
-- * 'wlock' is always full (ie. no one blocks taking 'wlock').


new :: IO (IVar a)
new = do lock <- newMVar ()
         sig  <- newEmptyMVar
         val  <- newIORef Nothing
         return $ IVar { wlock = lock, ready = sig, value = val }


get :: IVar a -> IO a
get v = do readMVar (ready v)                -- block waiting for 'ready';
           fromJust <$> readIORef (value v)  -- then read value


put :: IVar a -> a -> IO ()
put v x = withMVar (wlock v) $ \ _ -> do
            maybe_val <- readIORef (value v)
            case maybe_val of
              Just _  -> return ()  -- 2nd write attempt: fail silently
              Nothing -> do -- 1st write attempt:
                -- For correctness, this code would need to run uninterrupted
                -- (it is guaranteed not to block) and there would need to be
                -- a memory barrier after the 'writeIORef'.
                writeIORef (value v) (Just x)  -- write value
                putMVar (ready v) ()           -- signal 'ready'
-}


-----------------------------------------------------------------------------
-- Global IVars

newtype GIVar a = GIVar { unGIVar :: GRef (IVar a) }
                  deriving (Eq, Ord, NFData, Serialize)

-- Show instance (mainly for debugging)
instance Show (GIVar a) where
  showsPrec _ (GIVar gref) = showString "GIVar:" . shows gref


-- Host of global IVar
at :: GIVar a -> NodeId
at = GRef.at . unGIVar


-- globalise IVar (of Closure type)
glob :: IVar (Closure a) -> IO (GIVar (Closure a))
glob v = GIVar <$> GRef.globalise v


-- remote write to global IVar (of Closure type)
rput :: GIVar (Closure a) -> Closure a -> IO ()
rput gv clo = pushTo $(mkClosure [| rput_abs (gv, clo) |]) (at gv)

-- write to locally hosted global IVar; don't export
{-# INLINE rput_abs #-}
rput_abs :: (GIVar (Closure a), Closure a) -> IO ()
rput_abs (GIVar gref, clo) = do
  GRef.withGRef gref (\ v -> put v clo) (return ())  -- ignore dead 'gref'
  GRef.free gref                                     -- free 'gref' async'ly


-----------------------------------------------------------------------------
-- explicit work placement

-- NOTE: Work placement will always send a message, even if sender and
-- destination are the same node.

-- Push 'work' to the given node (for eager execution).
-- The 'work' must not block; otherwise the message handler may block or crash.
pushTo :: Closure (IO ()) -> NodeId -> IO ()
pushTo work dest = send dest (PUSH work)


-----------------------------------------------------------------------------
-- messages

data Msg = STARTUP             -- startup system msg (double ring broadcast)
             [NodeId]
         | SHUTDOWN            -- shutdown system msg (star broadcast)
         | PUSH                -- eager work placement (peer to peer)
             (Closure (IO ()))


-- Show instance (mainly for debugging)
instance Show Msg where
  showsPrec _ (STARTUP nodes) = showString "STARTUP(" . shows nodes .
                                showString ")"
  showsPrec _ (SHUTDOWN)      = showString "SHUTDOWN"
  showsPrec _ (PUSH _work)    = showString "PUSH(_)"


instance NFData Msg where
  rnf (STARTUP nodes) = rnf nodes
  rnf (SHUTDOWN)      = ()
  rnf (PUSH work)     = rnf work


instance Serialize Msg where
  put (STARTUP nodes) = Data.Serialize.put (0 :: Word8) >>
                        Data.Serialize.put nodes
  put (SHUTDOWN)      = Data.Serialize.put (1 :: Word8)
  put (PUSH work)     = Data.Serialize.put (2 :: Word8) >>
                        Data.Serialize.put work

  get = do tag <- Data.Serialize.get
           case tag :: Word8 of
             0 -> do nodes <- Data.Serialize.get
                     return $ STARTUP nodes
             1 -> do return $ SHUTDOWN
             2 -> do work  <- Data.Serialize.get
                     return $ PUSH work


-- NOTE: Not optimised for the case when 'dest' is sender;
--       message will go through MPI layer regardless.
send :: NodeId -> Msg -> IO ()
send dest msg = MPI.send (rank dest) (encodeLazy msg)


-- NOTE: Sends msg to all nodes incl sender node; msg to sender is sent last.
broadcast :: Msg -> IO ()
broadcast msg = do
  let serialised_msg = encodeLazy msg
  myRank <- MPI.myRank
  allRanks <- MPI.allRanks
  mapM_ (\ dest -> MPI.send dest serialised_msg) (allRanks \\ [myRank])
  MPI.send myRank serialised_msg  -- send msg to myself last


receive :: IO Msg
receive = decodeLazy <$> (MPI.getMsg =<< MPI.recv)


-----------------------------------------------------------------------------
-- RTS (including shutdown and message handling)

-- NOTE: This RTS is very simplistic. 
--       * RTS relies on MPI message queue rather than implementing its own;
--         may therefore be vulnerable to MPI buffer overruns. 
--       * RTS does not avoid serialisation of Closures that are pushed
--         to the same node; such optimisation essentially requires
--         RTS' own message queue.


-- Token ring barrier gathering node IDs of all nodes in the virtual machine;
-- Token is STARTUP msg (with list of node IDs) and travels round twice,
-- first to collect node IDs, then to distribute them, starting and ending
-- at the main node. The second time the msg is received, a node knows all
-- node IDs in the system.
allgatherNodes :: NodeId -> MPI.Rank -> Bool -> IO [NodeId]
allgatherNodes me prev iAmMain = do
  -- main node starts off first round by sending empty list
  when iAmMain $
    MPI.send prev $ encodeLazy $ STARTUP []

  -- 1st round: receive a list, cons me on and pass it to previous rank
  STARTUP someNodes <- decodeLazy <$> (MPI.getMsg =<< MPI.recv)
  MPI.send prev $ encodeLazy $ STARTUP (me:someNodes)

  -- 2nd round: receive a message and pass it on (except for main node)
  msg <- MPI.getMsg =<< MPI.recv
  unless iAmMain $
    MPI.send prev msg

  -- decode message and return list of all nodes
  let STARTUP allNodes = decodeLazy msg
  return allNodes


-- Broadcast SHUTDOWN message to all nodes; send to executing node last.
shutdown :: IO ()
shutdown = broadcast SHUTDOWN

-- Handle incomming messages:
-- * SHUTDOWN terminates handler.
-- * Work sent by PUSH is either eagerly run inline in the handler,
--   or is run in a newly forked worker IO thread. The former is unsafe
--   and may lead to deadlocks or crashes in case the work blocks.
-- NOTES: * SHUTDOWN does not kill remaining worker threads; arguably it 
--          should but that requires registering each thread.
--        * There is no control over how many threads are started quasi
--          simultaneously; threads are always forked eagerly.


-- Handle incomming messages:
-- * SHUTDOWN terminates handler.
-- * Work sent by PUSH is run eagerly inline in the handler.
-- NOTES: * SHUTDOWN does not kill threads that were created when executing 
--          PUSH messages.
--        * If executing PUSH messages does create threads then there
--          is no control over how many threads are started quasi
--          simultaneously.
handler :: IO ()
handler = do
  msg <- receive
  case msg of
    STARTUP _ -> error $ "HdpH_IO.handler: unexpected msg " ++ show msg
    SHUTDOWN  -> return ()
    PUSH work -> do let action = unClosure work
                    action
                    handler


-- Run 'prog' with HdpH. That is, start 'prog' on the main node (which
-- is the first node in the list returned by 'allNodes').
-- TODO: Check whether 'withHdpH_ prog' can be called twice in a row,
--       and shows the same behaviour.
withHdpH_ :: IO () -> IO ()
withHdpH_ prog = do
  -- init RTS: set own node ID
  me <- mkMyNodeId
  writeIORef myNodeRef me

  -- init RTS: gather all node IDs
  iAmMain <- isMain
  allRanks <- MPI.allRanks
  let prev = head $ tail $ dropWhile (/= rank me) $ cycle (reverse allRanks)
  all <- allgatherNodes me prev iAmMain
  writeIORef allNodesRef all

  -- end of initialisation
  
  if iAmMain 
    then do
     -- main node; first fork 'prog' (should print Static table before)
      bracketOnError
        (forkIO (prog >> shutdown))
        (\ tid -> killThread tid >> shutdown)  -- on error: kill prog, shutdown

        $ \ _ -> do
          -- then run message handler (terminates on receiving SHUTDOWN)
          handler

    else do
      -- other node; only run message handler (until receiving SHUTDOWN)
      handler

  -- print stats (none at the moment)

  -- finalise RTS: reset HdpH.Internal.State.Location
  writeIORef allNodesRef $ []
  writeIORef myNodeRef $ throw NodeIdUnset


-- Convenience: variant of 'withHdpH_' which does return a result
-- (on the main node; all other nodes return Nothing).
withHdpH :: IO a -> IO (Maybe a)
withHdpH prog = do
  res <- newIORef Nothing
  withHdpH_ (prog >>= writeIORef res . Just)
  readIORef res


-----------------------------------------------------------------------------
-- delayed threads

poolRef :: IORef [IO ()]
poolRef = unsafePerformIO $ newIORef []
{-# NOINLINE poolRef #-}

delay :: IO () -> IO ()
delay action =
  join $
    atomicModifyIORef poolRef $ \ actions ->
      case actions of
        [] -> (action:actions, startScheduler)
        _  -> (action:actions, return ())

schedule :: IO ()
schedule =
  join $
    atomicModifyIORef poolRef $ \ actions ->
      case actions of
        act:acts -> (acts, forkIO act >> startScheduler)
        []       -> ([],   return ())

startScheduler :: IO ()
startScheduler = schedule `par` return ()
-- Will this work? After all, schedule is garbage here.
-- Yes, there is a problem.
