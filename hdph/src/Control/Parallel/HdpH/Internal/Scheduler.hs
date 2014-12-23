-- Work stealing scheduler and thread pools
--
-- Author: Patrick Maier
-----------------------------------------------------------------------------

{-# LANGUAGE GeneralizedNewtypeDeriving #-}  -- req'd for type 'RTS'
{-# LANGUAGE ScopedTypeVariables #-}         -- req'd for type annotations

module Control.Parallel.HdpH.Internal.Scheduler
  ( -- * abstract run-time system monad
    RTS,          -- instances: Monad, Functor
    run_,         -- :: RTSConf -> RTS () -> IO ()
    liftThreadM,  -- :: ThreadM RTS a -> RTS a
    liftSparkM,   -- :: SparkM RTS a -> RTS a
    liftIO,       -- :: IO a -> RTS a
    forkStub,     -- :: RTS () -> RTS ThreadId

    -- * scheduler ID
    schedulerID,  -- :: RTS Int

    -- * converting and executing threads
    mkThread,     -- :: ParM RTS a -> Thread RTS
    execThread,   -- :: Thread RTS -> RTS ()
    execHiThread, -- :: Thread RTS -> RTS ()

    -- * pushing sparks
    sendPUSH      -- :: Spark RTS -> Node -> RTS ()
  ) where

import Prelude hiding (error)
import Control.Applicative (Applicative)
import Control.Concurrent (ThreadId, forkIO, killThread)
import Control.Concurrent.MVar (MVar, newEmptyMVar, takeMVar, tryPutMVar)
import Control.Monad (unless, when, void)
import Data.Functor ((<$>))

import Control.Parallel.HdpH.Closure (unClosure)
import Control.Parallel.HdpH.Conf (RTSConf(scheds, wakeupDly))
import qualified Control.Parallel.HdpH.Internal.Comm as Comm
       (myNode, allNodes, isRoot, send, receive, withCommDo)
import qualified Control.Parallel.HdpH.Internal.Data.Deque as Deque (emptyIO)
import qualified Control.Parallel.HdpH.Internal.Data.Sem as Sem
       (new, signalPeriodically)
import Control.Parallel.HdpH.Internal.Location
       (Node, dbgStats, dbgMsgSend, dbgMsgRcvd, error)
import qualified Control.Parallel.HdpH.Internal.Location as Location (debug)
import Control.Parallel.HdpH.Internal.Misc
       (encodeLazy, decodeLazy, ActionServer, newServer, killServer)
import Control.Parallel.HdpH.Internal.Sparkpool
       (SparkM, blockSched, getLocalSpark, Msg(TERM,PUSH), dispatch,
        readFishSentCtr, readSparkRcvdCtr, readSparkGenCtr,
        readMaxSparkCtrs)
import qualified Control.Parallel.HdpH.Internal.Sparkpool as Sparkpool (run)
import Control.Parallel.HdpH.Internal.Threadpool
       (ThreadM, poolID, forkThreadM, stealThread, readMaxThreadCtrs)
import qualified Control.Parallel.HdpH.Internal.Threadpool as Threadpool
       (run, liftSparkM, liftIO)
import Control.Parallel.HdpH.Internal.Type.Par
       (ParM, unPar, Thread(Atom), ThreadCont(ThreadCont, ThreadDone), Spark)


-----------------------------------------------------------------------------
-- RTS monad

-- The RTS monad hides monad stack (IO, SparkM, ThreadM) as abstract.
newtype RTS a = RTS { unRTS :: ThreadM RTS a }
                deriving (Functor, Monad, Applicative)


-- Fork a new thread to execute the given 'RTS' action; the integer 'n'
-- dictates how much to rotate the thread pools (so as to avoid contention
-- due to concurrent access).
forkRTS :: Int -> RTS () -> RTS ThreadId
forkRTS n = liftThreadM . forkThreadM n . unRTS

-- Fork a stub to stand in for an external computing resource (eg. GAP).
-- Will share thread pool with message handler.
forkStub :: RTS () -> RTS ThreadId
forkStub = forkRTS 0


-- Eliminate the whole RTS monad stack down the IO monad by running the given
-- RTS action 'main'; aspects of the RTS's behaviour are controlled by
-- the respective parameters in the given RTSConf.
-- NOTE: This function start various threads (for executing schedulers, 
--       a message handler, and various timeouts). On normal termination,
--       all these threads are killed. However, there is no cleanup in the 
--       event of aborting execution due to an exception. The functions
--       for doing so (see Control.Execption) all live in the IO monad.
--       Maybe they could be lifted to the RTS monad by using the monad-peel
--       package.
run_ :: RTSConf -> RTS () -> IO ()
run_ conf main = do
  let n = scheds conf
  unless (n > 0) $
    error "HdpH.Internal.Scheduler.run_: no schedulers"

  -- allocate n+1 empty thread pools (numbered from 0 to n)
  pools <- mapM (\ k -> do { pool <- Deque.emptyIO; return (k,pool) }) [0 .. n]

  -- fork nowork server (for clearing the "FISH outstanding" flag on NOWORK)
  noWorkServer <- newServer

  -- create semaphore for idle schedulers
  idleSem <- Sem.new

  -- fork wakeup server (periodically waking up racey sleeping scheds)
  wakeupServerTid <- forkIO $ Sem.signalPeriodically idleSem (wakeupDly conf)

  -- start the RTS
  Comm.withCommDo conf $
    Sparkpool.run conf noWorkServer idleSem $
      Threadpool.run pools $
        unRTS $
          rts n noWorkServer wakeupServerTid

    where
      -- RTS action
      rts :: Int -> ActionServer -> ThreadId -> RTS ()
      rts n_scheds noWorkServer wakeupServerTid = do
        -- get some data from Comm module
        all_nodes@(me:_) <- liftIO Comm.allNodes
        is_root <- liftIO Comm.isRoot

        -- create termination barrier
        barrier <- liftIO newEmptyMVar

        -- fork message handler (accessing thread pool 0)
        let n_nodes = if is_root then length all_nodes else 0
        handlerTid <- forkRTS 0 (handler barrier n_nodes)

        -- fork schedulers (each accessing thread pool k, 1 <= k <= n_scheds)
        schedulerTids <- mapM (\ k -> forkRTS k scheduler) [1 .. n_scheds]

        -- run main RTS action
        main

        -- termination
        when is_root $ do
          -- root: send TERM msg to all nodes to lift termination barrier
          everywhere <- liftIO Comm.allNodes
          let term_msg = encodeLazy (TERM me)
          liftIO $ mapM_ (\ node -> Comm.send node term_msg) everywhere

        -- all nodes: block waiting for termination barrier
        liftIO $ takeMVar barrier

        -- print stats
        printFinalStats

        -- kill nowork server
        liftIO $ killServer noWorkServer

        -- kill wakeup server
        liftIO $ killThread wakeupServerTid

        -- kill message handler
        liftIO $ killThread handlerTid

        -- kill schedulers
        liftIO $ mapM_ killThread schedulerTids


-- lifting lower layers
liftThreadM :: ThreadM RTS a -> RTS a
liftThreadM = RTS

liftSparkM :: SparkM RTS a -> RTS a
liftSparkM = liftThreadM . Threadpool.liftSparkM

liftIO :: IO a -> RTS a
liftIO = liftThreadM . Threadpool.liftIO


-- Return scheduler ID, that is ID of scheduler's own thread pool.
schedulerID :: RTS Int
schedulerID = liftThreadM poolID


-----------------------------------------------------------------------------
-- cooperative scheduling

-- Converts 'Par' computations into threads (of whatever priority).
mkThread :: ParM RTS a -> Thread RTS
mkThread p = unPar p $ \ _c -> Atom (\ _ -> return $ ThreadDone [])


-- Execute the given (low priority) thread until it blocks or terminates.
execThread :: Thread RTS -> RTS ()
execThread = runThread (return ())

-- Execute the given (high priority) thread until it and all its high
-- priority descendents block or terminate.
execHiThread :: Thread RTS -> RTS ()
execHiThread = runHiThreads (return ()) []


-- Try to get a thread from a thread pool or the spark pool and execute it
-- (with low priority) until it blocks or terminates, whence repeat forever;
-- if there is no thread to execute then block the scheduler (ie. its
-- underlying IO thread).
scheduler :: RTS ()
scheduler = getThread >>= runThread scheduler


-- Try to steal a thread from any thread pool (with own pool preferred);
-- if there is none, try to convert a spark from the spark pool;
-- if there is none too, block the scheduler such that the 'getThread'
-- action will be repeated on wake up.
-- NOTE: Sleeping schedulers should be woken up
--       * after new threads have been added to a thread pool,
--       * after new sparks have been added to the spark pool, and
--       * once the delay after a NOWORK message has expired.
getThread :: RTS (Thread RTS)
getThread = do
  schedID <- schedulerID
  maybe_thread <- liftThreadM stealThread
  case maybe_thread of
    Just thread -> return thread
    Nothing     -> do
      maybe_spark <- liftSparkM $ getLocalSpark schedID
      case maybe_spark of
        Just spark -> return $ mkThread $ unClosure spark
        Nothing    -> liftSparkM blockSched >> getThread


-- Execute given (low priority) thread until it blocks or terminates,
-- whence action 'onTerm' is executed.
-- NOTE: Any high priority threads arising during the execution of 'runThread'
--       are executed immediately by a call to 'runHiThreads'.
runThread :: RTS () -> Thread RTS -> RTS ()
runThread onTerm (Atom m) = do
  x <- m False  -- action 'm' executed in low priority context
  case x of
    ThreadCont (ht:hts) t -> runHiThreads (runThread onTerm t) hts ht
    ThreadCont []       t -> runThread onTerm t
    ThreadDone (ht:hts)   -> runHiThreads onTerm hts ht
    ThreadDone []         -> onTerm

-- Execute given high priority thread and given stack of such threads
-- until they all block or terminate, whence action 'onTerm' is executed.
runHiThreads :: RTS () -> [Thread RTS] -> Thread RTS -> RTS ()
runHiThreads onTerm stack (Atom m) = do
  x <- m True   -- action 'm' executed in high priority context
  case x of
    ThreadCont hts ht -> runHiThreads onTerm (hts ++ stack) ht
    ThreadDone hts    -> case hts ++ stack of
                           ht:hts' -> runHiThreads onTerm hts' ht
                           []      -> onTerm


-----------------------------------------------------------------------------
-- pushed sparks

-- Send a 'spark' via PUSH message to the given 'target' unless 'target'
-- is the current node (in which case 'spark' is executed immediately
-- as a high priority thread).
sendPUSH :: Spark RTS -> Node -> RTS ()
sendPUSH spark target = do
  here <- liftIO Comm.myNode
  if target == here
    then do
      -- short cut PUSH msg locally
      execHiThread $ mkThread $ unClosure spark
    else do
      -- construct and send PUSH message
      let msg = PUSH spark :: Msg RTS
      debug dbgMsgSend $
        show msg ++ " ->> " ++ show target
      liftIO $ Comm.send target $ encodeLazy msg


-- Handle a PUSH message by converting the spark into a high priority thread
-- and executing it immediately.
handlePUSH :: Msg RTS -> RTS ()
handlePUSH (PUSH spark) = execHiThread $ mkThread $ unClosure spark
handlePUSH _ = error "panic in handlePUSH: not a PUSH message"


-- Handle a TERM message, depending on whether this node is root or not.
handleTERM :: MVar () -> Int -> Msg RTS -> RTS Int
handleTERM term_barrier term_count msg@(TERM root) = do
  if term_count == 0
    then do -- non-root node: deflect TERM msg, lift term barrier, term handler
            liftIO $ Comm.send root $ encodeLazy msg
            void $ liftIO $ tryPutMVar term_barrier () 
            return (-1)
    else -- root node
         if term_count > 1
           then do -- at least one TERM msg outstanding: decrement term count
                   return $! (term_count - 1)
           else do -- last TERM msg received: lift term barrier, term handler
                   void $ liftIO $ tryPutMVar term_barrier ()
                   return (-1)
handleTERM _ _ _ = error "panic in handleTERM: not a TERM message"


-----------------------------------------------------------------------------
-- message handler; only PUSH and TERM messages are actually handled here in
-- this module, other messages are relegated to module Sparkpool.

-- Message handler, running continously (in its own thread) receiving
-- and handling messages (some of which may unblock threads or create sparks)
-- as they arrive. Message handler terminates on receiving TERM message(s).
handler :: MVar () -> Int -> RTS ()
handler term_barrier term_count =
  when (term_count >= 0) $ do
    msg <- decodeLazy <$> liftIO Comm.receive
    debug dbgMsgRcvd $
      ">> " ++ show msg
    case msg of
      TERM _ -> handleTERM term_barrier term_count msg >>= handler term_barrier
      PUSH _ -> handlePUSH msg >> handler term_barrier term_count
      _      -> liftSparkM (dispatch msg) >> handler term_barrier term_count


-----------------------------------------------------------------------------
-- auxiliary stuff

-- Print stats (#sparks, threads, FISH, ...) at appropriate debug level.
-- TODO: Log time elapsed since RTS is up
printFinalStats :: RTS ()
printFinalStats = do
  fishes       <- liftSparkM $ readFishSentCtr
  schedules    <- liftSparkM $ readSparkRcvdCtr
  sparks       <- liftSparkM $ readSparkGenCtr
  max_sparks   <- liftSparkM $ readMaxSparkCtrs
  maxs_threads <- liftThreadM $ readMaxThreadCtrs
  debug dbgStats $ "#SPARK=" ++ show sparks ++ "   " ++
                   "max_SPARK=" ++ show max_sparks ++ "   " ++
                   "max_THREAD=" ++ show maxs_threads
  debug dbgStats $ "#FISH_sent=" ++ show fishes ++ "   " ++
                   "#SCHED_rcvd=" ++ show schedules

debug :: Int -> String -> RTS ()
debug level message = liftIO $ Location.debug level message
