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
    liftCommM,    -- :: CommM a -> RTS a
    liftIO,       -- :: IO a -> RTS a

    -- * scheduler ID
    schedulerID,  -- :: RTS Int

    -- * converting and executing threads
    mkThread,     -- :: ParM RTS a -> Thread RTS
    execThread,   -- :: Thread RTS -> RTS ()

    -- * pushing sparks
    sendPUSH      -- :: Spark RTS -> NodeId -> RTS ()
  ) where

import Prelude hiding (error)
import Control.Concurrent (ThreadId, forkIO, killThread)
import Control.Monad (unless, replicateM)
import Data.Functor ((<$>))

import Control.Parallel.HdpH.Closure (unClosure)
import Control.Parallel.HdpH.Conf (RTSConf(scheds, wakeupDly))
import Control.Parallel.HdpH.Internal.Comm (CommM)
import qualified Control.Parallel.HdpH.Internal.Comm as Comm
       (myNode, send, receive, run_, waitShutdown)
import qualified Control.Parallel.HdpH.Internal.Data.Deque as Deque (emptyIO)
import qualified Control.Parallel.HdpH.Internal.Data.Sem as Sem
       (new, signalPeriodically)
import Control.Parallel.HdpH.Internal.Location
       (NodeId, dbgNone, dbgStats, dbgMsgSend, dbgMsgRcvd, error)
import qualified Control.Parallel.HdpH.Internal.Location as Location (debug)
import Control.Parallel.HdpH.Internal.Misc
       (encodeLazy, decodeLazy, ActionServer, newServer, killServer)
import Control.Parallel.HdpH.Internal.Sparkpool
       (SparkM, blockSched, getSpark, Msg(PUSH), dispatch, readPoolSize,
        readFishSentCtr, readSparkRcvdCtr, readSparkGenCtr, readMaxSparkCtr)
import qualified Control.Parallel.HdpH.Internal.Sparkpool as Sparkpool (run)
import Control.Parallel.HdpH.Internal.Threadpool
       (ThreadM, poolID, forkThreadM, stealThread, readMaxThreadCtrs)
import qualified Control.Parallel.HdpH.Internal.Threadpool as Threadpool
       (run, liftSparkM, liftCommM, liftIO)
import Control.Parallel.HdpH.Internal.Type.Par
       (ParM, unPar, Thread(Atom), Spark)


-----------------------------------------------------------------------------
-- RTS monad

-- The RTS monad hides monad stack (IO, CommM, SparkM, ThreadM) as abstract.
newtype RTS a = RTS { unRTS :: ThreadM RTS a }
                deriving (Functor, Monad)


-- Fork a new thread to execute the given 'RTS' action; the integer 'n'
-- dictates how much to rotate the thread pools (so as to avoid contention
-- due to concurrent access).
forkRTS :: Int -> RTS () -> RTS ThreadId
forkRTS n = liftThreadM . forkThreadM n . unRTS


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
  Comm.run_ conf $
    Sparkpool.run conf noWorkServer idleSem $
      Threadpool.run pools $
        unRTS $
          rts n noWorkServer wakeupServerTid

    where
      -- RTS action
      rts :: Int -> ActionServer -> ThreadId -> RTS ()
      rts scheds noWorkServer wakeupServerTid = do

        -- fork message handler (accessing thread pool 0)
        handlerTid <- forkRTS 0 handler

        -- fork schedulers (each accessing thread pool k, 1 <= k <= scheds)
        schedulerTids <- mapM (\ k -> forkRTS k scheduler) [1 .. scheds]

        -- run main RTS action
        main

        -- block waiting for shutdown barrier
        liftCommM $ Comm.waitShutdown

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

liftCommM :: CommM a -> RTS a
liftCommM = liftThreadM . Threadpool.liftCommM

liftIO :: IO a -> RTS a
liftIO = liftThreadM . Threadpool.liftIO


-- Return scheduler ID, that is ID of scheduler's own thread pool.
schedulerID :: RTS Int
schedulerID = liftThreadM poolID


-----------------------------------------------------------------------------
-- cooperative scheduling

-- Execute the given thread until it blocks or terminates.
execThread :: Thread RTS -> RTS ()
execThread (Atom m) = m >>= maybe (return ()) execThread


-- Try to get a thread from a thread pool or the spark pool and execute it
-- until it blocks or terminates, whence repeat forever; if there is no
-- thread to execute then block the scheduler (ie. its underlying IO thread).
scheduler :: RTS ()
scheduler = getThread >>= scheduleThread


-- Execute given thread until it blocks or terminates, whence call 'scheduler'.
scheduleThread :: Thread RTS -> RTS ()
scheduleThread (Atom m) = m >>= maybe scheduler scheduleThread


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
      maybe_spark <- liftSparkM $ getSpark schedID
      case maybe_spark of
        Just spark -> return $ mkThread $ unClosure spark
        Nothing    -> liftSparkM blockSched >> getThread


-- Converts 'Par' computations into threads.
mkThread :: ParM RTS a -> Thread RTS
mkThread p = unPar p $ \ _c -> Atom (return Nothing)


-----------------------------------------------------------------------------
-- pushed sparks

-- Send a 'spark' via PUSH message to the given 'target' unless 'target'
-- is the current node (in which case 'spark' is executed immediately).
sendPUSH :: Spark RTS -> NodeId -> RTS ()
sendPUSH spark target = do
  here <- liftCommM Comm.myNode
  if target == here
    then do
      -- short cut PUSH msg locally
      execSpark spark
    else do
      -- construct and send PUSH message
      let msg = PUSH spark :: Msg RTS
      debug dbgMsgSend $
        show msg ++ " ->> " ++ show target
      liftCommM $ Comm.send target $ encodeLazy msg


-- Handle a PUSH message by converting the spark into a thread and
-- executing it immediately.
handlePUSH :: Msg RTS -> RTS ()
handlePUSH (PUSH spark) = execSpark spark


-- Execute a spark (by converting it to a thread and executing).
execSpark :: Spark RTS -> RTS ()
execSpark spark = execThread $ mkThread $ unClosure spark


-----------------------------------------------------------------------------
-- message handler; only PUSH messages are actually handled here in this
-- module, other messages are relegated to module Sparkpool.

-- Message handler, running continously (in its own thread) receiving
-- and handling messages (some of which may unblock threads or create sparks)
-- as they arrive.
handler :: RTS ()
handler = do
  msg <- decodeLazy <$> liftCommM Comm.receive
  sparks <- liftSparkM readPoolSize
  debug dbgMsgRcvd $
    ">> " ++ show msg ++ " #sparks=" ++ show sparks
  case msg of
    PUSH _ -> handlePUSH msg
    _      -> liftSparkM $ dispatch msg
  handler


-----------------------------------------------------------------------------
-- auxiliary stuff

-- Print stats (#sparks, threads, FISH, ...) at appropriate debug level.
-- TODO: Log time elapsed since RTS is up
printFinalStats :: RTS ()
printFinalStats = do
  fishes <- liftSparkM $ readFishSentCtr
  scheds <- liftSparkM $ readSparkRcvdCtr
  sparks <- liftSparkM $ readSparkGenCtr
  max_sparks  <- liftSparkM $ readMaxSparkCtr
  maxs_threads <- liftThreadM $ readMaxThreadCtrs
  debug dbgStats $ "#SPARK=" ++ show sparks ++ "   " ++
                   "max_SPARK=" ++ show max_sparks ++ "   " ++
                   "max_THREAD=" ++ show maxs_threads
  debug dbgStats $ "#FISH_sent=" ++ show fishes ++ "   " ++
                   "#SCHED_rcvd=" ++ show scheds

debug :: Int -> String -> RTS ()
debug level message = liftIO $ Location.debug level message
