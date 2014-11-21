-- Thread pool and work stealing
--
-- Author: Patrick Maier
-----------------------------------------------------------------------------

module Control.Parallel.HdpH.Internal.Threadpool
  ( -- * thread pool monad
    ThreadM,      -- synonym: ThreadM m = ReaderT <State m> (SparkM m)
    run,          -- :: [DequeIO (Thread m)] -> ThreadM m a -> SparkM m a
    forkThreadM,  -- :: Int -> ThreadM m () ->
                  --      ThreadM m Control.Concurrent.ThreadId
    liftSparkM,   -- :: SparkM m a -> ThreadM m a
    liftIO,       -- :: IO a -> ThreadM m a

    -- * thread pool ID (of scheduler's own pool)
    poolID,       -- :: ThreadM m Int

    -- * putting threads into the scheduler's own pool
    putThread,    -- :: Thread m -> ThreadM m ()
    putThreads,   -- :: [Thread m] -> ThreadM m ()

    -- * stealing threads (from scheduler's own pool, or from other pools)
    stealThread,  -- :: ThreadM m (Maybe (Thread m))

    -- * statistics
    readMaxThreadCtrs  -- :: ThreadM m [Int]
  ) where

import Prelude hiding (error)
import Control.Concurrent (ThreadId)
import Control.Monad.Reader (ReaderT, runReaderT, ask)
import Control.Monad.Trans (lift)

import Control.Parallel.HdpH.Internal.Data.Deque
       (DequeIO, pushFrontIO, popFrontIO, popBackIO, maxLengthIO)
import Control.Parallel.HdpH.Internal.Location (error)
import Control.Parallel.HdpH.Internal.Misc (fork, rotate)
import Control.Parallel.HdpH.Internal.Sparkpool (SparkM, wakeupSched)
import qualified Control.Parallel.HdpH.Internal.Sparkpool as Sparkpool
       (liftIO)
import Control.Parallel.HdpH.Internal.Type.Par (Thread)


-----------------------------------------------------------------------------
-- thread pool monad

-- 'ThreadM' is a reader monad sitting on top of the 'SparkM' monad;
-- the parameter 'm' abstracts a monad (cf. module HdpH.Internal.Type.Par).
type ThreadM m = ReaderT (State m) (SparkM m)


-- thread pool state (mutable bits held in DequeIO)
type State m = [(Int, DequeIO (Thread m))]  -- list of actual thread pools,
                                            -- each with identifying Int

-- Eliminates the 'ThreadM' layer by executing the given 'action' (typically
-- a scheduler loop) on the given non-empty list of thread 'pools' (the first
-- of which is the scheduler's own pool).
-- NOTE: An empty list of pools is admitted but then 'action' must not call
--      'putThread', 'putThreads', 'stealThread' or 'readMaxThreadCtrs'.
run :: [(Int, DequeIO (Thread m))] -> ThreadM m a -> SparkM m a
run pools action = runReaderT action pools


-- Execute the given 'ThreadM' action in a new thread, sharing the same
-- thread pools (but rotated by 'n' pools).
forkThreadM :: Int -> ThreadM m () -> ThreadM m ThreadId
forkThreadM n action = do
  pools <- getPools
  lift $ fork $ run (rotate n pools) action


-- Lifting lower layers.
liftSparkM :: SparkM m a -> ThreadM m a
liftSparkM = lift

liftIO :: IO a -> ThreadM m a
liftIO = liftSparkM . Sparkpool.liftIO


-----------------------------------------------------------------------------
-- access to state

getPools :: ThreadM m [(Int, DequeIO (Thread m))]
getPools = do pools <- ask
              case pools of
                [] -> error "HdpH.Internal.Threadpool.getPools: no pools"
                _  -> return pools


-----------------------------------------------------------------------------
-- access to thread pool

-- Return thread pool ID, that is ID of scheduler's own pool.
poolID :: ThreadM m Int
poolID = do
  my_pool:_ <- getPools
  return $ fst my_pool


-- Read the max size of each thread pool.
readMaxThreadCtrs :: ThreadM m [Int]
readMaxThreadCtrs = getPools >>= liftIO . mapM (maxLengthIO . snd)


-- Steal a thread from any thread pool, with own pool as highest priority;
-- threads from own pool are always taken from the front; threads from other
-- pools are stolen from the back of those pools.
-- Rationale: Preserve locality as much as possible for own threads; try
-- not to disturb locality for threads stolen from others.
stealThread :: ThreadM m (Maybe (Thread m))
stealThread = do
  my_pool:other_pools <- getPools
  maybe_thread <- liftIO $ popFrontIO $ snd my_pool
  case maybe_thread of
    Just _  -> return maybe_thread
    Nothing -> steal other_pools
      where
        steal :: [(Int, DequeIO (Thread m))] -> ThreadM m (Maybe (Thread m))
        steal []           = return Nothing
        steal (pool:pools) = do
          maybe_thread' <- liftIO $ popBackIO $ snd pool
          case maybe_thread' of
            Just _  -> return maybe_thread'
            Nothing -> steal pools


-- Put the given thread at the front of the executing scheduler's own pool;
-- wake up 1 sleeping scheduler (if there is any).
putThread :: Thread m -> ThreadM m ()
putThread thread = do
  my_pool:_ <- getPools
  liftIO $ pushFrontIO (snd my_pool) thread
  liftSparkM $ wakeupSched 1


-- Put the given threads at the front of the executing scheduler's own pool;
-- the last thread in the list will end up at the front of the pool;
-- wake up as many sleeping schedulers as threads added.
putThreads :: [Thread m] -> ThreadM m ()
putThreads threads = do
  all_pools@(my_pool:_) <- getPools
  liftIO $ mapM_ (pushFrontIO $ snd my_pool) threads
  liftSparkM $ wakeupSched (min (length all_pools) (length threads))
