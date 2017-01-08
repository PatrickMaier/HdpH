-- Branch-And-Bound Skeletons
--
-- Author: Patrick Maier <C.Patrick.Maier@gmail.com>
--
-- This module might be integrated into the HdpH library, alongside Strategies.
-- Iterators and tree iterators might be included in this module.
---------------------------------------------------------------------------

{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE FlexibleInstances #-}     -- for MonadIO instance

{-# OPTIONS_GHC -fno-warn-orphans #-}  -- for MonadIO instance

module Aux.BranchAndBound
  ( -- * re-exported from TreeIter module
    Path,           -- type of paths in a tree (from current node to root)
    GeneratorM,     -- type of ordered tree generators

    -- * dictionary of operations necessary for Branch&Bound
    BBDict(         -- dictionary type
      BBDict,         -- dictionary constructor
      bbToClosure,    -- closure conversion for Path
      bbGenerator,    -- ordered tree generator (monadic)
      bbObjective,    -- objective function (pure)
      bbDoPrune,      -- pruning predicate (monadic)
      bbIsTask,       -- task selection predicate (pure)
      bbMkTask),      -- optional task constructor

    -- * branch & bound skeletons
    seqBB,          -- sequential skeleton
    parBB,          -- parallel skeleton

    -- * buffered branch & bound skeletons
    seqBufBB,       -- sequential skeleton
    parBufBB,       -- sequential skeleton

    -- * this module's Static declaration
    declareStatic
  ) where

import Prelude
import Control.Concurrent.MVar (MVar, newEmptyMVar, putMVar, takeMVar)
import Control.DeepSeq (NFData)
import Control.Exception (mask_)
import Control.Monad (filterM)
import Control.Monad.IO.Class (MonadIO(liftIO))
import Data.IORef (IORef, newIORef, atomicModifyIORef')
import Data.Maybe (isNothing)
import Data.Serialize (Serialize)
import GHC.Generics (Generic)

import Control.Parallel.HdpH
       (Par, IVar, GIVar, Node, myNode, allNodes,
        io, pushTo, spark, spawn, new, glob, rput, get, tryGet,
        Thunk(Thunk), Closure, mkClosure, unClosure, unitC,
        StaticDecl, static, declare)
import qualified Control.Parallel.HdpH as HdpH (declareStatic)
import Control.Parallel.HdpH.Dist (Dist, one)
import Aux.ECRef
       (ECRef, ECRefDict,
        newECRef, freeECRef, readECRef', writeECRef, gatherECRef',
        RRef, newRRef, freeRRef, readRRef')
import qualified Aux.ECRef as ECRef (declareStatic)
import Aux.Iter (IterM, nextIterM, bufferM)
import Aux.TreeIter
       (Path, GeneratorM, TreeIterM, newTreeIterM, newPruneTreeIterM)


---------------------------------------------------------------------------
-- MonadIO instance for Par (orphan instance)

instance MonadIO Par where
  liftIO = io

{-# SPECIALIZE nextIterM :: IterM Par s a -> Par (Maybe a) #-}
{-# SPECIALIZE newTreeIterM
               :: Path a -> GeneratorM Par a -> Par (TreeIterM Par a) #-}
{-# SPECIALIZE newPruneTreeIterM
               :: (Path a -> Bool) -> Path a -> GeneratorM Par a
               -> Par (TreeIterM Par a) #-}


-----------------------------------------------------------------------------
-- HdpH branch-and-bound skeleton (based on tree iterators and ECRefs)

data MODE = SEQ | PAR deriving (Eq, Ord, Generic, Show)

instance NFData MODE
instance Serialize MODE


-- | Dictionary of operations for BnB skeleton.
-- Type variable 'x' is the path alphabet for tree the iterators,
-- type variable 'y' is the solution (including the bound) to be maximised.
data BBDict x y =
  BBDict {
    bbToClosure :: Path x -> Closure (Path x),     -- closure conv for Path
    bbGenerator :: GeneratorM Par x,               -- ordered tree generator
    bbObjective :: Path x -> y,                    -- objective function
    bbDoPrune   :: Path x -> ECRef y -> Par Bool,  -- pruning predicate
    bbIsTask    :: Path x -> Bool,                 -- task predicate
    bbMkTask    :: Maybe (Path x -> ECRef y -> Closure (Par (Closure ())))
      -- optional task constructor (applied to path picked by bbIsTask)
  }


-- | Sequential BnB skeleton.
-- Takes a closured BnB dictionary and a closured ECRef dictionary.
-- Runs a sequential BnB search and returns the maximal solution and
-- the number of tasks generated.
seqBB :: Closure (BBDict x y) -> Closure (ECRefDict y) -> Par (y, Int)
seqBB = bb SEQ


-- | Parallel BnB skeleton.
-- Takes a closured BnB dictionary and a closured ECRef dictionary.
-- Runs a parallel BnB search and returns the maximal solution and
-- the number of tasks generated.
parBB :: Closure (BBDict x y) -> Closure (ECRefDict y) -> Par (y, Int)
parBB = bb PAR


-- BnB search wrapper.
-- Takes a mode, a closured BnB dictionary, and a closured ECRef dictionary.
-- Runs a BnB search (as per mode) and returns the maximal solution and
-- the number of tasks generated.
bb :: MODE -> Closure (BBDict x y) -> Closure (ECRefDict y) -> Par (y, Int)
{-# INLINE bb #-}
bb mode bbDictC ecDictYC = do
  let !bbDict = unClosure bbDictC
  -- set up solution & bound propagating ECRef
  nodes <- case mode of { SEQ -> fmap (\ x -> [x]) myNode; PAR -> allNodes }
  let path0 = []
  let y0 = bbObjective bbDict path0
  yRef <- newECRef ecDictYC nodes y0
  -- run BnB search
  !tasks <- bbSearch mode bbDictC path0 yRef
  -- collect maximal solution (a readECRef' call should do that, too)
  y <- gatherECRef' yRef
  -- deallocate the ECRef and return result
  freeECRef yRef
  return (y, tasks)


-- Actual BnB search.
-- Takes a mode, a closured BnB dictionary, a starting path, and an ECRef
-- for propagating the solution.
-- Returns (when all tasks are completed) the number of tasks generated.
bbSearch :: MODE -> Closure (BBDict x y) -> Path x -> ECRef y -> Par Int
{-# INLINE bbSearch #-}
bbSearch mode bbDictC path0 yRef = do
  let !bbDict = unClosure bbDictC
  -- select task constructor
  let mkTask = case bbMkTask bbDict of
                 Just mk -> mk
                 Nothing -> defaultMkTask bbDictC
  -- set up outer tree iterator (which generates tasks)
  let generator []   = bbGenerator bbDict []
      generator path = do
        prune <- bbDoPrune bbDict path yRef
        if prune then return [] else bbGenerator bbDict path
  iter <- newPruneTreeIterM (bbIsTask bbDict) path0 generator
  -- loop stepping through outer iterator
  case mode of
    SEQ -> evalTasks 0
             where
               -- immediately evaluate tasks generated by iterator
               evalTasks !count = do
                 maybe_path <- nextIterM iter
                 case maybe_path of
                   Nothing   -> return count
                   Just path -> do
                     _ <- unClosure $ mkTask path yRef
                     evalTasks (count + 1)
    PAR -> spawnTasks 0 [] >>= \ (!count, ivars) ->
           -- block until all tasks are completed; wait on tasks in order of
           -- creation so thread may not block immediately (in case the oldest
           -- tasks have already completed); while thread is blocked, scheduler
           -- may evaluate further tasks (typically the youngest ones).
           mapM_ get (reverse ivars) >>
           return count
             where
               -- spawn tasks generated by iterator
               spawnTasks !count ivars = do
                 maybe_path <- nextIterM iter
                 case maybe_path of
                   Nothing   -> return (count, ivars)
                   Just path -> do
                     ivar <- spawn one $ mkTask path yRef
                     spawnTasks (count + 1) (ivar:ivars)


-- Default task generation (used when bbMkTask == Nothing).
defaultMkTask :: Closure (BBDict x y)
              -> Path x -> ECRef y -> Closure (Par (Closure ()))
defaultMkTask bbDictC path0 yRef =
  $(mkClosure [| defaultMkTask_abs (bbDictC, path0C, yRef) |])
    where
      !bbDict = unClosure bbDictC
      path0C = bbToClosure bbDict path0

defaultMkTask_abs :: (Closure (BBDict x y), Closure (Path x), ECRef y)
                  -> Thunk (Par (Closure ()))
defaultMkTask_abs (bbDictC, path0C, yRef) = Thunk $ do
  let !bbDict = unClosure bbDictC
  -- set up inner tree iterator (which enumerates path)
  let generator []   = bbGenerator bbDict []
      generator path = do
        prune <- bbDoPrune bbDict path yRef
        if prune then return [] else bbGenerator bbDict path
  iter <- newTreeIterM (unClosure path0C) generator
  -- loop stepping through inner iterator, maximising objective function
  let maxLoop = do
        maybe_path <- nextIterM iter
        case maybe_path of
          Nothing   -> return ()
          Just path -> do
            _maybe_y <- writeECRef yRef (bbObjective bbDict path)
            maxLoop
  maxLoop
  -- return unit closure to signal completion
  return unitC


-----------------------------------------------------------------------------
-- HdpH branch-and-bound skeleton with delayed task generation

-- | Sequential BnB skeleton with buffered task generation.
-- Similar to 'seqBB' but takes an extra argument 'buf_sz' which is the size
-- of the task buffer (at least 1, which is standard sequential execution).
-- The skeleton runs a sequential BnB search but generates always 'buf_sz'
-- tasks ahead. As a result, there are less opportunities to prune tasks
-- than in a standard sequential search.
seqBufBB :: Int
         -> Closure (BBDict x y) -> Closure (ECRefDict y) -> Par (y, Integer)
seqBufBB buf_sz = bbBuf SEQ buf_sz' ntf_freq
  where
    !buf_sz'  = max 1 buf_sz
    !ntf_freq = 1  -- ntf_freq is irrelevant for mode SEQ


-- | Parallel BnB skeleton with delayed task generation.
-- Similar to 'parBB' but takes two extra arguments: 'buf_sz' limits the number
-- of tasks in the spark pool, and every 'ntf_freq'-th task sends a notification
-- back to trigger further task generation.
-- The skeleton ensures sensible values for 'buf_sz' (at least 2) and
-- 'ntf_freq' (between 1 and half of 'buf_sz').
-- Nevertheless, HdpH may deadlock when running this skeleton due to
-- blocking the calling scheduler on an empty MVar.
--
-- The skeleton risks blocking if
-- * there is only one node and one scheduler in the system, or
-- * the calling node runs only one scheduler and task generation deadlocks
--   because the tasks to notify of demand are locked away in the sparkpool
--   of the calling scheduler which is blocked.
--
-- To avoid deadlocks run with multiple nodes, large 'buf_sz', small 'ntf_freq',
-- and keep the spark pool watermarks low (eg. at their defaults).
--
parBufBB :: Int -> Int -> Closure (BBDict x y) -> Closure (ECRefDict y)
         -> Par (y, Integer)
parBufBB buf_sz ntf_freq = bbBuf PAR buf_sz' ntf_freq'
  where
    !buf_sz'   = max 2 buf_sz
    !ntf_freq' = min (max 1 ntf_freq) (buf_sz' `div` 2)


-- BnB search wrapper with buffered/delayed task generation.
-- Takes a mode, a task buffer size, a notification frequency,
-- a closured BnB dictionary, and a closured ECRef dictionary.
-- Runs a BnB search (as per mode) and returns the maximal solution and
-- the number of tasks generated.
bbBuf :: MODE -> Int -> Int
      -> Closure (BBDict x y) -> Closure (ECRefDict y) -> Par (y, Integer)
{-# INLINE bbBuf #-}
bbBuf mode buf_sz ntf_freq bbDictC ecDictYC = do
  let !bbDict = unClosure bbDictC
  -- set up solution & bound propagating ECRef
  nodes <- case mode of { SEQ -> fmap (\ x -> [x]) myNode; PAR -> allNodes }
  let path0 = []
  let y0 = bbObjective bbDict path0
  yRef <- newECRef ecDictYC nodes y0
  -- set up channel for notifying task generation demand
  demandRef <- newRRef =<< io (newIncSkipChan $ -1)
  -- run BnB search
  !tasks <- bbBufSearch mode buf_sz ntf_freq demandRef bbDictC path0 yRef
  -- collect maximal solution (a readECRef' call should do that, too)
  y <- gatherECRef' yRef
  -- dealloc demand channel, bound ECRef and return result + #tasks generated
  freeRRef demandRef
  freeECRef yRef
  return (y, tasks)


-- Actual BnB search with buffered/delayed task generation.
-- Takes a mode, a task buffer size, a notification frequency, a task
-- generation demand channel, a closured BnB dictionary, a starting path,
-- and an ECRef for propagating the solution.
-- Returns (when all tasks are completed) the number of tasks generated.
bbBufSearch :: MODE -> Int -> Int -> RRef (IncSkipChan Integer)
            -> Closure (BBDict x y) -> Path x -> ECRef y -> Par Integer
{-# INLINE bbBufSearch #-}
bbBufSearch mode buf_sz ntf_freq demandRef bbDictC path0 yRef = do
  let !bbDict = unClosure bbDictC
  demandChan <- readRRef' demandRef
  -- select task constructor
  let mkTask = case bbMkTask bbDict of
                 Just mk -> mk
                 Nothing -> defaultMkTask bbDictC
  -- set up outer tree iterator (which generates tasks)
  let generator []   = bbGenerator bbDict []
      generator path = do
        prune <- bbDoPrune bbDict path yRef
        if prune then return [] else bbGenerator bbDict path
  iter <- newPruneTreeIterM (bbIsTask bbDict) path0 generator
  -- loop stepping through outer iterator
  case mode of
    SEQ -> bufferM buf_sz iter >>= evalTasks 0
             where
               -- immediately evaluate tasks generated by buffered iterator
               evalTasks !task_cnt buf_iter = do
                 maybe_path <- nextIterM buf_iter
                 case maybe_path of
                   Nothing   -> return task_cnt
                   Just path -> do
                     _ <- unClosure $ mkTask path yRef
                     evalTasks (task_cnt + 1) buf_iter
    PAR -> spawnTasks (fromIntegral buf_sz) 0 buf_sz []
             where
               -- always keep ~ `buf_sz` tasks ready for evaluation
               spawnTasks demand !task_cnt !purge_cnt ivars = do
                 if task_cnt >= demand
                   then do  -- no tasks to spawn for the moment
                     -- purge filled IVars if it is time to
                     ivars' <- if purge_cnt > 0
                                 then return ivars
                                 else filterM (fmap isNothing . tryGet) ivars
                     let purge_cnt' | purge_cnt > 0 = purge_cnt
                                    | otherwise     = buf_sz
                     -- block waiting for more demand
                     demand' <- io $ getIncSkipChan demandChan
                     -- then tail recurse
                     spawnTasks demand' task_cnt purge_cnt' ivars'
                   else do  -- task_cnt < demand; spawn demand - task_cnt tasks
                     maybe_path <- nextIterM iter
                     case maybe_path of
                       Nothing   -> do  -- all tasks spawned
                         -- block on outstanding empty IVars
                         mapM_ get ivars
                         -- all tasks completed
                         return task_cnt
                       Just path -> do
                         -- construct new task
                         let task = mkTask path yRef
                         let !task_cnt'  = task_cnt + 1
                         let !purge_cnt' = purge_cnt - 1
                         -- spawn task, with notifier if necessary
                         ivar <- if task_cnt `mod` fromIntegral ntf_freq > 0
                                   then do -- no notifier
                                     spawn one task
                                   else do -- construct notifier, spawn with it
                                     let notifier =
                                           mkNotifier
                                             demandRef
                                             (task_cnt' + fromIntegral buf_sz)
                                     spawnNotify one notifier task
                         -- then tail recurse
                         spawnTasks demand task_cnt' purge_cnt' (ivar:ivars)


-- Constructs notifier computation, which posts 'demand' into the task
-- generation demand channel 'demandRef'. Closure to be pushed to the
-- node which owns 'demandRef' (and hence the channel).
mkNotifier :: RRef (IncSkipChan Integer) -> Integer -> Closure (Par ())
mkNotifier demandRef !demand =
  $(mkClosure [| mkNotifier_abs (demandRef, demand) |])

mkNotifier_abs :: (RRef (IncSkipChan Integer), Integer) -> Thunk (Par ())
mkNotifier_abs (demandRef, demand) = Thunk $ do
  demandChan <- readRRef' demandRef
  io $ putIncSkipChan demandChan demand


-----------------------------------------------------------------------------
-- monotonic integer skip channel

-- An increasing skip channel is a one-place channel with a single blocking
-- reader and multiple non-blocking writers, holding totally ordererd data.
-- Writes only succeed if they overwrite the channel value with a value
-- that is strictly greater than the previous value.
--
-- An increasing skip channel consist of an IORef and an MVar.
-- The IORef contains the current value, and a list of semaphores corresponding
-- to blocked readers that need to be notified when the value changes.
-- The MVar is a semaphore for this particular reader: it is full if there
-- is a value in the channel that this reader has not read yet, and empty
-- otherwise.

data IncSkipChan a = IncSkipChan (IORef (a, [MVar ()])) (MVar ())

-- Creates a new increasing skip channel.
-- The channel is initially empty; subsequent writes will be successful only if
-- they write values that are strictly greater than 'bottom'.
newIncSkipChan :: a -> IO (IncSkipChan a)
newIncSkipChan !bottom = do
  sem <- newEmptyMVar
  main <- newIORef (bottom, [sem])
  return (IncSkipChan main sem)

-- Blocking read from increasing skip channel.
getIncSkipChan :: IncSkipChan a -> IO a
getIncSkipChan (IncSkipChan main sem) = mask_ $ do
  takeMVar sem  -- may block
  atomicModifyIORef' main $ \ (v, sems) -> ((v, sem:sems), v)

-- Non-blocking write to increasing skip channel.
putIncSkipChan :: (Ord a) => IncSkipChan a -> a -> IO ()
putIncSkipChan (IncSkipChan main _) !v = mask_ $ do
  blocked_readers <- atomicModifyIORef' main $ \ old@(old_v, sems) ->
    if v <= old_v
      then (old,     [])
      else ((v, []), sems)
  mapM_ (\ sem -> putMVar sem ()) blocked_readers
  -- Above putMVar ought not to block; MVar sem ought to be empty.

-- Same as putIntSkipChan, but returns Nothing if write was successful,
-- the old value otherwise.
putIncSkipChan' :: (Ord a) => IncSkipChan a -> a -> IO (Maybe a)
putIncSkipChan' (IncSkipChan main _) !v = mask_ $ do
  (blocked_readers, maybe_v) <- atomicModifyIORef' main $ \ old@(old_v, sems) ->
    if v <= old_v
      then (old,     ([],   Just old_v))
      else ((v, []), (sems, Nothing))
  mapM_ (\ sem -> putMVar sem ()) blocked_readers
  return maybe_v


-----------------------------------------------------------------------------
-- spawning tasks with notification (execute receipts)

-- Similar to 'spawn' except that, at the start of the execution of 'task',
-- pushes 'notifier' back to the spawning node, where 'notifier' will be
-- executed eagerly and asynchronously.
spawnNotify :: Dist ->  Closure (Par ()) -> Closure (Par (Closure a))
            -> Par (IVar (Closure a))
spawnNotify r notifier task = do
  spawner <- myNode
  v <- new
  gv <- glob v
  spark r $(mkClosure [| spawnNotify_abs (notifier, spawner, task, gv) |])
  return v

spawnNotify_abs :: (Closure (Par ()),
                    Node,
                    Closure (Par (Closure a)), 
                    GIVar (Closure a))
                -> Thunk (Par ())
spawnNotify_abs (notifier, spawner, task, gv) = Thunk $
  pushTo notifier spawner >> unClosure task >>= rput gv


-----------------------------------------------------------------------------
-- Static declaration (just before 'main')

-- Empty splice; TH hack to make all environment abstractions visible.
$(return [])

declareStatic :: StaticDecl
declareStatic = mconcat [HdpH.declareStatic,
                         ECRef.declareStatic,
                         declare $(static 'defaultMkTask_abs),
                         declare $(static 'mkNotifier_abs),
                         declare $(static 'spawnNotify_abs)]
