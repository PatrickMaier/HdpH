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

    -- * this module's Static declaration
    declareStatic
  ) where

import Prelude
import Control.DeepSeq (NFData)
import Control.Monad.IO.Class (MonadIO(liftIO))
import Data.Serialize (Serialize)
import GHC.Generics (Generic)

import Control.Parallel.HdpH
       (Par, myNode, allNodes, io, spawn, get,
        Thunk(Thunk), Closure, mkClosure, unClosure, unitC,
        StaticDecl, static, declare)
import qualified Control.Parallel.HdpH as HdpH (declareStatic)
import Control.Parallel.HdpH.Dist (one)
import Aux.ECRef
       (ECRef, ECRefDict,
        newECRef, freeECRef, readECRef', writeECRef, gatherECRef')
import qualified Aux.ECRef as ECRef (declareStatic)
import Aux.Iter (IterM, nextIterM)
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
-- Static declaration (just before 'main')

-- Empty splice; TH hack to make all environment abstractions visible.
$(return [])

declareStatic :: StaticDecl
declareStatic = mconcat [HdpH.declareStatic,
                         ECRef.declareStatic,
                         declare $(static 'defaultMkTask_abs)]
