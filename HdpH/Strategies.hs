-- Strategies (and skeletons) in the Par monad
--
-- Visibility: HdpH external
-- Author: Patrick Maier <P.Maier@hw.ac.uk>
-- Created: 30 Aug 2011
--
-----------------------------------------------------------------------------

{-# LANGUAGE ScopedTypeVariables #-}  -- req'd for type annotations
{-# LANGUAGE FlexibleInstances #-}    -- req'd for some StaticId instances
{-# LANGUAGE TemplateHaskell #-}      -- req'd for mkClosure, etc

module HdpH.Strategies
  ( -- * Strategy type and application
    Strategy,      -- synonym: Strategy a = a -> Par a
    using,         -- :: a -> Strategy a -> Par a
    
    -- * basic sequential strategies
    r0,            -- :: Strategy a
    rseq,          -- :: Strategy a
    rdeepseq,      -- :: (NFData a) => Strategy a

    -- * "fully forcing" closure strategy
    forceClosure,          -- :: (NFData a, StaticId a) => Strategy (Closure a)
    forceClosureClosure,   -- :: (StaticForceClosure a)
                           -- => Closure (Strategy (Closure a))
    StaticForceClosure(    -- context: NFData, StaticId
      staticForceClosureTD   -- :: a -> Static (Env -> Strategy (Closure a))
    ),

    -- * proto-strategies for generating parallelism
    sparkClosure,  -- :: Closure (Strategy (Closure a)) ->
                   --      (Closure a -> Par (IVar (Closure a)))
    pushClosure,   -- :: Closure (Strategy (Closure a)) -> NodeId ->
                   --     (Closure a -> Par (IVar (Closure a)))

    -- * list strategies
    evalList,               -- :: Strategy a -> Strategy [a]
    evalClosureListClosure, -- :: Strategy (Closure a) ->
                            --      Strategy (Closure [Closure a])
    parClosureList,         -- :: Closure (Strategy (Closure a)) ->
                            --      Strategy [Closure a]
    pushClosureList,        -- :: Closure (Strategy (Closure a)) -> [NodeId] ->
                            --      Strategy [Closure a]
    pushRandClosureList,    -- :: Closure (Strategy (Closure a)) -> [NodeId] ->
                            --      Strategy [Closure a]

    -- * clustering strategies
    evalClusterBy,            -- :: (a -> b) 
                              -- -> (b -> a) 
                              -- -> Strategy b 
                              -- -> Strategy a
    parClosureListClusterBy,  -- :: ([Closure a] -> [[Closure a]])
                              -- -> ([[Closure a]] -> [Closure a])
                              -- -> Closure (Strategy (Closure a))
                              -- -> Strategy [Closure a]
    parClosureListChunked,    -- :: Int
                              -- -> Closure (Strategy (Closure a))
                              -- -> Strategy [Closure a]
    parClosureListSliced,     -- :: Int
                              -- -> Closure (Strategy (Closure a))
                              -- -> Strategy [Closure a]

    -- * task farm skeletons
    parMap,          -- :: (StaticId a)
                     -- => Closure (Strategy (Closure b))
                     -- -> Closure (a -> b)
                     -- -> [a]
                     -- -> Par [b]
    parMapNF,        -- :: (StaticId a, StaticForceClosure b)
                     -- => Closure (a -> b)
                     -- -> [a]
                     -- -> Par [b]
    parMapChunked,   -- :: (StaticId a)
                     -- => Int
                     -- -> Closure (Strategy (Closure b))
                     -- -> Closure (a -> b)
                     -- -> [a]
                     -- -> Par [b]
    parMapChunkedNF, -- :: (StaticId a, StaticForceClosure b)
                     -- => Int
                     -- -> Closure (a -> b)
                     -- -> [a]
                     -- -> Par [b]
    parMapSliced,    -- :: (StaticId a)
                     -- => Int
                     -- -> Closure (Strategy (Closure b))
                     -- -> Closure (a -> b)
                     -- -> [a]
                     -- -> Par [b]
    parMapSlicedNF,  -- :: (StaticId a, StaticForceClosure b)
                     -- => Int
                     -- -> Closure (a -> b)
                     -- -> [a]
                     -- -> Par [b]
    parClosureMapM,  -- :: Closure (Closure a -> Par (Closure b))
                     -- -> [Closure a]
                     -- -> Par [Closure b]
    parMapM,         -- :: (StaticId a)
                     -- => Closure (a -> Par (Closure b))
                     -- -> [a]
                     -- -> Par [b]
    parMapM_,        -- :: (StaticId a)
                     -- => Closure (a -> Par ())
                     -- -> [a]
                     -- -> Par ()
    pushMap,         -- :: (StaticId a)
                     -- => Closure (Strategy (Closure b))
                     -- -> [NodeId]
                     -- -> Closure (a -> b)
                     -- -> [a]
                     -- -> Par [b]
    pushMapNF,       -- :: (StaticId a, StaticForceClosure b)
                     -- => [NodeId]
                     -- -> Closure (a -> b)
                     -- -> [a]
                     -- -> Par [b]
    pushClosureMapM, -- :: [NodeId]
                     -- -> Closure (Closure a -> Par (Closure b))
                     -- -> [Closure a]
                     -- -> Par [Closure b]
    pushMapM,        -- :: (StaticId a)
                     -- => [NodeId]
                     -- -> Closure (a -> Par (Closure b))
                     -- -> [a]
                     -- -> Par [b]
    pushMapM_,       -- :: (StaticId a)
                     -- => [NodeId]
                     -- -> Closure (a -> Par ())
                     -- -> [a]
                     -- -> Par ()
    pushRandClosureMapM, -- :: [NodeId]
                         -- -> Closure (Closure a -> Par (Closure b))
                         -- -> [Closure a]
                         -- -> Par [Closure b]
    pushRandMapM,        -- :: (StaticId a)
                         -- => [NodeId]
                         -- -> Closure (a -> Par (Closure b))
                         -- -> [a]
                         -- -> Par [b]
    pushRandMapM_,       -- :: (StaticId a)
                         -- => [NodeId]
                         -- -> Closure (a -> Par ())
                         -- -> [a]
                         -- -> Par ()

    -- * divide and conquer skeletons
    parDivideAndConquer,  -- :: Closure (Closure a -> Bool)
                          -- -> Closure (Closure a -> Par (Closure b))
                          -- -> Closure (Closure a -> [Closure a])
                          -- -> Closure (Closure a -> [Closure b] -> Closure b)
                          -- -> Closure a
                          -- -> Par (Closure b)
    pushDivideAndConquer, -- :: [NodeId]
                          -- -> Closure (Closure a -> Bool)
                          -- -> Closure (Closure a -> Par (Closure b))
                          -- -> Closure (Closure a -> [Closure a])
                          -- -> Closure (Closure a -> [Closure b] -> Closure b)
                          -- -> Closure a
                          -- -> Par (Closure b)

    -- * Static registration
    registerStatic  -- :: IO ()
  ) where

import Prelude
import Control.DeepSeq (NFData, deepseq)
import Control.Monad (zipWithM, zipWithM_)
import Data.Functor ((<$>))
import Data.List (transpose)
import System.Random (randomRIO)

import HdpH (Par, io, force, fork, pushTo, spark, new, get, glob, rput,
             NodeId, IVar, GIVar,
             Env, encodeEnv, decodeEnv,
             Closure, unClosure, toClosure, mapClosure, mkClosure, mkClosureTD,
             Static, register, static, static_, staticTD_,
             StaticId, staticIdTD)
import qualified HdpH.Closure as Closure (registerStatic)


-----------------------------------------------------------------------------
-- 'Static' registration

-- 'StaticId' instance req'd for 'evalClosureListClosure'
instance StaticId [Closure a]

-- 'StaticForceClosure' instance for explicit closures
instance StaticForceClosure (Closure a)

registerStatic :: IO ()
registerStatic = do
  Closure.registerStatic  -- repeat static reg of imported modules
  register $ staticIdTD (undefined :: [Closure a])
  register $ staticForceClosureTD (undefined :: Closure a)
  register $(static 'sparkClosure_abs)
  register $(static 'pushClosure_abs)
  register $(static 'parClosureMapM_abs)
  register $(static 'parMapM_abs)
  register $(static_ 'evalClosureListClosure)
  register $(static 'parDivideAndConquer_abs)
  register $(static 'pushDivideAndConquer_abs)


-----------------------------------------------------------------------------
-- strategy types

-- A 'Strategy' for type 'a' is a (semantic) identity in the 'Par' monad;
-- cf. evaluation strategies in the 'Eval' monad (Marlow et al., Haskell 2010)
type Strategy a = a -> Par a

-- strategy application is actual application (in the Par monad)
using :: a -> Strategy a -> Par a
using = flip ($)


-----------------------------------------------------------------------------
-- basic sequential strategies (polymorphic)

-- "do nothing" strategy
r0 :: Strategy a
r0 = return

-- "evaluate headstrict" strategy; probably not very useful here
rseq :: Strategy a
rseq x = x `seq` return x -- Order of eval irrelevant due to 2nd arg converging

-- "evaluate fully" strategy
rdeepseq :: (NFData a) => Strategy a
rdeepseq x = x `deepseq` return x  -- Order of eval irrelevant (2nd arg conv)


-----------------------------------------------------------------------------
-- "fully forcing" closure strategy

-- "fully forcing" (ie. fully normalising the thunk inside) closure strategy;
-- note that 'forceClosure clo' does not have the same effect as
-- * 'deepseq'ing 'clo' (because 'forceClosure' changes the closure 
--    representation), or
-- * 'deepseq'ing 'toClosure $ unClosure clo' (because 'forceClosure'
--   does not force the serialised environment of its result), or
-- * 'deepseq'ing 'clo' and returning 'toClosure $ unClosure clo' (because
--   this does hang on to the old closure environment whereas 'forceClosure' 
--   gives up the old environment.
-- NB: An 'evalClosure' variant that evaluates the thunk inside head-strictly 
--     does not make sense. Serialising such a closure would turn it into a
--     fully evaluated one.
forceClosure :: (NFData a, StaticId a) => Strategy (Closure a)
forceClosure clo = unClosure clo' `deepseq` return clo'  -- Order of eval irrel
                     where clo' = toClosure $ unClosure clo


-- Static deserialisers corresponding to "fully forcing" closure strategy.
-- NOTE: Do not override default class methods when instantiating.
class (NFData a, StaticId a) => StaticForceClosure a where
  -- static closure forcing strategy to be registered for every class instance;
  -- argument seerves as type discriminator but is never evaluated
  staticForceClosureTD :: a -> Static (Env -> Strategy (Closure a))
  staticForceClosureTD typearg = $(staticTD_ 'forceClosure) typearg


-- closure wrapping "fully forcing" closure strategy
forceClosureClosure :: forall a . (StaticForceClosure a)
                    => Closure (Strategy (Closure a))
forceClosureClosure = $(mkClosureTD [| forceClosure |]) (undefined :: a)
                      -- NOTE: passing 'undefined' as a typearg


-----------------------------------------------------------------------------
-- proto-strategies for generating parallelism

-- sparking strategy combinator, converting a strategy into a kind of
-- "future strategy"; note that strategy argument must be a closure itself
sparkClosure :: Closure (Strategy (Closure a)) ->
                  (Closure a -> Par (IVar (Closure a)))
sparkClosure clo_strat clo = do
  v <- new
  gv <- glob v
  spark $(mkClosure [| sparkClosure_abs (clo, clo_strat, gv) |])
  return v

sparkClosure_abs :: (Closure a, 
                     Closure (Strategy (Closure a)), 
                     GIVar (Closure a))
                 -> Par ()
sparkClosure_abs (clo, clo_strat, gv) =
  (clo `using` unClosure clo_strat) >>= rput gv


-- pushing strategy combinator, converting a strategy into a kind of
-- "future strategy"; note that strategy argument must be a closure itself;
-- note also that the pushed closure is executed in a new thread (rather
-- than inline in the scheduler)
pushClosure :: Closure (Strategy (Closure a)) -> NodeId ->
                 (Closure a -> Par (IVar (Closure a)))
pushClosure clo_strat node clo = do
  v <- new
  gv <- glob v
  pushTo $(mkClosure [| pushClosure_abs (clo, clo_strat, gv) |]) node
  return v

pushClosure_abs :: (Closure a, 
                    Closure (Strategy (Closure a)), 
                    GIVar (Closure a))
                -> Par ()
pushClosure_abs (clo, clo_strat, gv) =
  fork $ (clo `using` unClosure clo_strat) >>= rput gv


------------------------------------------------------------------------------
-- list strategies

-- 'evalList' is a (type-restricted) monadic map; should be suitably 
-- generalisable for all data structures that support mapping over
evalList :: Strategy a -> Strategy [a]
evalList strat []     = return []
evalList strat (x:xs) = do x' <- strat x
                           xs' <- evalList strat xs
                           return (x':xs')


-- specialisation of 'evalList' to lists of closures (wrapped in a closure)
evalClosureListClosure :: Strategy (Closure a) ->
                            Strategy (Closure [Closure a])
evalClosureListClosure strat clo =
  toClosure <$> (unClosure clo `using` evalList strat)


-- parallel list strategy combinator;
-- * stratgy argument must be a closure,
-- * expects list of closures
parClosureList :: Closure (Strategy (Closure a)) -> Strategy [Closure a]
parClosureList clo_strat xs = mapM (sparkClosure clo_strat) xs >>=
                              mapM get


-- parallel list strategy combinator, pushing in round-robin fashion;
-- * stratgy argument must be a closure,
-- * expects list of nodes and list of closures
pushClosureList :: Closure (Strategy (Closure a)) -> [NodeId] ->
                     Strategy [Closure a]
pushClosureList clo_strat nodes xs =
  zipWithM (pushClosure clo_strat) (cycle nodes) xs >>=
  mapM get


-- parallel list strategy combinator, pushing to random nodes;
-- * stratgy argument must be a closure,
-- * expects list of nodes and list of closures
pushRandClosureList :: Closure (Strategy (Closure a)) -> [NodeId] ->
                         Strategy [Closure a]
pushRandClosureList clo_strat nodes xs =
  mapM (\ x -> do { node <- rand; pushClosure clo_strat node x}) xs >>=
  mapM get
    where
      rand :: Par NodeId
      rand = (nodes !!) <$> io (randomRIO (0, length nodes - 1))


------------------------------------------------------------------------------
-- clustering strategies

-- generic clustering strategy combinator
evalClusterBy :: (a -> b) -> (b -> a) -> Strategy b -> Strategy a
evalClusterBy cluster uncluster strat x =
  uncluster <$> (cluster x `using` strat)


-- generic parallel clustering strategy combinator for lists of closures
-- (clustering to lists of closures, again)
parClosureListClusterBy :: ([Closure a] -> [[Closure a]])
                        -> ([[Closure a]] -> [Closure a])
                        -> Closure (Strategy (Closure a))
                        -> Strategy [Closure a]
parClosureListClusterBy cluster uncluster clo_strat =
  evalClusterBy cluster' uncluster' strat'
    where cluster'   = map toClosure . cluster
          uncluster' = uncluster . map unClosure
       -- strat' :: Strategy [Closure [Closure a]]
          strat' = parClosureList clo_strat''
       -- clo_strat'' :: Closure (Strategy (Closure [Closure a]))
          clo_strat'' =
            mapClosure $(mkClosure [| evalClosureListClosure |]) clo_strat


-- chunking and slicing parallel clustering strategy combinators
parClosureListChunked :: Int
                      -> Closure (Strategy (Closure a))
                      -> Strategy [Closure a]
parClosureListChunked n = parClosureListClusterBy (chunk n) unchunk

parClosureListSliced :: Int
                     -> Closure (Strategy (Closure a))
                     -> Strategy [Closure a]
parClosureListSliced n = parClosureListClusterBy (slice n) unslice


-- clustering functions: chunking and slicing
chunk :: Int -> [a] -> [[a]]
chunk n | n <= 0    = chunk 1
        | otherwise = go
            where
              go [] = []
              go xs = ys : go zs where (ys,zs) = splitAt n xs

unchunk :: [[a]] -> [a]
unchunk = concat

slice :: Int -> [a] -> [[a]]
slice n = transpose . chunk n

unslice :: [[a]] -> [a]
unslice = concat . transpose


------------------------------------------------------------------------------
-- skeletons

-- task farm with parameterised by a strategy
parMap :: (StaticId a)
       => Closure (Strategy (Closure b))
       -> Closure (a -> b)
       -> [a]
       -> Par [b]
parMap clo_strat clo_f xs =
  do clo_ys <- map f clo_xs `using` parClosureList clo_strat
     return $ map unClosure clo_ys
       where f = mapClosure clo_f
             clo_xs = map toClosure xs

-- "fully forcing" task farm
parMapNF :: (StaticId a, StaticForceClosure b)
         => Closure (a -> b)
         -> [a]
         -> Par [b]
parMapNF = parMap forceClosureClosure



-- chunking task farm
parMapChunked :: (StaticId a)
              => Int
              -> Closure (Strategy (Closure b))
              -> Closure (a -> b)
              -> [a]
              -> Par [b]
parMapChunked n clo_strat clo_f xs =
  do clo_ys <- map f clo_xs `using` parClosureListChunked n clo_strat
     return $ map unClosure clo_ys
       where f = mapClosure clo_f
             clo_xs = map toClosure xs

-- "fully forcing" chunking task farm
parMapChunkedNF :: (StaticId a, StaticForceClosure b)
                => Int
                -> Closure (a -> b)
                -> [a]
                -> Par [b]
parMapChunkedNF n = parMapChunked n forceClosureClosure


-- slicing task farm
parMapSliced :: (StaticId a)
             => Int
             -> Closure (Strategy (Closure b))
             -> Closure (a -> b)
             -> [a]
             -> Par [b]
parMapSliced n clo_strat clo_f xs =
  do clo_ys <- map f clo_xs `using` parClosureListSliced n clo_strat
     return $ map unClosure clo_ys
       where f = mapClosure clo_f
             clo_xs = map toClosure xs

-- "fully forcing" slicing task farm
parMapSlicedNF :: (StaticId a, StaticForceClosure b)
               => Int
               -> Closure (a -> b)
               -> [a]
               -> Par [b]
parMapSlicedNF n = parMapSliced n forceClosureClosure


-- monadic task farms
parClosureMapM :: Closure (Closure a -> Par (Closure b))
               -> [Closure a]
               -> Par [Closure b]
parClosureMapM clo_f clo_xs =
  do vs <- mapM spawn clo_xs
     mapM get vs
       where
         spawn clo_x = do
           v <- new
           gv <- glob v
           spark $(mkClosure [| parClosureMapM_abs (clo_f, clo_x, gv) |])
           return v

parClosureMapM_abs :: (Closure (Closure a -> Par (Closure b)),
                       Closure a,
                       GIVar (Closure b))
                   -> Par ()
parClosureMapM_abs (clo_f, clo_x, gv) = unClosure clo_f clo_x >>= rput gv


parMapM :: (StaticId a)
        => Closure (a -> Par (Closure b))
        -> [a]
        -> Par [b]
parMapM clo_f xs =
  do vs <- mapM spawn xs
     mapM (\ v -> unClosure <$> get v) vs
       where
         spawn x = do
           let clo_x = toClosure x
           v <- new
           gv <- glob v
           spark $(mkClosure [| parMapM_abs (clo_f, clo_x, gv) |])
           return v

parMapM_abs :: (Closure (a -> Par (Closure b)), 
                Closure a, 
                GIVar (Closure b)) 
            -> Par ()
parMapM_abs (clo_f, clo_x, gv) = unClosure (mapClosure clo_f clo_x) >>= rput gv


parMapM_ :: (StaticId a)
         => Closure (a -> Par ())
         -> [a]
         -> Par ()
parMapM_ clo_f xs = mapM_ (spark . mapClosure clo_f . toClosure) xs


-- eagerly pushing task farm; round robin work distribution
pushMap :: (StaticId a)
        => Closure (Strategy (Closure b))
        -> [NodeId]
        -> Closure (a -> b)
        -> [a]
        -> Par [b]
pushMap clo_strat nodes clo_f xs =
  do clo_ys <- map f clo_xs `using` pushClosureList clo_strat nodes
     return $ map unClosure clo_ys
       where f = mapClosure clo_f
             clo_xs = map toClosure xs

-- "fully forcing" task farm; round robin work distribution by pushing eagerly
pushMapNF :: (StaticId a, StaticForceClosure b)
          => [NodeId]
          -> Closure (a -> b)
          -> [a]
          -> Par [b]
pushMapNF = pushMap forceClosureClosure


-- eagerly pushing monadic task farms; round robin work distribution
pushClosureMapM :: [NodeId]
                -> Closure (Closure a -> Par (Closure b))
                -> [Closure a]
                -> Par [Closure b]
pushClosureMapM nodes clo_f clo_xs =
  do vs <- zipWithM spawn (cycle nodes) clo_xs
     mapM get vs
       where
         spawn node clo_x = do
           v <- new
           gv <- glob v
           pushTo $(mkClosure [| parClosureMapM_abs (clo_f, clo_x, gv) |]) node
           return v


pushMapM :: (StaticId a)
         => [NodeId]
         -> Closure (a -> Par (Closure b))
         -> [a]
         -> Par [b]
pushMapM nodes clo_f xs =
  do vs <- zipWithM spawn (cycle nodes) xs
     mapM (\ v -> unClosure <$> get v) vs
       where
         spawn node x = do
           let clo_x = toClosure x
           v <- new
           gv <- glob v
           pushTo $(mkClosure [| parMapM_abs (clo_f, clo_x, gv) |]) node
           return v


pushMapM_ :: (StaticId a)
          => [NodeId]
          -> Closure (a -> Par ())
          -> [a]
          -> Par ()
pushMapM_ nodes clo_f xs =
  zipWithM_
    (\ node x -> pushTo (mapClosure clo_f $ toClosure x) node)
    (cycle nodes)
    xs


-- eagerly pushing monadic task farms; random work distribution
pushRandClosureMapM :: [NodeId]
                    -> Closure (Closure a -> Par (Closure b))
                    -> [Closure a]
                    -> Par [Closure b]
pushRandClosureMapM nodes clo_f clo_xs =
  do vs <- mapM spawn clo_xs
     mapM get vs
       where
         rand = (nodes !!) <$> io (randomRIO (0, length nodes - 1))
         spawn clo_x = do
           v <- new
           gv <- glob v
           node <- rand
           pushTo $(mkClosure [| parClosureMapM_abs (clo_f, clo_x, gv) |]) node
           return v


pushRandMapM :: (StaticId a)
             => [NodeId]
             -> Closure (a -> Par (Closure b))
             -> [a]
             -> Par [b]
pushRandMapM nodes clo_f xs =
  do vs <- mapM spawn xs
     mapM (\ v -> unClosure <$> get v) vs
       where
         rand = (nodes !!) <$> io (randomRIO (0, length nodes - 1))
         spawn x = do
           let clo_x = toClosure x
           v <- new
           gv <- glob v
           node <- rand
           pushTo $(mkClosure [| parMapM_abs (clo_f, clo_x, gv) |]) node
           return v


pushRandMapM_ :: (StaticId a)
              => [NodeId]
              -> Closure (a -> Par ())
              -> [a]
              -> Par ()
pushRandMapM_ nodes clo_f xs =
  mapM_ spawn xs
    where
      rand = (nodes !!) <$> io (randomRIO (0, length nodes - 1))
      spawn x = do
        node <- rand
        pushTo (mapClosure clo_f $ toClosure x) node


-- generic divide and conquer skeletons
divideAndConquer :: (a -> Bool)      -- trivial
                 -> (a -> b)         -- simplySolve
                 -> (a -> [a])       -- decompose
                 -> (a -> [b] -> b)  -- combineSolutions
                 -> a                -- problem
                 -> b
divideAndConquer trivial simplySolve decompose combineSolutions problem
  | trivial problem = simplySolve problem
  | otherwise =
      combineSolutions problem $
      map solveRec
      (decompose problem)
        where
          solveRec = divideAndConquer
                       trivial
                       simplySolve
                       decompose
                       combineSolutions


-- lazy work distribution
parDivideAndConquer :: Closure (Closure a -> Bool)                     -- trivial
                    -> Closure (Closure a -> Par (Closure b))          -- simplySolve
                    -> Closure (Closure a -> [Closure a])              -- decompose
                    -> Closure (Closure a -> [Closure b] -> Closure b) -- combineSolutions
                    -> Closure a                                       -- problem
                    -> Par (Closure b)
parDivideAndConquer trivial simplySolve decompose combineSolutions problem
  | trivial' problem = simplySolve' problem
  | otherwise =
      combineSolutions' problem <$>
      parClosureMapM solveRec_clo
      (decompose' problem)
        where
          trivial'          = unClosure trivial
          simplySolve'      = unClosure simplySolve
          decompose'        = unClosure decompose
          combineSolutions' = unClosure combineSolutions
          solveRec_clo = $(mkClosure
                           [| parDivideAndConquer_abs
                                (trivial,
                                 simplySolve,
                                 decompose,
                                 combineSolutions) |])

parDivideAndConquer_abs :: (Closure (Closure a -> Bool),
                            Closure (Closure a -> Par (Closure b)),
                            Closure (Closure a -> [Closure a]),
                            Closure (Closure a -> [Closure b] -> Closure b))
                        -> Closure a -> Par (Closure b)
parDivideAndConquer_abs (trivial, simplySolve, decompose, combineSolutions) =
  parDivideAndConquer trivial simplySolve decompose combineSolutions



-- eager, random work distribution
pushDivideAndConquer :: [NodeId]
                     -> Closure (Closure a -> Bool)                     -- trivial
                     -> Closure (Closure a -> Par (Closure b))          -- simplySolve
                     -> Closure (Closure a -> [Closure a])              -- decompose
                     -> Closure (Closure a -> [Closure b] -> Closure b) -- combineSolutions
                     -> Closure a                                       -- problem
                     -> Par (Closure b)
pushDivideAndConquer nodes trivial simplySolve decompose combineSolutions problem
  | trivial' problem = simplySolve' problem
  | otherwise =
      combineSolutions' problem <$>
      pushRandClosureMapM nodes solveRec_clo
      (decompose' problem)
        where
          trivial'          = unClosure trivial
          simplySolve'      = unClosure simplySolve
          decompose'        = unClosure decompose
          combineSolutions' = unClosure combineSolutions
          solveRec_clo = $(mkClosure
                           [| pushDivideAndConquer_abs
                                (nodes,
                                 trivial,
                                 simplySolve,
                                 decompose,
                                 combineSolutions) |])

pushDivideAndConquer_abs :: ([NodeId],
                             Closure (Closure a -> Bool),
                             Closure (Closure a -> Par (Closure b)),
                             Closure (Closure a -> [Closure a]),
                             Closure (Closure a -> [Closure b] -> Closure b))
                         -> Closure a -> Par (Closure b)
pushDivideAndConquer_abs (nodes, trivial, simplySolve, decompose, combineSolutions) =
  pushDivideAndConquer nodes trivial simplySolve decompose combineSolutions
