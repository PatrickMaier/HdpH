-- Strategies (and skeletons) in the Par monad
--
-- Visibility: HdpH external
-- Author: Patrick Maier <P.Maier@hw.ac.uk>
-- Created: 30 Aug 2011
--
-----------------------------------------------------------------------------

{-# LANGUAGE ScopedTypeVariables #-}  -- for type annotations in Static decl
{-# LANGUAGE FlexibleInstances #-}    -- req'd for some 'ToClosure' instances
{-# LANGUAGE TemplateHaskell #-}      -- req'd for 'mkClosure', etc

module HdpH.Strategies
  ( -- * strategy type and application
    Strategy,       -- synonym: Strategy a = a -> Par a
    using,          -- :: a -> Strategy a -> Par a
    
    -- * basic sequential strategies
    r0,             -- :: Strategy a
    rseq,           -- :: Strategy a
    rdeepseq,       -- :: (NFData a) => Strategy a

    -- * "fully forcing" Closure strategy
    forceC,         -- :: (NFData a, ToClosure a) => Strategy (Closure a)
    forceCC,        -- :: (ForceCC a) => Closure (Strategy (Closure a))
    ForceCC(        -- context: NFData, ToClosure
      locForceCC      -- :: LocT (Strategy (Closure a))
    ),
    StaticForceCC,  -- * -> *; syonoym: Static (Env -> Strategy (Closure _ ))
    staticForceCC,  -- :: (ForceC a) => StaticForceCC a

    -- * proto-strategies for generating parallelism
    sparkClosure,   -- :: Closure (Strategy (Closure a)) ->
                    --    (Closure a -> Par (IVar (Closure a)))
    pushClosure,    -- :: Closure (Strategy (Closure a)) ->
                    --    NodeId ->
                    --    (Closure a -> Par (IVar (Closure a)))

    -- * list strategies
    evalList,                 -- :: Strategy a ->
                              --    Strategy [a]
    evalClosureListClosure,   -- :: Strategy (Closure a) ->
                              --    Strategy (Closure [Closure a])
    parClosureList,           -- :: Closure (Strategy (Closure a)) ->
                              --    Strategy [Closure a]
    pushClosureList,          -- :: Closure (Strategy (Closure a)) ->
                              --    [NodeId] ->
                              --    Strategy [Closure a]
    pushRandClosureList,      -- :: Closure (Strategy (Closure a)) ->
                              --    [NodeId] ->
                              --    Strategy [Closure a]

    -- * clustering strategies
    evalClusterBy,            -- :: (a -> b) ->
                              --    (b -> a) ->
                              --    Strategy b ->
                              --    Strategy a
    parClosureListClusterBy,  -- :: ([Closure a] -> [[Closure a]]) ->
                              --    ([[Closure a]] -> [Closure a]) ->
                              --    Closure (Strategy (Closure a)) ->
                              --    Strategy [Closure a]
    parClosureListChunked,    -- :: Int ->
                              --    Closure (Strategy (Closure a)) ->
                              --    Strategy [Closure a]
    parClosureListSliced,     -- :: Int ->
                              --    Closure (Strategy (Closure a)) ->
                              --    Strategy [Closure a]

    -- * task farm skeletons
    parMap,              -- :: (ToClosure a) =>
                         --    Closure (Strategy (Closure b)) ->
                         --    Closure (a -> b) ->
                         --    [a] -> Par [b]
    parMapNF,            -- :: (ToClosure a, ForceCC b) =>
                         --    Closure (a -> b) ->
                         --    [a] -> Par [b]
    parMapChunked,       -- :: (ToClosure a) =>
                         --    Int ->
                         --    Closure (Strategy (Closure b)) ->
                         --    Closure (a -> b) ->
                         --    [a] -> Par [b]
    parMapChunkedNF,     -- :: (ToClosure a, ForceCC b) =>
                         --    Int ->
                         --    Closure (a -> b) ->
                         --    [a] -> Par [b]
    parMapSliced,        -- :: (ToClosure a) =>
                         --    Int ->
                         --    Closure (Strategy (Closure b)) ->
                         --    Closure (a -> b) ->
                         --    [a] -> Par [b]
    parMapSlicedNF,      -- :: (ToClosure a, ForceCC b) =>
                         --    Int ->
                         --    Closure (a -> b) ->
                         --    [a] -> Par [b]

    parClosureMapM,      -- :: Closure (Closure a -> Par (Closure b)) ->
                         --    [Closure a] -> Par [Closure b]
    parMapM,             -- :: (ToClosure a) =>
                         --    Closure (a -> Par (Closure b)) ->
                         --    [a] -> Par [b]
    parMapM_,            -- :: (ToClosure a) =>
                         --    Closure (a -> Par b) ->
                         --    [a] -> Par ()

    pushMap,             -- :: (ToClosure a) =>
                         --    Closure (Strategy (Closure b)) ->
                         --    [NodeId] ->
                         --    Closure (a -> b) ->
                         --    [a] -> Par [b]
    pushMapNF,           -- :: (ToClosure a, ForceCC b) =>
                         --    [NodeId] ->
                         --    Closure (a -> b) ->
                         --    [a] -> Par [b]

    pushClosureMapM,     -- :: [NodeId] ->
                         --    Closure (Closure a -> Par (Closure b)) ->
                         --    [Closure a] -> Par [Closure b]
    pushMapM,            -- :: (ToClosure a) =>
                         --    [NodeId] ->
                         --    Closure (a -> Par (Closure b)) ->
                         --    [a] -> Par [b]
    pushMapM_,           -- :: (ToClosure a) =>
                         --    [NodeId] ->
                         --    Closure (a -> Par b) ->
                         --    [a] -> Par ()

    pushRandClosureMapM, -- :: [NodeId] ->
                         --    Closure (Closure a -> Par (Closure b)) ->
                         --    [Closure a] -> Par [Closure b]
    pushRandMapM,        -- :: (ToClosure a) =>
                         --    [NodeId] ->
                         --    Closure (a -> Par (Closure b)) ->
                         --    [a] -> Par [b]
    pushRandMapM_,       -- :: (ToClosure a) =>
                         --    [NodeId] ->
                         --    Closure (a -> Par b) ->
                         --    [a] -> Par ()

    -- * divide and conquer skeletons
    parDivideAndConquer,  -- :: Closure (Closure a -> Bool) ->
                          --    Closure (Closure a -> Par (Closure b)) ->
                          --    Closure (Closure a -> [Closure a]) ->
                          --    Closure
                          --      (Closure a -> [Closure b] -> Closure b) ->
                          --    Closure a ->
                          --    Par (Closure b)
    pushDivideAndConquer, -- :: [NodeId] ->
                          --    Closure (Closure a -> Bool) ->
                          --    Closure (Closure a -> Par (Closure b)) ->
                          --    Closure (Closure a -> [Closure a]) ->
                          --    Closure
                          --      (Closure a -> [Closure b] -> Closure b) ->
                          --    Closure a ->
                          --    Par (Closure b)

    -- * Static declaration
    declareStatic    -- :: StaticDecl
  ) where

import Prelude
import Control.DeepSeq (NFData, deepseq)
import Control.Monad (zipWithM, zipWithM_)
import Data.Functor ((<$>))
import Data.List (transpose)
import Data.Monoid (mconcat)
import System.Random (randomRIO)

import HdpH (Par, io, fork, pushTo, spark, new, get, glob, rput,
             NodeId, IVar, GIVar,
             Env, LocT, here,
             Closure, unClosure, mkClosure, mkClosureLoc, apC, compC,
             ToClosure(locToClosure), toClosure,
             StaticToClosure, staticToClosure,
             Static, static, static_, staticLoc_,
             StaticDecl, declare)
import qualified HdpH (declareStatic)


-----------------------------------------------------------------------------
-- Static declaration

-- 'ToClosure' instance required for 'evalClosureListClosure'
instance ToClosure [Closure a] where locToClosure = $(here)

instance ForceCC (Closure a) where locForceCC = $(here)

declareStatic :: StaticDecl
declareStatic =
  mconcat
    [HdpH.declareStatic,  -- 'Static' decl of imported modules
     declare (staticToClosure :: forall a . StaticToClosure [Closure a]),
     declare (staticForceCC :: forall a . StaticForceCC (Closure a)),
     declare $(static 'sparkClosure_abs),
     declare $(static 'pushClosure_abs),
     declare $(static_ 'evalClosureListClosure),
     declare $(static 'parClosureMapM_abs),
     declare $(static 'parMapM_abs),
     declare $(static_ 'constReturnUnit),
     declare $(static 'parDivideAndConquer_abs),
     declare $(static 'pushDivideAndConquer_abs)]


-----------------------------------------------------------------------------
-- strategy types

-- A 'Strategy' for type 'a' is a (semantic) identity in the 'Par' monad;
-- cf. evaluation strategies in the 'Eval' monad (Marlow et al., Haskell 2010)
type Strategy a = a -> Par a

-- strategy application is actual application (in the 'Par' monad)
using :: a -> Strategy a -> Par a
using = flip ($)


-----------------------------------------------------------------------------
-- basic sequential strategies (polymorphic)

-- "do nothing" strategy
r0 :: Strategy a
r0 = return

-- "evaluate head-strict" strategy; probably not very useful here
rseq :: Strategy a
rseq x = x `seq` return x -- Order of eval irrelevant due to 2nd arg converging

-- "evaluate fully" strategy
rdeepseq :: (NFData a) => Strategy a
rdeepseq x = x `deepseq` return x  -- Order of eval irrelevant (2nd arg conv)


-----------------------------------------------------------------------------
-- "fully forcing" strategy for Closures

-- "fully forcing" (ie. fully normalising the thunk inside) Closure strategy.
-- Importantly, 'forceC' alters the serialisable closure represention so that
-- serialisation will not need to force the closure again.
forceC :: (NFData a, ToClosure a) => Strategy (Closure a)
forceC clo = unClosure clo' `deepseq` return clo'  -- Order of eval irrelevant
               where
                 clo' = toClosure $ unClosure clo

-- Note that 'forceC clo' does not have the same effect as
-- * 'rdeepseq clo' (because 'forceC' changes the closure representation), or
-- * 'rdeepseq $ toClosure $ unClosure clo' (because 'forceC' does not force
--   the serialised environment of its result), or
-- * 'rdeepseq clo >> return (toClosure (unClosure clo))' (because this does
--   hang on to the old serialisable environment whereas 'forceC' replaces
--   the old enviroment with a new one).
--
-- Note that it does not make sense to construct a variant of 'forceC' that
-- would evaluate the thunk inside a Closure head-strict only. The reason is
-- that serialising such a Closure would turn it into a fully forced one.


-----------------------------------------------------------------------------
-- "fully forcing" Closure strategy wrapped into a Closure
--
-- To enable passing strategy 'forceC' around in distributed contexts, it
-- has to be wrapped into a Closure. That is, this module should export
--
-- > forceCC :: (NFData a, ToClosure a) => Closure (Strategy (Closure a))
--
-- The tutorial in module 'HdpH.Closure' details how to cope with the
-- type class constraint by introducing a new class.

-- "fully forcing" Closure strategy wrapped into a Closure
forceCC :: (ForceCC a) => Closure (Strategy (Closure a))
forceCC = $(mkClosureLoc [| forceC |]) locForceCC

-- Indexing class for 'forceCC'
class (NFData a, ToClosure a) => ForceCC a where
  locForceCC :: LocT (Strategy (Closure a))
                -- The phantom type argument of 'LocT' is the type of the thunk
                -- that is quoted and passed to 'mkClosureLoc' above.

-- Type synonym for declaring the Static deserialisers required by 'forceCC'
type StaticForceCC a = Static (Env -> Strategy (Closure a))

-- Static deserialisers required by 'forceCC'
staticForceCC :: (ForceCC a) => StaticForceCC a
staticForceCC = $(staticLoc_ 'forceC) locForceCC


-----------------------------------------------------------------------------
-- proto-strategies for generating parallelism

-- sparking strategy combinator, converting a strategy into a kind of
-- "future strategy"; note that strategy argument must be a Closure itself
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
-- "future strategy"; note that strategy argument must be a Closure itself.
-- The pushed Closure is executed in a new thread (rather than inline in
-- the scheduler)
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


-- specialisation of 'evalList' to lists of Closures (wrapped in a Closure)
evalClosureListClosure :: Strategy (Closure a) -> Strategy (Closure [Closure a])
evalClosureListClosure strat clo =
  toClosure <$> (unClosure clo `using` evalList strat)


-- parallel list strategy combinator;
-- * expects a list of Closures,
-- * strategy argument must be a Closure
parClosureList :: Closure (Strategy (Closure a)) -> Strategy [Closure a]
parClosureList clo_strat xs = mapM (sparkClosure clo_strat) xs >>=
                              mapM get


-- parallel list strategy combinator, pushing in round-robin fashion;
-- * expects a list of Closures,
-- * expects a list of nodes (of a length unrelated to the list of Closures),
-- * strategy argument must be a Closure
pushClosureList :: Closure (Strategy (Closure a))
                -> [NodeId]
                -> Strategy [Closure a]
pushClosureList clo_strat nodes xs =
  zipWithM (pushClosure clo_strat) (cycle nodes) xs >>=
  mapM get


-- parallel list strategy combinator, pushing to random nodes;
-- * expects a list of Closures,
-- * expects a list of nodes (of a length unrelated to the list of Closures),
-- * strategy argument must be a Closure
pushRandClosureList :: Closure (Strategy (Closure a))
                    -> [NodeId]
                    -> Strategy [Closure a]
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


-- generic parallel clustering strategy combinator for lists of Closures
-- (clustering a list of Closures to a list of lists of Closures)
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
            $(mkClosure [| evalClosureListClosure |]) `apC` clo_strat


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

-- task farm parameterised by a strategy
parMap :: (ToClosure a)
       => Closure (Strategy (Closure b))
       -> Closure (a -> b)
       -> [a]
       -> Par [b]
parMap clo_strat clo_f xs =
  do clo_ys <- map f clo_xs `using` parClosureList clo_strat
     return $ map unClosure clo_ys
       where f = apC clo_f
             clo_xs = map toClosure xs

-- "fully forcing" task farm
parMapNF :: (ToClosure a, ForceCC b)
         => Closure (a -> b)
         -> [a]
         -> Par [b]
parMapNF = parMap forceCC


-- chunking task farm
parMapChunked :: (ToClosure a)
              => Int
              -> Closure (Strategy (Closure b))
              -> Closure (a -> b)
              -> [a]
              -> Par [b]
parMapChunked n clo_strat clo_f xs =
  do clo_ys <- map f clo_xs `using` parClosureListChunked n clo_strat
     return $ map unClosure clo_ys
       where f = apC clo_f
             clo_xs = map toClosure xs

-- "fully forcing" chunking task farm
parMapChunkedNF :: (ToClosure a, ForceCC b)
                => Int
                -> Closure (a -> b)
                -> [a]
                -> Par [b]
parMapChunkedNF n = parMapChunked n forceCC


-- slicing task farm
parMapSliced :: (ToClosure a)
             => Int
             -> Closure (Strategy (Closure b))
             -> Closure (a -> b)
             -> [a]
             -> Par [b]
parMapSliced n clo_strat clo_f xs =
  do clo_ys <- map f clo_xs `using` parClosureListSliced n clo_strat
     return $ map unClosure clo_ys
       where f = apC clo_f
             clo_xs = map toClosure xs

-- "fully forcing" slicing task farm
parMapSlicedNF :: (ToClosure a, ForceCC b)
               => Int
               -> Closure (a -> b)
               -> [a]
               -> Par [b]
parMapSlicedNF n = parMapSliced n forceCC


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


-- a parallel 'mapM'
parMapM :: (ToClosure a)
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
parMapM_abs (clo_f, clo_x, gv) = unClosure (clo_f `apC` clo_x) >>= rput gv


-- a parallel 'mapM_'; note that applying the 'termParC' transformation
-- is necessary because 'spark' only accepts Closures of type 'Par ()'.
parMapM_ :: (ToClosure a)
         => Closure (a -> Par b)
         -> [a]
         -> Par ()
parMapM_ clo_f xs = mapM_ (spark . apC (termParC `compC` clo_f) . toClosure) xs

-- terminal arrow in the Par monad, wrapped in a Closure
termParC :: Closure (a -> Par ())
termParC = $(mkClosure [| constReturnUnit |])

{-# INLINE constReturnUnit #-}
constReturnUnit :: a -> Par ()
constReturnUnit = const (return ())


-- eagerly pushing task farm; round robin work distribution
pushMap :: (ToClosure a)
        => Closure (Strategy (Closure b))
        -> [NodeId]
        -> Closure (a -> b)
        -> [a]
        -> Par [b]
pushMap clo_strat nodes clo_f xs =
  do clo_ys <- map f clo_xs `using` pushClosureList clo_strat nodes
     return $ map unClosure clo_ys
       where f = apC clo_f
             clo_xs = map toClosure xs

-- "fully forcing" task farm; round robin work distribution by pushing eagerly
pushMapNF :: (ToClosure a, ForceCC b)
          => [NodeId]
          -> Closure (a -> b)
          -> [a]
          -> Par [b]
pushMapNF = pushMap forceCC


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


-- a parallel 'mapM' with eager round robin work distribution
pushMapM :: (ToClosure a)
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


-- a parallel 'mapM_' with eager round robin work distribution
pushMapM_ :: (ToClosure a)
          => [NodeId]
          -> Closure (a -> Par b)
          -> [a]
          -> Par ()
pushMapM_ nodes clo_f xs =
  zipWithM_
    (\ node x -> pushTo (compC termParC clo_f `apC` toClosure x) node)
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


-- a parallel 'mapM' with eager random work distribution
pushRandMapM :: (ToClosure a)
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


-- a parallel 'mapM_' with eager random work distribution
pushRandMapM_ :: (ToClosure a)
              => [NodeId]
              -> Closure (a -> Par b)
              -> [a]
              -> Par ()
pushRandMapM_ nodes clo_f xs =
  mapM_ spawn xs
    where
      rand = (nodes !!) <$> io (randomRIO (0, length nodes - 1))
      spawn x = do
        node <- rand
        pushTo (compC termParC clo_f `apC` toClosure x) node


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


-- Divide-and-conquer with lazy work distribution.
-- Problems and solutions must be of Closure type, and all higher-order
-- arguments must be wrapped into Closures.
parDivideAndConquer :: Closure (Closure a -> Bool)
                    -> Closure (Closure a -> Par (Closure b))
                    -> Closure (Closure a -> [Closure a])
                    -> Closure (Closure a -> [Closure b] -> Closure b)
                    -> Closure a
                    -> Par (Closure b)
parDivideAndConquer
  trivial_clo
  simplySolve_clo
  decompose_clo
  combineSolutions_clo
  problem
    | trivial problem = simplySolve problem
    | otherwise =
        combineSolutions problem <$>
        parClosureMapM solveRec_clo
        (decompose problem)
          where
            trivial          = unClosure trivial_clo
            simplySolve      = unClosure simplySolve_clo
            decompose        = unClosure decompose_clo
            combineSolutions = unClosure combineSolutions_clo
            solveRec_clo = $(mkClosure [| parDivideAndConquer_abs
                                            (trivial_clo,
                                             simplySolve_clo,
                                             decompose_clo,
                                             combineSolutions_clo) |])

parDivideAndConquer_abs :: (Closure (Closure a -> Bool),
                            Closure (Closure a -> Par (Closure b)),
                            Closure (Closure a -> [Closure a]),
                            Closure (Closure a -> [Closure b] -> Closure b))
                        -> Closure a -> Par (Closure b)
parDivideAndConquer_abs (trivial_clo,
                         simplySolve_clo,
                         decompose_clo,
                         combineSolutions_clo) =
  parDivideAndConquer trivial_clo
                      simplySolve_clo
                      decompose_clo
                      combineSolutions_clo


-- Divide-and-conquer with eager, random work distribution.
pushDivideAndConquer :: [NodeId]
                     -> Closure (Closure a -> Bool)
                     -> Closure (Closure a -> Par (Closure b))
                     -> Closure (Closure a -> [Closure a])
                     -> Closure (Closure a -> [Closure b] -> Closure b)
                     -> Closure a
                     -> Par (Closure b)
pushDivideAndConquer
  nodes
  trivial_clo
  simplySolve_clo
  decompose_clo
  combineSolutions_clo
  problem
    | trivial problem = simplySolve problem
    | otherwise =
        combineSolutions problem <$>
        pushRandClosureMapM nodes solveRec_clo
        (decompose problem)
          where
            trivial          = unClosure trivial_clo
            simplySolve      = unClosure simplySolve_clo
            decompose        = unClosure decompose_clo
            combineSolutions = unClosure combineSolutions_clo
            solveRec_clo = $(mkClosure [| pushDivideAndConquer_abs
                                            (nodes,
                                             trivial_clo,
                                             simplySolve_clo,
                                             decompose_clo,
                                             combineSolutions_clo) |])

pushDivideAndConquer_abs :: ([NodeId],
                             Closure (Closure a -> Bool),
                             Closure (Closure a -> Par (Closure b)),
                             Closure (Closure a -> [Closure a]),
                             Closure (Closure a -> [Closure b] -> Closure b))
                         -> Closure a -> Par (Closure b)
pushDivideAndConquer_abs (nodes,
                          trivial_clo,
                          simplySolve_clo,
                          decompose_clo,
                          combineSolutions_clo) =
  pushDivideAndConquer nodes
                       trivial_clo
                       simplySolve_clo
                       decompose_clo
                       combineSolutions_clo
