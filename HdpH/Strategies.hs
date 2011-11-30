-- Strategies (and skeletons) in the Par monad
--
-- Visibility: HdpH external
-- Author: Patrick Maier <P.Maier@hw.ac.uk>
-- Created: 30 Aug 2011
--
-----------------------------------------------------------------------------

{-# LANGUAGE ScopedTypeVariables #-}  -- req'd for phantom type annotations
{-# LANGUAGE FlexibleInstances #-}    -- req'd for some DecodeStatic instances

module HdpH.Strategies
  ( -- * Strategy type and application
    Strategy,      -- synonym: Strategy a = a -> Par a
    using,         -- :: a -> Strategy a -> Par a
    
    -- * basic sequential strategies
    r0,            -- :: Strategy a
    rseq,          -- :: Strategy a
    rdeepseq,      -- :: (NFData a) => Strategy a

    -- * basic closure strategies
    forceClosure,  -- :: (NFData a, DecodeStatic a) => Strategy (Closure a)
    sparkClosure,  -- :: Closure (Strategy (Closure a)) ->
                   --      (Closure a -> Par (IVar (Closure a)))

    -- * list strategies
    evalList,               -- :: Strategy a -> Strategy [a]
    evalClosureListClosure, -- :: Strategy (Closure a) ->
                            --      Strategy (Closure [Closure a])
    parClosureList,         -- :: Closure (Strategy (Closure a)) ->
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
    parMap,          -- :: (DecodeStatic a)
                     -- => Closure (Strategy (Closure b))
                     -- -> Closure (a -> b)
                     -- -> [a]
                     -- -> Par [b]
    parMapNF,        -- :: forall a b . (DecodeStatic a, ForceClosureStatic b)
                     -- => Closure (a -> b)
                     -- -> [a]
                     -- -> Par [b]
    parMapChunked,   -- :: (DecodeStatic a)
                     -- => Int
                     -- -> Closure (Strategy (Closure b))
                     -- -> Closure (a -> b)
                     -- -> [a]
                     -- -> Par [b]
    parMapChunkedNF, -- :: forall a b . (DecodeStatic a, ForceClosureStatic b)
                     -- => Int
                     -- -> Closure (a -> b)
                     -- -> [a]
                     -- -> Par [b]
    parMapSliced,    -- :: (DecodeStatic a)
                     -- => Int
                     -- -> Closure (Strategy (Closure b))
                     -- -> Closure (a -> b)
                     -- -> [a]
                     -- -> Par [b]
    parMapSlicedNF,  -- :: forall a b . (DecodeStatic a, ForceClosureStatic b)
                     -- => Int
                     -- -> Closure (a -> b)
                     -- -> [a]
                     -- -> Par [b]

    -- * static closure forcing functions
    ForceClosureStatic(       -- context: NFData, DecodeStatic
      forceClosureStatic      -- :: Static (Env -> Strategy (Closure a))    
    ),

    -- * Static declaration and registration
    registerStatic  -- :: IO ()
  ) where

import Prelude
import Control.DeepSeq (NFData, rnf)
import Data.List (transpose)
import Data.Monoid (mconcat)

import HdpH (Par, spark, new, get, glob, rput,
             IVar, GIVar,
             Env, encodeEnv, decodeEnv,
             Closure, toClosure, unsafeMkClosure, unsafeMkClosure0, unClosure,
             mapClosure,
             DecodeStatic, decodeStatic,
             Static, staticAs, staticAsTD, declare, register)
import qualified HdpH.Closure as Closure (forceClosure, registerStatic)


-----------------------------------------------------------------------------
-- 'Static' declaration and registration

-- 'DecodeStatic' instance req'd for 'evalClosureListClosure'
instance DecodeStatic [Closure a]

registerStatic :: IO ()
registerStatic = do
  Closure.registerStatic  -- repeat static reg of imported modules
  register $ mconcat
    [declare (decodeStatic :: Static (Env -> [Closure a])),
     declare sparkClosureStatic,
     declare evalClosureListClosureStatic]


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
rdeepseq x = rnf x `seq` return x  -- Order of eval irrelevant (2nd arg conv)


-----------------------------------------------------------------------------
-- basic closure strategies

-- "fully forcing" closure strategy
forceClosure :: (NFData a, DecodeStatic a) => Strategy (Closure a)
forceClosure clo = clo' `seq` return clo'  -- Order of eval irrelevant, again
  where clo' = Closure.forceClosure clo

-- sparking strategy combinator, converting a strategy into a kind of
-- "future strategy"; note that strategy argument must be a closure itself
sparkClosure :: Closure (Strategy (Closure a)) ->
                  (Closure a -> Par (IVar (Closure a)))
sparkClosure clo_strat clo = do
  v <- new
  gv <- glob v
  let val = (clo `using` unClosure clo_strat) >>= rput gv
  let env = encodeEnv (clo, clo_strat, gv)
  let fun = sparkClosureStatic
  spark $ unsafeMkClosure val fun env
  return v

sparkClosureStatic :: forall a . Static (Env -> Par ())
sparkClosureStatic = staticAs
  (\ env -> let (clo       :: Closure a                     ,
                 clo_strat :: Closure (Strategy (Closure a)),
                 gv        :: GIVar (Closure a)             ) = decodeEnv env
              in (clo `using` unClosure clo_strat) >>= rput gv)
  "HdpH.Strategies.sparkClosure"


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
  toClosure `fmap` (unClosure clo `using` evalList strat)


-- parallel list strategy combinator;
-- * stratgy argument must be a closure,
-- * expects list of closures
parClosureList :: Closure (Strategy (Closure a)) -> Strategy [Closure a]
parClosureList clo_strat xs = mapM (sparkClosure clo_strat) xs >>=
                              mapM get


------------------------------------------------------------------------------
-- clustering strategies

-- generic clustering strategy combinator
evalClusterBy :: (a -> b) -> (b -> a) -> Strategy b -> Strategy a
evalClusterBy cluster uncluster strat x =
  uncluster `fmap` (cluster x `using` strat)


-- generic parallel clustering strategy combinator for lists of closures
-- (clustering to lists of closures, again)
parClosureListClusterBy :: forall a .
                           ([Closure a] -> [[Closure a]])
                        -> ([[Closure a]] -> [Closure a])
                        -> Closure (Strategy (Closure a))
                        -> Strategy [Closure a]
parClosureListClusterBy cluster uncluster clo_strat =
  evalClusterBy cluster' uncluster' strat'
    where cluster'   = map toClosure . cluster
          uncluster' = uncluster . map unClosure
          strat' :: Strategy [Closure [Closure a]]
          strat' = parClosureList clo_strat''
          clo_strat'' :: Closure (Strategy (Closure [Closure a]))
          clo_strat'' = mapClosure clo_eCL clo_strat
            where clo_eCL = unsafeMkClosure0 val fun
                  val = evalClosureListClosure
                  fun = evalClosureListClosureStatic

evalClosureListClosureStatic :: Static (Env ->
                                        Strategy (Closure a) ->
                                        Strategy (Closure [Closure a]))
evalClosureListClosureStatic = staticAs
  (const evalClosureListClosure)                              
  "HdpH.Strategies.evalClosureListClosure"


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
-- static closure forcing functions

-- class of static closure forcers
class (NFData a, DecodeStatic a) => ForceClosureStatic a where
  forceClosureStatic :: Static (Env -> Strategy (Closure a))
  forceClosureStatic = staticAsTD 
    (const forceClosure) 
    "HdpH.Strategies.forceClosure"
    (undefined :: a)

-- NOTE: Every 'ForceClosureStatic' instance also needs to be registered by
--       the 'declareStatic' function (cf. 'DecodeStatic' instances).


------------------------------------------------------------------------------
-- skeletons

-- task farm with parameterised by a strategy
parMap :: (DecodeStatic a)
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
parMapNF :: forall a b . (DecodeStatic a, ForceClosureStatic b)
         => Closure (a -> b)
         -> [a]
         -> Par [b]
parMapNF = parMap clo_strat
  where val = forceClosure :: Strategy (Closure b)
        fun = forceClosureStatic
        clo_strat = unsafeMkClosure0 val fun


-- chunking task farm
parMapChunked :: (DecodeStatic a)
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
parMapChunkedNF :: forall a b . (DecodeStatic a, ForceClosureStatic b)
                => Int
                -> Closure (a -> b)
                -> [a]
                -> Par [b]
parMapChunkedNF n = parMapChunked n clo_strat
  where val = forceClosure :: Strategy (Closure b)
        fun = forceClosureStatic
        clo_strat = unsafeMkClosure0 val fun


-- slicing task farm
parMapSliced :: (DecodeStatic a)
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
parMapSlicedNF :: forall a b . (DecodeStatic a, ForceClosureStatic b)
               => Int
               -> Closure (a -> b)
               -> [a]
               -> Par [b]
parMapSlicedNF n = parMapSliced n clo_strat
  where val = forceClosure :: Strategy (Closure b)
        fun = forceClosureStatic
        clo_strat = unsafeMkClosure0 val fun
