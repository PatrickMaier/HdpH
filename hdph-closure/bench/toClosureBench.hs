-- Benchmark toClosure and unClosure.
--
-- Author: Patrick Maier
-----------------------------------------------------------------------------

{-# OPTIONS_GHC -fno-warn-orphans #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE BangPatterns #-}

module Main where

import Prelude
import Criterion.Main (bench, bgroup, defaultMain, nf, whnf)
import Data.Monoid (mconcat)

import Control.Parallel.HdpH.Closure
       (Closure, Thunk(Thunk), mkClosure, unClosure,
        toClosure, ToClosure, mkToClosure,
        static, StaticDecl, declare, register)
import qualified Control.Parallel.HdpH.Closure (declareStatic)


toClosureListInt :: [Int] -> Closure [Int]
toClosureListInt xs = $(mkClosure [| toClosureListInt_abs xs |])

toClosureListInt_abs :: [Int] -> Thunk [Int]
toClosureListInt_abs xs = Thunk xs


-----------------------------------------------------------------------------
-- Static declaration (just before 'main')

tcListInt = mkToClosure :: ToClosure [Int]

-- Empty splice; TH hack to make all environment abstractions visible.
$(return [])

declareStatic :: StaticDecl
declareStatic =
  mconcat
    [Control.Parallel.HdpH.Closure.declareStatic,
     declare $(static 'toClosureListInt_abs),
     declare tcListInt]


-----------------------------------------------------------------------------
-- main

mkListInt :: Int -> [Int]
mkListInt n = [1 .. n]

main = defaultMain $
  [let xs = mkListInt n in
     bgroup ("whnf [1.." ++ show n ++ "]") [
       bench "id" $ whnf id xs,
       bench "toClosureListInt" $ whnf toClosureListInt xs,
       bench "toClosure" $ whnf (toClosure tcListInt) xs,
       bench "unClosure" $ whnf unClosure (toClosure tcListInt xs),
       bench "unClosure.toClosure" $ whnf (unClosure . toClosure tcListInt) xs,
       bench "unClosure.toClosureListInt" $ whnf (unClosure . toClosureListInt) xs]
  | n <- [0, 1, 300]]
  ++
  [let xs = mkListInt n in
     bgroup ("nf [1.." ++ show n ++ "]") [
       bench "id" $ nf id xs,
       bench "toClosureListInt" $ nf toClosureListInt xs,
       bench "toClosure" $ nf (toClosure tcListInt) xs,
       bench "unClosure" $ nf unClosure (toClosure tcListInt xs),
       bench "unClosure.toClosure" $ nf (unClosure . toClosure tcListInt) xs,
       bench "unClosure.toClosureListInt" $ nf (unClosure . toClosureListInt) xs]
  | n <- [0, 1, 300]]
