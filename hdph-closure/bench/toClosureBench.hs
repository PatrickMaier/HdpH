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
        toClosure, ToClosure(locToClosure),
        static, StaticToClosure, staticToClosure,
        StaticDecl, declare, register, here)
import qualified Control.Parallel.HdpH.Closure (declareStatic)


toClosureIntList :: [Int] -> Closure [Int]
toClosureIntList xs = $(mkClosure [| toClosureIntList_abs xs |])

toClosureIntList_abs :: [Int] -> Thunk [Int]
toClosureIntList_abs xs = Thunk xs


-----------------------------------------------------------------------------
-- Static declaration (just before 'main')

-- Empty splice; TH hack to make all environment abstractions visible.
$(return [])

-- orphan ToClosure instances (unavoidably so)
instance ToClosure [Int] where locToClosure = $(here)

declareStatic :: StaticDecl
declareStatic =
  mconcat
    [Control.Parallel.HdpH.Closure.declareStatic,
     declare $(static 'toClosureIntList_abs),
     declare (staticToClosure :: StaticToClosure [Int])]


-----------------------------------------------------------------------------
-- main

mkIntList :: Int -> [Int]
mkIntList n = [1 .. n]

main = defaultMain $
  [let xs = mkIntList n in
     bgroup ("whnf [1.." ++ show n ++ "]") [
       bench "id" $ whnf id xs,
       bench "toClosureIntList" $ whnf toClosureIntList xs,
       bench "toClosure" $ whnf toClosure xs,
       bench "unClosure" $ whnf unClosure (toClosure xs),
       bench "unClosure.toClosure" $ whnf (unClosure . toClosure) xs,
       bench "unClosure.toClosureIntList" $ whnf (unClosure . toClosureIntList) xs]
  | n <- [0, 1, 300]]
  ++
  [let xs = mkIntList n in
     bgroup ("nf [1.." ++ show n ++ "]") [
       bench "id" $ nf id xs,
       bench "toClosureIntList" $ nf toClosureIntList xs,
       bench "toClosure" $ nf toClosure xs,
       bench "unClosure" $ nf unClosure (toClosure xs),
       bench "unClosure.toClosure" $ nf (unClosure . toClosure) xs,
       bench "unClosure.toClosureIntList" $ nf (unClosure . toClosureIntList) xs]
  | n <- [0, 1, 300]]
