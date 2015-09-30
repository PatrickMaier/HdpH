-- | Benchmarks for the HdpH-closure representation


{-# OPTIONS_GHC -fno-warn-orphans #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE BangPatterns #-}

module Main where

import Criterion.Main

import Data.Int (Int32)
import Data.Serialize (encode, decode)
import Control.DeepSeq (deepseq)
import Control.Exception (evaluate)

import qualified Data.ByteString as BS
import qualified Data.IntSet as VertexSet

import Control.Parallel.HdpH.Closure
       (Closure, unClosure,
        toClosure, ToClosure(locToClosure),
        StaticToClosure, staticToClosure,
        StaticDecl, declare, register, here,
        mkClosure, Thunk(Thunk), static)

import qualified Control.Parallel.HdpH.Closure (declareStatic)

-- From ToClosure Benchmark

-- Do we really need to do the sum's, probably not.

measureListListInt :: [[Int]] -> (Int, Int)
measureListListInt xss =
    let !bs  = encode xss
        !sz = BS.length bs
        !z  = sum $ concat $ either error id $ (decode bs :: Either String [[Int]])
    in (z, sz)

measureCloListListInt :: [[Int]] -> (Int, Int)
measureCloListListInt xss =
    let clo = toClosure xss :: Closure [[Int]]
        !bs = encode clo
        !sz = BS.length bs
        !z  = sum $ concat $ unClosure $ either error id $ (decode bs :: Either String (Closure [[Int]]))
    in (z, sz)

measureListCloListInt :: [[Int]] -> (Int, Int)
measureListCloListInt xss =
    let clos = map toClosure xss :: [Closure [Int]]
        !bs  = encode clos
        !sz  = BS.length bs
        !z   = sum $ concat $ map unClosure $ either error id $ (decode bs :: Either String ([Closure [Int]]))
    in (z, sz)

measureCloListCloListInt :: [[Int]] -> (Int, Int)
measureCloListCloListInt xss =
    let clos = map toClosure xss :: [Closure [Int]]
        !clo = toClosure clos :: Closure [Closure [Int]]
        !bs  = encode clo
        !sz  = BS.length bs
        !z   = sum $ concat $ map unClosure $ unClosure $
                either error id $ (decode bs :: Either String (Closure [Closure [Int]]))
    in (z, sz)

-- Overhead

redNative :: [[Int32]] -> Int32
redNative xss = sum $ map reduce xss

-- list of closures
redClosure :: [[Int32]] -> Int32
redClosure xss =
  let !clos = map (\ xs -> $(mkClosure [| reduce_abs xs |])) xss
  in  sum $ map unClosure clos

reduce_abs :: [Int32] -> Thunk Int32
reduce_abs xs = Thunk $ reduce xs

-- list of closures serialised to ByteStrings
redClosureBS :: [[Int32]] -> Int32
redClosureBS xss =
  let !bs = map (\ xs -> encode $(mkClosure [| reduce_abs xs |])) xss
  in  sum $ map (unClosure . either error id . decode) bs

-- Helper Functions
generate c = chunk c . iterate poly

-- reduce a lists of 32-bit ints
reduce :: [Int32] -> Int32
reduce = sum . map poly

-- some polynomial on Int32
poly :: Int32 -> Int32
poly v = 1 * v^12 + 5 * v^10 + 13 * v^8 + 25 * v^6 + 40 * v^4 + 55 *v^2 + 67 -
      75 * v - 78 * v^3 - 75 * v^5 - 67 * v^7 - 55 * v^9 - 40 * v^11 -
      25 * v^13 - 13* v^15 - 5 * v^17 - 1 * v^19

chunk :: Int -> [a] -> [[a]]
chunk k xs | k <= 0 = error "chunk: chunk size non-positive"
           | otherwise = go xs
              where
                go [] = []
                go zs = pref : go suff where (pref,suff) = splitAt k zs

-- Nested

redFlat :: [[Int32]] -> Int32
redFlat xss = let !clo = mkFlat xss
              in unClosure clo

redNested :: [[Int32]] -> Int32
redNested xss = let !clo = mkNested xss
                in unClosure clo

mkFlat :: [[Int32]] -> Closure Int32
mkFlat xss = $(mkClosure [| mkFlat_abs xss |])

mkFlat_abs :: [[Int32]] -> Thunk Int32
mkFlat_abs xss = Thunk $ sum $ map reduce xss

mkNested :: [[Int32]] -> Closure Int32
mkNested xss = $(mkClosure [| mkNested_abs clos |])
  where
    clos = map (\ xs -> $(mkClosure [| reduce_abs xs |])) xss

mkNested_abs :: [Closure Int32] -> Thunk Int32
mkNested_abs clos = Thunk $ sum $ map unClosure clos

-- Max Clique Style Closures
type Vertex = Int

parMaxCliqueSkel_gen :: Closure [Vertex]
                     -> Closure VertexSet.IntSet
                     -> (Vertex, Int)
parMaxCliqueSkel_gen cur remaining =
  let !vs = unClosure remaining
  in (1,1)

parMaxCliqueSkel_prune :: Closure (Vertex, Int)
                       -> Closure [Vertex]
                       -> Closure Int
                       -> Bool
parMaxCliqueSkel_prune col sol bnd =
  let (v,c) = unClosure col
      b     = unClosure bnd
      sol'  = unClosure sol
  in (length sol') + c <= b

parMaxCliqueSkel_updateSolution :: Closure (Vertex, Int)
                                -> Closure [Vertex]
                                -> Closure VertexSet.IntSet
                                -> (Closure [Vertex],
                                    Closure Int,
                                    Closure VertexSet.IntSet)
parMaxCliqueSkel_updateSolution c sol vs =
  let (v,col) = unClosure c
      sol'    = unClosure sol
      !vs'    = unClosure vs
      newSol  = (v:sol')
      newRemaining = VertexSet.fromAscList [1,2,3,4,5]

  in (toClosure newSol, toClosure (length newSol), toClosure newRemaining)

updateSolFunction :: (Closure (Closure (Vertex, Int)
                                -> Closure [Vertex]
                                -> Closure VertexSet.IntSet
                                -> (Closure [Vertex],
                                    Closure Int,
                                    Closure VertexSet.IntSet))
                   , Closure (Vertex, Int)
                   , Closure [Vertex]
                   , Closure VertexSet.IntSet)
                   -> (Closure [Vertex], Closure Int, Closure VertexSet.IntSet)
updateSolFunction (fn,x,y,z) = let f = unClosure fn in f x y z

pruneFunction :: (Closure (Closure (Vertex, Int)
                           -> Closure [Vertex]
                           -> Closure Int
                           -> Bool),
                  Closure (Vertex, Int),
                  Closure [Vertex],
                  Closure Int)
                 -> Bool
pruneFunction (fn, x, y, z) = let f = unClosure fn in f x y z


closureColourOrders :: [(Vertex, Int)] -> [Closure (Vertex, Int)]
closureColourOrders = map toClosure

toClosureInt :: Int -> Closure Int
toClosureInt x = $(mkClosure [| toClosureInt_abs x |])

toClosureInt_abs :: Int -> Thunk Int
toClosureInt_abs x = Thunk x

toClosureListVertex :: [Vertex] -> Closure [Vertex]
toClosureListVertex x = $(mkClosure [| toClosureListVertex_abs x |])

toClosureListVertex_abs :: [Vertex] -> Thunk [Vertex]
toClosureListVertex_abs x = Thunk x

toClosureVertexSet :: VertexSet.IntSet -> Closure VertexSet.IntSet
toClosureVertexSet x = $(mkClosure [| toClosureVertexSet_abs x |])

toClosureVertexSet_abs :: VertexSet.IntSet -> Thunk VertexSet.IntSet
toClosureVertexSet_abs x = Thunk x

$(return [])
instance ToClosure Int where locToClosure = $(here)
instance ToClosure [Int] where locToClosure = $(here)
instance ToClosure [[Int]] where locToClosure = $(here)
instance ToClosure VertexSet.IntSet where locToClosure = $(here)
instance ToClosure (Vertex, Int) where locToClosure = $(here)

declareStatic :: StaticDecl
declareStatic =
  mconcat
    [Control.Parallel.HdpH.Closure.declareStatic,
     declare (staticToClosure :: StaticToClosure Int),
     declare (staticToClosure :: StaticToClosure [Int]),
     declare (staticToClosure :: StaticToClosure [[Int]]),
     declare (staticToClosure :: StaticToClosure VertexSet.IntSet),
     declare (staticToClosure :: StaticToClosure (Vertex,Int)),
     declare $(static 'mkFlat_abs),
     declare $(static 'mkNested_abs),
     declare $(static 'reduce_abs),
     declare $(static 'toClosureVertexSet_abs),
     declare $(static 'toClosureInt_abs),
     declare $(static 'toClosureListVertex_abs)
    ]


main :: IO ()
main = do
 register declareStatic
 defaultMain [
     bgroup "toClosure" [
         bench "[[Int]] 10*10" $ nf
           measureListListInt (chunk 10 $ take 100 [1 .. ])
       , bench "[[Int]] 100*100" $ nf
           measureListListInt (chunk 100 $ take 10000 [1 .. ])
       ,  bench "Closure [[Int]] 10*10" $ nf
           measureCloListListInt (chunk 10 $ take 100 [1 .. ])
       , bench "Closure [[Int]] 100*100" $ nf
           measureCloListListInt (chunk 100 $ take 10000 [1 .. ])
       ,  bench "[Closure [Int]] 10*10" $ nf
           measureListCloListInt (chunk 10 $ take 100 [1 .. ])
       , bench "[Closure [Int]] 100*100" $ nf
           measureListCloListInt (chunk 100 $ take 10000 [1 .. ])
       ,  bench "Closure [Closure [Int]] 10*10" $ nf
           measureCloListCloListInt (chunk 10 $ take 100 [1 .. ])
       , bench "Closure [Closure [Int]] 100*100" $ nf
           measureCloListCloListInt (chunk 100 $ take 10000 [1 .. ])
       ]
   ,  bgroup "Overhead" [
         bench "Reduce [[Int]] 10*10" $ nf
           redNative (chunk 10 $ take 100 [1 .. ])
       , bench "Reduce [[Int]] 100*100" $ nf
           redNative (chunk 100 $ take 10000 [1 .. ])
       , bench "Reduce [Closure [Int]] 10*10" $ nf
           redClosure (chunk 10 $ take 100 [1 .. ])
       , bench "Reduce [Closure [Int]] 100*100" $ nf
           redClosure (chunk 100 $ take 10000 [1 .. ])
         ]
   ,  bgroup "Nested Functions" [
         bench "Reduce Flat [[Int]] 10*10" $ nf
           redFlat (chunk 10 $ take 100 [1 .. ])
       , bench "Reduce Flat [[Int]] 100*100" $ nf
           redFlat (chunk 100 $ take 10000 [1 .. ])
       , bench "Reduce Nested [[Int]] 10*10" $ nf
           redNested (chunk 10 $ take 100 [1 .. ])
       , bench "Reduce Nested [[Int]] 100*100" $ nf
           redNested (chunk 100 $ take 10000 [1 .. ])
         ]
   , bgroup "MaxClique" [
         bench "toClosure Vertex" $ nf
           toClosure (10 :: Int)
       , bench "Explicit toClosure Vertex" $ nf
           toClosureInt (10 :: Int)
       , bench "unClosure Vertex" $ nf
           unClosure (toClosure 10 :: Closure Int)
       , bench "toClosure [Vertex]" $ nf
           toClosure ([1,2,3,4,5,6,7,8,9,10] :: [Int])
       , bench "Explicit toClosure [Vertex]" $ nf
           toClosureListVertex ([1,2,3,4,5,6,7,8,9,10] :: [Int])
       , bench "unClosure [Vertex]" $ nf
           unClosure (toClosure [1,2,3,4,5,6,7,8,9,10] :: Closure [Int])
       , bench "toClosure VertexSet" $ nf
           toClosure (VertexSet.fromAscList [1,2,3,4,5,6,7,8,9,10])
       , bench "Explicity toClosure VertexSet" $ nf
           toClosureVertexSet (VertexSet.fromAscList [1,2,3,4,5,6,7,8,9,10])
       , bench "unClosure VertexSet" $ nf
           unClosure (toClosure (VertexSet.fromAscList [1,2,3,4,5,6,7,8,9,10]))
       , bench "unClosure genFunction" $ nf
            unClosure ($(mkClosure [| parMaxCliqueSkel_gen |]))
       , bench "toClosure Colour Orders" $ nf
            closureColourOrders (replicate 10 (1,1))
       , bench "Apply PruneFunction" $ nf
            pruneFunction
                ($(mkClosure [| parMaxCliqueSkel_prune |]),
                toClosure (1,5),
                toClosure [1,2,3,4,5] :: Closure [Vertex],
                toClosure 6)
       , bench "Apply UpdateSolutions" $ nf
            updateSolFunction
                ($(mkClosure [| parMaxCliqueSkel_updateSolution |])
                , toClosure (1,5) :: Closure (Vertex, Int)
                , toClosure [2,3,4,5] :: Closure [Vertex]
                , toClosure (VertexSet.fromAscList [1,2,3,4,5,6,7,8]))
         ]

   ]
