{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE FlexibleInstances #-}

-- A HdpH implementation of the summatory liouville function
--
-- Based largely on code by Rob Stewart <R.Stewart@hw.ac.uk>
--
-- Author: Blair Archibald

module Main where

import Control.Arrow ((&&&))
import Control.Applicative ((<$>))

import Control.Exception (evaluate)

import Control.Monad (zipWithM)

import Control.Parallel.HdpH
import Control.Parallel.HdpH.Conf
import Control.Parallel.HdpH.Strategies
import Control.Parallel.HdpH.Dist (one)

import qualified Control.Parallel.HdpH as HdpH (declareStatic)
import qualified Control.Parallel.HdpH.Strategies as Strategies (declareStatic)

import Data.Monoid (mconcat)
import Data.Random (runRVar)
import Data.Random.Source.DevRandom (DevRandom(..))
import Data.Random.List (shuffle)

import System.Clock
import System.IO
import System.Environment

import Debug.Trace (trace)

type MSTime = Double

-- Instances
instance ToClosure Integer where locToClosure = $(here)
instance ToClosure (Integer,Integer) where locToClosure = $(here)
instance ForceCC Integer where locForceCC = $(here)

-- 2 Level Skeletons
sumLiouvilleHdpH2Level :: Integer ->  Int -> Par Integer
sumLiouvilleHdpH2Level n chunkSize = do
  let cs = map toClosure (chunkedList n chunkSize)
  rs <- parMapM2Level one $(mkClosure [|sumLEvalChunkPar|]) cs
  force $ sum (map unClosure rs)

sumLiouvilleHdpH2LevelRelaxed :: Integer ->  Int -> Par Integer
sumLiouvilleHdpH2LevelRelaxed n chunkSize = do
  let cs = map toClosure (chunkedList n chunkSize)
  rs <- parMapM2LevelRelaxed one $(mkClosure [|sumLEvalChunkPar|]) cs
  force $ sum (map unClosure rs)

sumLEvalChunkPar :: (Integer, Integer) -> Par (Closure Integer)
sumLEvalChunkPar bnds = do
  c <- force $ sumLEvalChunk bnds
  return (toClosure c)

-- Push to 'N' random nodes rather than equidistant basis.
-- Use relaxed work stealing.
sumLiouvilleHdpHPushN :: Integer -> Int -> Int -> Par Integer
sumLiouvilleHdpHPushN n chunkSize pushCount = do
  let cs = map toClosure (chunkedList n chunkSize)
  rs <- pushToN one pushCount $(mkClosure [|sumLEvalChunkPar|]) cs
  force $ sum (map unClosure rs)

pushToN :: Dist                            -- bounding radius
        -> Int                             -- Number of nodes to push to
        -> Closure (a -> Par (Closure b))  -- function closure
        -> [Closure a]                     -- input list
        -> Par [Closure b]                 -- output list
pushToN r n clo_f clo_xs = do
  basis  <- equiDist r
  rBasis <- io $ runRVar (shuffle basis) DevURandom
  let (nodes, ks) = unzip . take n $ rBasis
  let chunks = chunkWith ks clo_xs
  vs <- zipWithM spawn nodes chunks
  concat <$> mapM (\ v -> unClosure <$> get v) vs
    where
      spawn q a_chunk = do
        v <- new
        gv <- glob v
        pushTo $(mkClosure [|parMapM2LevelRelaxed_abs (r,clo_f,a_chunk,gv)|]) q
        return v

parMapM2LevelRelaxed_abs :: (Dist,
                             Closure (a -> Par (Closure b)),
                             [Closure a],
                             GIVar (Closure [Closure b]))
                         -> Thunk (Par ())
parMapM2LevelRelaxed_abs (r, clo_f, a_chunk, gv) =
  Thunk $ parMapMLocal r clo_f a_chunk >>= rput gv . toClosure

-- Chunk list 'as' according to the given list of integers 'ns';
-- the result has as many chunks as the length of 'ns', and the length of
-- the i-th chunk is proportional to the size of n_i.

-- Work Pushing
sumLiouvilleHdpHPushMap :: Integer -> Int -> Par Integer
sumLiouvilleHdpHPushMap n chunkSize = do
  let cs = chunkedList n chunkSize
  ns <- allNodes
  rs <- pushMapNF ns $(mkClosure [|sumLEvalChunk|]) cs
  force $ sum rs

-- Divide and Conquer
sumLiouvilleHdpHDandC :: Integer -> Int -> Par Integer
sumLiouvilleHdpHDandC n minChunkSize = do
  res <- parDivideAndConquer
          $(mkClosure [|trivial minChunkSize|])
          $(mkClosure [|decompose|])
          $(mkClosure [|combine|])
          $(mkClosure [|algorithm|])
          (toClosure (1,n))
  return $ unClosure res

trivial :: Int -> Thunk (Closure (Integer, Integer) -> Bool)
trivial minChunk = Thunk $ \r -> let (s,e) = unClosure r in
                                 e - s < toInteger minChunk

decompose :: Closure (Integer, Integer) -> [Closure (Integer, Integer)]
decompose r = let (s, e) = unClosure r
                  mid    = s + (e - s) `quot` 2
              in
                  [toClosure (s, mid), toClosure (mid+1, e)]

combine :: Closure a -> [Closure Integer] -> Closure Integer
combine _ results = toClosure $ sum $ map unClosure results

algorithm :: Closure (Integer, Integer) -> Par (Closure Integer)
algorithm r = sumLEvalChunkPar (unClosure r)

-- Work stealing
sumLiouvilleHdpHParMap :: Integer -> Int -> Par Integer
sumLiouvilleHdpHParMap n chunkSize = do
  let cs = chunkedList n chunkSize
  rs <- parMapNF $(mkClosure [|sumLEvalChunk|]) cs
  force $ sum rs

-- Work stealing Sliced
sumLiouvilleHdpHParMapSliced :: Integer -> Int -> Par Integer
sumLiouvilleHdpHParMapSliced n chunkSize = do
  let cs = chunkedList n chunkSize
  ns <- allNodes
  rs <- parMapSlicedNF (length ns) $(mkClosure [|sumLEvalChunk|]) cs
  force $ sum rs

-- Liouville Functions
sumLEvalChunk :: (Integer,Integer) -> Integer
sumLEvalChunk (lower,upper) =
  let chunkSize = 1000
      smp_chunks = chunk chunkSize [lower,lower+1..upper] :: [[Integer]]
      tuples      = map (head &&& last) smp_chunks
      in sum $ map (\(l,u) -> sumLEvalChunk' l u 0) tuples

sumLEvalChunk' :: Integer -> Integer -> Integer -> Integer
sumLEvalChunk' lower upper total
  | lower > upper = total
  | otherwise = let s = toInteger $ liouville lower
                    in sumLEvalChunk' (lower+1) upper (total+s)

liouville :: Integer -> Int
liouville n
  | n == 1 = 1
  | length (primeFactors n) `mod` 2 == 0 = 1
  | otherwise = -1

-- Utility Functions

-- Haskell version taken from:
-- http://www.haskell.org/haskellwiki/99_questions/Solutions/35
primeFactors :: Integer -> [Integer]
primeFactors n = factor primesTME n
  where
    factor ps@(p:pt) n | p*p > n      = [n]
      | rem n p == 0 = p : factor ps (quot n p)
      | otherwise    =     factor pt n

-- Tree-merging Eratosthenes sieve
-- producing infinite list of all prime numbers
primesTME :: [Integer]
primesTME = 2 : gaps 3 (join [[p*p,p*p+2*p..] | p <- primes'])
  where
    primes' = 3 : gaps 5 (join [[p*p,p*p+2*p..] | p <- primes'])
    join  ((x:xs):t)        = x : union xs (join (pairs t))
    pairs ((x:xs):ys:t)     = (x : union xs ys) : pairs t
    gaps k xs@(x:t) | k==x  = gaps (k+2) t
      | otherwise  = k : gaps (k+2) xs
    -- duplicates-removing union of two ordered increasing lists
    union (x:xs) (y:ys) = case compare x y of
      LT -> x : union  xs  (y:ys)
      EQ -> x : union  xs     ys
      GT -> y : union (x:xs)  ys

chunk :: Int -> [a] -> [[a]]
chunk _ [] = []
chunk n xs = ys : chunk n zs where (ys,zs) = splitAt n xs

chunkedList :: Integer -> Int -> [(Integer,Integer)]
chunkedList x chunkSize = zip lowers uppers
  where
    lowers       = [1, toInteger (chunkSize + 1) .. x]
    uppers       = [toInteger chunkSize, toInteger (chunkSize*2) .. x]++[x]

chunkWith :: [Int] -> [a] -> [[a]]
chunkWith ns as = chop ns as
  where
    n_as   = length as
    sum_ns = sum ns
    chop []     _  = []
    chop [_]    xs = [xs]
    chop (k:ks) xs = ys : chop ks zs
      where
        (ys,zs) = splitAt chunk_sz xs
        chunk_sz = round (fromIntegral (n_as*k) / fromIntegral sum_ns :: Double)

-- Timing Utilities

timeDiffMSecs :: TimeSpec -> TimeSpec -> MSTime
timeDiffMSecs (TimeSpec s1 n1) (TimeSpec s2 n2) = fromIntegral (t2 - t1)
                                                          /
                                                  fromIntegral (10 ^ 6)
  where t1 = (fromIntegral s1 * 10 ^ 9) + fromIntegral n1
        t2 = (fromIntegral s2 * 10 ^ 9) + fromIntegral n2

timeIOMs :: IO a -> IO (a, MSTime)
timeIOMs action = do s  <- getTime Monotonic
                     x  <- action
                     e  <- getTime Monotonic
                     return (x, timeDiffMSecs s e)

-- TH Hack to allow proper reifying
$(return [])
dncStatics :: StaticDecl
dncStatics = mconcat
              [ declare $(static 'trivial)
              , declare $(static 'decompose)
              , declare $(static 'combine)
              , declare $(static 'algorithm)
              ]

declareStatic :: StaticDecl
declareStatic =
  mconcat
    [HdpH.declareStatic,         -- declare Static deserialisers
     Strategies.declareStatic,   -- from imported modules
     dncStatics,
     declare (staticToClosure :: StaticToClosure Integer),
     declare (staticToClosure :: StaticToClosure (Integer,Integer)),
     declare (staticForceCC   :: StaticForceCC Integer),
     declare $(static 'sumLEvalChunkPar),
     declare $(static 'sumLEvalChunk),
     declare $(static 'pushToN),
     declare $(static 'parMapM2LevelRelaxed_abs)
    ]

parseArgs :: [String] -> Maybe (Int, Integer, Int, Int)
parseArgs [s1,s2,s3,s4] = Just (read s1, read s2, read s3, read s4)
parseArgs [s1,s2,s3]    = Just (read s1, read s2, read s3, 0)
parseArgs _             = Nothing

main :: IO ()
main = do
  hSetBuffering stdout LineBuffering
  hSetBuffering stderr LineBuffering
  register Main.declareStatic

  args <- getArgs
  Right (conf, as) <- updateConf args defaultRTSConf
  let ars = parseArgs as

  --TODO: Tidy up the option handling
  case ars of

    Just (1, n, chunkSize, _) -> do
      (x, t) <- timeIOMs $ evaluate $ sumLEvalChunk (1, n)
      putStrLn $ "Result: "  ++ show x
      putStrLn $ "RUNTIME: " ++ show t ++ "ms"

    Just (2, n, chunkSize, _) -> do
      res <- timeIOMs $ evaluate =<< runParIO conf
                                      (sumLiouvilleHdpHParMap n chunkSize)
      printOutput res

    Just (3, n, chunkSize, _) -> do
      res <- timeIOMs $ evaluate =<< runParIO conf
                                      (sumLiouvilleHdpHParMapSliced n chunkSize)
      printOutput res

    Just (4, n, chunkSize, _) -> do
      res <- timeIOMs $ evaluate =<< runParIO conf
                                      (sumLiouvilleHdpHPushMap n chunkSize)
      printOutput res

    Just (5, n, chunkSize, _) -> do
      res <- timeIOMs $ evaluate =<< runParIO conf
                                      (sumLiouvilleHdpH2Level n chunkSize)
      printOutput res

    Just (6, n, chunkSize, _) -> do
      res <- timeIOMs $ evaluate =<< runParIO conf
                                      (sumLiouvilleHdpHDandC n chunkSize)
      printOutput res

    Just (7, n, chunkSize, numPush) ->
      if numPush == 0
       then error "sumLiouvilleHdpHPushN: Expecting extra argument <numPush>"
       else do
        res <- timeIOMs $ evaluate =<< runParIO conf
                                      (sumLiouvilleHdpHPushN n chunkSize numPush)
        printOutput res

    Just (8, n, chunkSize, _) -> do
      res <- timeIOMs $ evaluate =<< runParIO conf
                                      (sumLiouvilleHdpH2LevelRelaxed n chunkSize)
      printOutput res

    Just _  -> putStrLn "Usage: sum-liouville <version> <n> <chunkSize> <numPush>"
    Nothing -> putStrLn "Usage: sum-liouville <version> <n> <chunkSize> <numPush>"

printOutput :: Show a => (Maybe a, MSTime) -> IO ()
printOutput (x, t) = case x of
                      Just x' -> do putStrLn $ "RESULT: "  ++ show x'
                                    putStrLn $ "RUNTIME: " ++ show t ++ "ms"
                      Nothing -> return ()
