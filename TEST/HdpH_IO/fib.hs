-- Fibonacci numbers in HdpH_IO
--
-- Visibility: HdpH test suite
-- Author: Patrick Maier <P.Maier@hw.ac.uk>
-- Created: 17 Jul 2011
--
-----------------------------------------------------------------------------

{-# LANGUAGE TemplateHaskell #-}

module Main where

import Prelude
import Control.Concurrent (forkIO)
import Control.DeepSeq (NFData, deepseq)
import Control.Monad (when)
import Data.List (elemIndex, stripPrefix)
import Data.Maybe (fromJust)
import System.Environment (getArgs)
import System.IO (stdout, stderr, hSetBuffering, BufferMode(..))
import System.Random (mkStdGen, setStdGen, randomRIO)

import MP.MPI (defaultWithMPI, myRank, allRanks)
import HdpH_IO (withHdpH,
                allNodes,
                pushTo, delay,
                new, get, put,
                GIVar, glob, rput,
                Closure, unClosure, toClosure, mkClosure, static,
                StaticId, staticIdTD, register)


-----------------------------------------------------------------------------
-- 'Static' registration

instance StaticId Integer

registerStatic :: IO ()
registerStatic = do
  register $ staticIdTD (undefined :: Integer)
  register $(static 'dist_fib_abs)
  register $(static 'dist_fib_nb_abs)


-----------------------------------------------------------------------------
-- sequential Fibonacci

fib :: Int -> Integer
fib n | n <= 1    = 1
      | otherwise = fib (n-1) + fib (n-2)


-----------------------------------------------------------------------------
-- parallel Fibonacci; shared memory IO threads

par_fib :: Int -> Int -> IO Integer
par_fib seqThreshold n
  | n <= k    = return $ force $ fib n
  | otherwise = do v <- new
                   let job = par_fib seqThreshold (n - 1) >>=
                             put v . force
                   forkIO job
                   y <- par_fib seqThreshold (n - 2)
                   x <- get v
                   return $ force $ x + y
  where k = max 1 seqThreshold


-----------------------------------------------------------------------------
-- distributed Fibonacci; explicit random placement; blocking handler crash

dist_fib :: Int -> Int -> Int -> IO Integer
dist_fib seqThreshold parThreshold n
  | n <= k    = return $ force $ fib n
  | n <= l    = par_fib seqThreshold n
  | otherwise = do
      v <- new
      gv <- glob v
      nodes <- allNodes
      randomTarget <- randomElem nodes
      let job =
            $(mkClosure [| dist_fib_abs (seqThreshold,parThreshold,n,gv) |])
      pushTo job randomTarget
      y <- dist_fib seqThreshold parThreshold (n - 2)
      clo_x <- get v   -- block handler here!
      return $ force $ unClosure clo_x + y
  where k = max 1 seqThreshold
        l = parThreshold

dist_fib_abs :: (Int, Int, Int, GIVar (Closure Integer)) -> IO ()
dist_fib_abs (seqThreshold, parThreshold, n, gv) =
  dist_fib seqThreshold parThreshold (n - 1) >>=
  rput gv . toClosure . force


-----------------------------------------------------------------------------
-- distributed Fibonacci; explicit random placement; not blocking handler

dist_fib_nb :: Int -> Int -> Int -> IO Integer
dist_fib_nb seqThreshold parThreshold n
  | n <= k    = return $ force $ fib n
  | n <= l    = par_fib seqThreshold n
  | otherwise = do
      v <- new
      gv <- glob v
      nodes <- allNodes
      randomTarget <- randomElem nodes
      let job =
            $(mkClosure [| dist_fib_nb_abs (seqThreshold,parThreshold,n,gv) |])
      pushTo job randomTarget
      y <- dist_fib_nb seqThreshold parThreshold (n - 2)
      clo_x <- get v   -- does not block handler due to forkIO
      return $ force $ unClosure clo_x + y
  where k = max 1 seqThreshold
        l = parThreshold

dist_fib_nb_abs :: (Int, Int, Int, GIVar (Closure Integer)) -> IO ()
dist_fib_nb_abs (seqThreshold, parThreshold, n, gv) =
  forkIO_ $  -- fork to avoid blocking handler below
  -- delay $  -- delay; may lead to deadlock
    dist_fib_nb seqThreshold parThreshold (n - 1) >>=
    rput gv . toClosure . force


-----------------------------------------------------------------------------
-- initialisation, argument processing and 'main'

-- initialize random number generator
initrand :: Int -> IO ()
initrand seed = do
  when (seed /= 0) $ do
    ranks <- allRanks
    self <- myRank
    let i = fromJust $ elemIndex self ranks
    setStdGen (mkStdGen (seed + i))


-- parse runtime system config options (ie. seed for random number generator)
parseOpts :: [String] -> (Int, [String])
parseOpts args = go (0, args) where
  go :: (Int, [String]) -> (Int, [String])
  go (seed, [])   = (seed, [])
  go (seed, s:ss) =
    case stripPrefix "-rand=" s of
      Just s  -> go (read s, ss)
      Nothing -> (seed, s:ss)


-- parse (optional) arguments in this order: 
-- * version to run
-- * argument to Fibonacci function
-- * threshold below which to execute sequentially
-- * threshold below which to use shared-memory parallelism
parseArgs :: [String] -> (Int, Int, Int, Int)
parseArgs []     = (defVers, defN, defSeqThreshold, defParThreshold)
parseArgs (s:ss) =
  let go :: Int -> [String] -> (Int, Int, Int, Int)
      go v []           = (v, defN,    defSeqThreshold, defParThreshold)
      go v [s1]         = (v, read s1, defSeqThreshold, defParThreshold)
      go v [s1,s2]      = (v, read s1, read s2,         read s2)
      go v (s1:s2:s3:_) = (v, read s1, read s2,         read s3)
  in case stripPrefix "v" s of
       Just s' -> go (read s') ss
       Nothing -> go defVers (s:ss)

-- defaults for optional arguments
defVers         =  3 :: Int  -- version
defN            = 40 :: Int  -- Fibonacci argument
defParThreshold = 30 :: Int  -- shared-memory threshold
defSeqThreshold = 30 :: Int  -- sequential threshold


main :: IO ()
main = do
  hSetBuffering stdout LineBuffering
  hSetBuffering stderr LineBuffering
  registerStatic
  defaultWithMPI $ do
    opts_args <- getArgs
    let (seed, args) = parseOpts opts_args
    let (version, n, seqThreshold, parThreshold) = parseArgs args
    initrand seed
    case version of
      0 -> do x <- return $ fib n
              putStrLn $ "{v0} fib " ++ show n ++ " = " ++ show x
      1 -> do x <- par_fib seqThreshold n
              putStrLn $
                "{v1, " ++ 
                "seqThreshold=" ++ show seqThreshold ++ "} " ++
                "fib " ++ show n ++ " = " ++ show x
      2 -> do output <- withHdpH $
                          dist_fib seqThreshold parThreshold n
              case output of
                Just x  -> putStrLn $
                             "{v2, " ++
                             "seqThreshold=" ++ show seqThreshold ++ ", " ++
                             "parThreshold=" ++ show parThreshold ++ "} " ++
                            "fib " ++ show n ++ " = " ++ show x
                Nothing -> return ()
      3 -> do output <- withHdpH $
                          dist_fib_nb seqThreshold parThreshold n
              case output of
                Just x  -> putStrLn $
                             "{v3, " ++
                             "seqThreshold=" ++ show seqThreshold ++ ", " ++
                             "parThreshold=" ++ show parThreshold ++ "} " ++
                            "fib " ++ show n ++ " = " ++ show x
                Nothing -> return ()
      _ -> return ()


-----------------------------------------------------------------------------
-- auxiliaries

-- fork an IO thread and ignore thread ID
forkIO_ :: IO () -> IO ()
forkIO_ job = forkIO job >> return ()


-- force to normal form; taken from deepseq-1.2.0.1
force :: (NFData a) => a -> a
force x = x `deepseq` x


-- 'randomElem xs' returns a random element of the list 'xs'
-- different from any of the elements in the set 'avoid'.
-- Requirements: 'n <= length xs' and 'xs' contains no duplicates.
randomElem :: [a] -> IO a
randomElem xs = do
  i <- randomRIO (0, length xs - 1)
  -- 0 <= i < length xs
  return (xs !! i)
