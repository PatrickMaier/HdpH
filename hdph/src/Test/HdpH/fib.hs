-- Fibonacci numbers in HdpH
--
-- Author: Patrick Maier
-----------------------------------------------------------------------------

{-# LANGUAGE TemplateHaskell #-}  -- req'd for mkClosure, etc

{-# OPTIONS_GHC -fno-warn-orphans #-}

module Main where

import Prelude
import Control.Exception (evaluate)
import Control.Monad (when)
import Data.Functor ((<$>))
import Data.List (stripPrefix)
import Data.Monoid (mconcat)
import Data.Time.Clock (NominalDiffTime, diffUTCTime, getCurrentTime)
import System.Environment (getArgs)
import System.IO (stdout, stderr, hSetBuffering, BufferMode(..))
import System.Random (mkStdGen, setStdGen)

import Control.Parallel.HdpH
       (RTSConf(..), defaultRTSConf, updateConf,
        Par, runParIO,
        allNodes, force, fork, spark, new, get, put, glob, rput,
        GIVar, Node,
        Thunk(Thunk), Closure, unClosure, mkClosure,
        toClosure, ToClosure(locToClosure),
        static, StaticToClosure, staticToClosure,
        StaticDecl, declare, register, here)
import qualified Control.Parallel.HdpH as HdpH (declareStatic)
import Control.Parallel.HdpH.Dist (one)
import Control.Parallel.HdpH.Strategies 
       (parDivideAndConquer, pushDivideAndConquer)
import qualified Control.Parallel.HdpH.Strategies as Strategies (declareStatic)


-----------------------------------------------------------------------------
-- sequential Fibonacci

fib :: Int -> Integer
fib n | n <= 1    = 1
      | otherwise = fib (n-1) + fib (n-2)


-----------------------------------------------------------------------------
-- parallel Fibonacci; shared memory

par_fib :: Int -> Int -> Par Integer
par_fib seqThreshold n
  | n <= k    = force $ fib n
  | otherwise = do v <- new
                   let job = par_fib seqThreshold (n - 1) >>=
                             force >>=
                             put v
                   fork job
                   y <- par_fib seqThreshold (n - 2)
                   x <- get v
                   force $ x + y
  where k = max 1 seqThreshold


-----------------------------------------------------------------------------
-- parallel Fibonacci; distributed memory

dist_fib :: Int -> Int -> Int -> Par Integer
dist_fib seqThreshold parThreshold n
  | n <= k    = force $ fib n
  | n <= l    = par_fib seqThreshold n
  | otherwise = do
      v <- new
      gv <- glob v
      spark one $(mkClosure [| dist_fib_abs (seqThreshold, parThreshold, n, gv) |])
      y <- dist_fib seqThreshold parThreshold (n - 2)
      clo_x <- get v
      force $ unClosure clo_x + y
  where k = max 1 seqThreshold
        l = parThreshold

dist_fib_abs :: (Int, Int, Int, GIVar (Closure Integer)) -> Thunk (Par ())
dist_fib_abs (seqThreshold, parThreshold, n, gv) =
  Thunk $ dist_fib seqThreshold parThreshold (n - 1) >>=
          force >>=
          rput gv . toClosure


-----------------------------------------------------------------------------
-- parallel Fibonacci; distributed memory; using sparking d-n-c skeleton

spark_skel_fib :: Int -> Int -> Par Integer
spark_skel_fib seqThreshold n = unClosure <$> skel (toClosure n)
  where 
    skel = parDivideAndConquer
             $(mkClosure [| dnc_trivial_abs (seqThreshold) |])
             $(mkClosure [| dnc_decompose |])
             $(mkClosure [| dnc_combine |])
             $(mkClosure [| dnc_f |])

dnc_trivial_abs :: Int -> Thunk (Closure Int -> Bool)
dnc_trivial_abs (seqThreshold) =
  Thunk $ \ clo_n -> unClosure clo_n <= max 1 seqThreshold

dnc_decompose :: Closure Int -> [Closure Int]
dnc_decompose =
  \ clo_n -> let n = unClosure clo_n in [toClosure (n-1), toClosure (n-2)]

dnc_combine :: Closure a -> [Closure Integer] -> Closure Integer
dnc_combine =
  \ _ clos -> toClosure $ sum $ map unClosure clos

dnc_f :: Closure Int -> Par (Closure Integer)
dnc_f =
  \ clo_n -> toClosure <$> (force $ fib $ unClosure clo_n)


-----------------------------------------------------------------------------
-- parallel Fibonacci; distributed memory; using pushing d-n-c skeleton

push_skel_fib :: [Node] -> Int -> Int -> Par Integer
push_skel_fib nodes seqThreshold n = unClosure <$> skel (toClosure n)
  where 
    skel = pushDivideAndConquer
             nodes
             $(mkClosure [| dnc_trivial_abs (seqThreshold) |])
             $(mkClosure [| dnc_decompose |])
             $(mkClosure [| dnc_combine |])
             $(mkClosure [| dnc_f |])


-----------------------------------------------------------------------------
-- Static declaration (just before 'main')

-- Empty splice; TH hack to make all environment abstractions visible.
$(return [])

-- orphan ToClosure instances (unavoidably so)
instance ToClosure Int where locToClosure = $(here)
instance ToClosure Integer where locToClosure = $(here)

declareStatic :: StaticDecl
declareStatic =
  mconcat
    [HdpH.declareStatic,         -- declare Static deserialisers
     Strategies.declareStatic,   -- from imported modules
     declare (staticToClosure :: StaticToClosure Int),
     declare (staticToClosure :: StaticToClosure Integer),
     declare $(static 'dist_fib_abs),
     declare $(static 'dnc_trivial_abs),
     declare $(static 'dnc_decompose),
     declare $(static 'dnc_combine),
     declare $(static 'dnc_f)]


-----------------------------------------------------------------------------
-- initialisation, argument processing and 'main'

-- time an IO action
timeIO :: IO a -> IO (a, NominalDiffTime)
timeIO action = do t0 <- getCurrentTime
                   x <- action
                   t1 <- getCurrentTime
                   return (x, diffUTCTime t1 t0)


-- initialize random number generator
initrand :: Int -> IO ()
initrand seed = do
  when (seed /= 0) $ do
    setStdGen (mkStdGen seed)


-- parse runtime system config options (+ seed for random number generator)
-- abort if there is an error
parseOpts :: [String] -> IO (RTSConf, Int, [String])
parseOpts args = do
  either_conf <- updateConf args defaultRTSConf
  case either_conf of
    Left err_msg             -> error $ "parseOpts: " ++ err_msg
    Right (conf, [])         -> return (conf, 0, [])
    Right (conf, arg':args') ->
      case stripPrefix "-rand=" arg' of
        Just s  -> return (conf, read s, args')
        Nothing -> return (conf, 0,      arg':args')


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
defVers, defN, defParThreshold, defSeqThreshold :: Int
defVers         =  2  -- version
defN            = 40  -- Fibonacci argument
defParThreshold = 30  -- shared-memory threshold
defSeqThreshold = 30  -- sequential threshold


main :: IO ()
main = do
  hSetBuffering stdout LineBuffering
  hSetBuffering stderr LineBuffering
  register declareStatic
  opts_args <- getArgs
  (conf, seed, args) <- parseOpts opts_args
  let (version, n, seqThreshold, parThreshold) = parseArgs args
  initrand seed
  case version of
      0 -> do (x, t) <- timeIO $ evaluate
                          (fib n)
              outputResults $ ["v0","0","0", show n, show x, show t]
      1 -> do (output, t) <- timeIO $ evaluate =<< runParIO conf
                               (par_fib seqThreshold n)
              case output of
                Just x  -> outputResults $ ["v1",show seqThreshold, "0", show n, show x, show t]
                Nothing -> return ()
      2 -> do (output, t) <- timeIO $ evaluate =<< runParIO conf
                               (dist_fib seqThreshold parThreshold n)
              case output of
                Just x  -> outputResults $ ["v2",show seqThreshold, show parThreshold, show n, show x, show t]
                Nothing -> return ()
      3 -> do (output, t) <- timeIO $ evaluate =<< runParIO conf
                               (spark_skel_fib seqThreshold n)
              case output of
                Just x  -> outputResults $ ["v3",show seqThreshold, "0", show n, show x, show t]
                Nothing -> return ()
      4 -> do (output, t) <- timeIO $ evaluate =<< runParIO conf
                               (allNodes >>= \ nodes ->
                                push_skel_fib nodes seqThreshold n)
              case output of
                Just x  -> outputResults $ ["v4",show seqThreshold, "0", show n, show x, show t]
                Nothing -> return ()
      _ -> return ()

outputResults :: [String] -> IO ()
outputResults [version, seqT, parT, input, output, runtime] =
  mapM_ printTags $ zip tags [version, seqT, parT, input, output, runtime]
    where tags = ["Version: ","SequentialThreshold: ", "ParallelThreshold: ", "Input: ","Output: ","Runtime: "]
          printTags (a,b) = putStrLn (a ++ b)
outputResults _ = putStrLn "Not enough arguments to output results"
