-- Fibonacci numbers in HdpH
--
-- Author: Patrick Maier
-----------------------------------------------------------------------------

{-# LANGUAGE TemplateHaskell #-}  -- req'd for mkClosure, etc

module Main where

import Prelude
import Control.Exception (evaluate)
import Control.Monad (when)
import Data.Functor ((<$>))
import Data.List (elemIndex, stripPrefix)
import Data.Maybe (fromJust)
import Data.Monoid (mconcat)
import Data.Time.Clock (NominalDiffTime, diffUTCTime, getCurrentTime)
import System.Environment (getArgs)
import System.IO (stdout, stderr, hSetBuffering, BufferMode(..))
import System.Random (mkStdGen, setStdGen)

import Control.Parallel.HdpH
       (RTSConf(..), defaultRTSConf,
        Par, runParIO,
        allNodes, force, fork, spark, new, get, put, glob, rput,
        GIVar, NodeId,
        Closure, unClosure, mkClosure,
        toClosure, ToClosure(locToClosure),
        static, static_, StaticToClosure, staticToClosure,
        StaticDecl, declare, register, here)
import qualified Control.Parallel.HdpH as HdpH (declareStatic)
import Control.Parallel.HdpH.Strategies 
       (parDivideAndConquer, pushDivideAndConquer)
import qualified Control.Parallel.HdpH.Strategies as Strategies (declareStatic)


-----------------------------------------------------------------------------
-- 'Static' declaration

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
     declare $(static_ 'dnc_decompose),
     declare $(static_ 'dnc_combine),
     declare $(static_ 'dnc_f)]


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
      spark $(mkClosure [| dist_fib_abs (seqThreshold, parThreshold, n, gv) |])
      y <- dist_fib seqThreshold parThreshold (n - 2)
      clo_x <- get v
      force $ unClosure clo_x + y
  where k = max 1 seqThreshold
        l = parThreshold

dist_fib_abs :: (Int, Int, Int, GIVar (Closure Integer)) -> Par ()
dist_fib_abs (seqThreshold, parThreshold, n, gv) =
  dist_fib seqThreshold parThreshold (n - 1) >>=
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

dnc_trivial_abs :: (Int) -> (Closure Int -> Bool)
dnc_trivial_abs (seqThreshold) =
  \ clo_n -> unClosure clo_n <= max 1 seqThreshold

dnc_decompose =
  \ clo_n -> let n = unClosure clo_n in [toClosure (n-1), toClosure (n-2)]

dnc_combine =
  \ _ clos -> toClosure $ sum $ map unClosure clos

dnc_f =
  \ clo_n -> toClosure <$> (force $ fib $ unClosure clo_n)


-----------------------------------------------------------------------------
-- parallel Fibonacci; distributed memory; using pushing d-n-c skeleton

push_skel_fib :: [NodeId] -> Int -> Int -> Par Integer
push_skel_fib nodes seqThreshold n = unClosure <$> skel (toClosure n)
  where 
    skel = pushDivideAndConquer
             nodes
             $(mkClosure [| dnc_trivial_abs (seqThreshold) |])
             $(mkClosure [| dnc_decompose |])
             $(mkClosure [| dnc_combine |])
             $(mkClosure [| dnc_f |])


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
parseOpts :: [String] -> (RTSConf, Int, [String])
parseOpts args = go (defaultRTSConf, 0, args) where
  go :: (RTSConf, Int, [String]) -> (RTSConf, Int, [String])
  go (conf, seed, [])   = (conf, seed, [])
  go (conf, seed, s:ss) =
   case stripPrefix "-rand=" s of
   Just s  -> go (conf, read s, ss)
   Nothing ->
    case stripPrefix "-d" s of
    Just s  -> go (conf { debugLvl = read s }, seed, ss)
    Nothing ->
     case stripPrefix "-scheds=" s of
     Just s  -> go (conf { scheds = read s }, seed, ss)
     Nothing ->
      case stripPrefix "-wakeup=" s of
      Just s  -> go (conf { wakeupDly = read s }, seed, ss)
      Nothing ->
       case stripPrefix "-hops=" s of
       Just s  -> go (conf { maxHops = read s }, seed, ss)
       Nothing ->
        case stripPrefix "-maxFish=" s of
        Just s  -> go (conf { maxFish = read s }, seed, ss)
        Nothing ->
         case stripPrefix "-minSched=" s of
         Just s  -> go (conf { minSched = read s }, seed, ss)
         Nothing ->
          case stripPrefix "-minNoWork=" s of
          Just s  -> go (conf { minFishDly = read s }, seed, ss)
          Nothing ->
           case stripPrefix "-numProcs=" s of
           Just s  -> go (conf { numProcs = read s }, seed, ss)
           Nothing ->
            case stripPrefix "-maxNoWork=" s of
            Just s  -> go (conf { maxFishDly = read s }, seed, ss)
            Nothing ->
             (conf, seed, s:ss)


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
defVers         =  2 :: Int  -- version
defN            = 40 :: Int  -- Fibonacci argument
defParThreshold = 30 :: Int  -- shared-memory threshold
defSeqThreshold = 30 :: Int  -- sequential threshold


main :: IO ()
main = do
  hSetBuffering stdout LineBuffering
  hSetBuffering stderr LineBuffering
  register declareStatic
  opts_args <- getArgs
  let (conf, seed, args) = parseOpts opts_args
  let (version, n, seqThreshold, parThreshold) = parseArgs args
  initrand seed
  case version of
      0 -> do (x, t) <- timeIO $ evaluate
                          (fib n)
              putStrLn $
                "{v0} fib " ++ show n ++ " = " ++ show x ++
                " {runtime=" ++ show t ++ "}"
      1 -> do (output, t) <- timeIO $ evaluate =<< runParIO conf
                               (par_fib seqThreshold n)
              case output of
                Just x  -> putStrLn $
                             "{v1, " ++ 
                             "seqThreshold=" ++ show seqThreshold ++ "} " ++
                             "fib " ++ show n ++ " = " ++ show x ++
                             " {runtime=" ++ show t ++ "}"
                Nothing -> return ()
      2 -> do (output, t) <- timeIO $ evaluate =<< runParIO conf
                               (dist_fib seqThreshold parThreshold n)
              case output of
                Just x  -> putStrLn $
                             "{v2, " ++
                             "seqThreshold=" ++ show seqThreshold ++ ", " ++
                             "parThreshold=" ++ show parThreshold ++ "} " ++
                             "fib " ++ show n ++ " = " ++ show x ++
                             " {runtime=" ++ show t ++ "}"
                Nothing -> return ()
      3 -> do (output, t) <- timeIO $ evaluate =<< runParIO conf
                               (spark_skel_fib seqThreshold n)
              case output of
                Just x  -> putStrLn $
                             "{v3, " ++
                             "seqThreshold=" ++ show seqThreshold ++ "} " ++
                             "fib " ++ show n ++ " = " ++ show x ++
                             " {runtime=" ++ show t ++ "}"
                Nothing -> return ()
      4 -> do (output, t) <- timeIO $ evaluate =<< runParIO conf
                               (allNodes >>= \ nodes ->
                                push_skel_fib nodes seqThreshold n)
              case output of
                Just x  -> putStrLn $
                             "{v4, " ++
                             "seqThreshold=" ++ show seqThreshold ++ "} " ++
                             "fib " ++ show n ++ " = " ++ show x ++
                             " {runtime=" ++ show t ++ "}"
                Nothing -> return ()
      _ -> return ()
