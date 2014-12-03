-- Sum of totients in HdpH
--
-- Author: Patrick Maier
-----------------------------------------------------------------------------

{-# LANGUAGE FlexibleInstances #-}  -- req'd for some ToClosure instances
{-# LANGUAGE TemplateHaskell #-}    -- req'd for mkClosure, etc

{-# OPTIONS_GHC -fno-warn-orphans #-}

module Main where

import Prelude
import Control.Exception (evaluate)
import Control.Monad (when)
import Data.List (stripPrefix)
import Data.Functor ((<$>))
import Data.Monoid (mconcat)
import Data.Time.Clock (NominalDiffTime, diffUTCTime, getCurrentTime)
import System.Environment (getArgs)
import System.IO (stdout, stderr, hSetBuffering, BufferMode(..))
import System.Random (mkStdGen, setStdGen)

import Control.Parallel.HdpH
       (RTSConf(..), defaultRTSConf, updateConf,
        Par, runParIO,
        eval, time,
        Closure, unClosure, mkClosure,
        toClosure, ToClosure(locToClosure),
        static, StaticToClosure, staticToClosure,
        StaticDecl, declare, register, here)
import qualified Control.Parallel.HdpH as HdpH (declareStatic)
import Control.Parallel.HdpH.Dist (Dist, one)
import Control.Parallel.HdpH.Strategies
       (parMapMLocal, parMapM2Level, parMapM2LevelRelaxed)
import qualified Control.Parallel.HdpH.Strategies as Strategies (declareStatic)


-----------------------------------------------------------------------------
-- Euler's totient function (for positive integers)

totient :: Integer -> Integer
totient n = toInteger $ length $ filter (\ k -> gcd n k == 1) [1 .. n]


-----------------------------------------------------------------------------
-- sequential sum of totients

sum_totient :: Integer -> Integer -> Integer
sum_totient lower upper = sum $ map totient [lower .. upper]


sum_totient_seq :: (Integer, Integer) -> Par (Closure Integer)
sum_totient_seq (lower,upper) = toClosure <$> eval (sum_totient lower upper)


-----------------------------------------------------------------------------
-- parallel sum of totients; generic skeleton

type SkelT =  Dist
           -> Closure ((Integer,Integer) -> Par (Closure Integer))
           -> [Closure (Integer,Integer)]
           -> Par [Closure Integer]

-- skel: actual skeleton used
-- lower, upper: lower/upper bounds of input interval
-- tasks: total number of tasks
sum_totient_generic :: SkelT -> Integer -> Integer -> Integer
                    -> Par (Integer, Time)
sum_totient_generic skel lower upper tasks =
  time $ eval =<< sum . map unClosure <$> skel one clo_f intervals
    where
      clo_f = $(mkClosure [|sum_totient_seq|])
      chunksize = (upper - lower + 1) `cdiv` tasks
      intervals = map toClosure $ mkIntervals lower upper chunksize


-----------------------------------------------------------------------------
-- parallel sum of totients; multi-level skeletons

sum_totient_1level :: Integer -> Integer -> Integer -> Par (Integer, Time)
sum_totient_1level = sum_totient_generic parMapMLocal

sum_totient_2level :: Integer -> Integer -> Integer -> Par (Integer, Time)
sum_totient_2level = sum_totient_generic parMapM2Level

sum_totient_2level_relax :: Integer -> Integer -> Integer -> Par (Integer, Time)
sum_totient_2level_relax = sum_totient_generic parMapM2LevelRelaxed


-----------------------------------------------------------------------------
-- auxiliary functions

mkIntervals :: (Num a, Ord a) => a -> a -> a -> [(a, a)]
mkIntervals lower upper chunksize = go lower
  where
    go lb | lb > upper = []
          | otherwise  = (lb, ub) : go lb'
                           where
                             lb' = lb + chunksize
                             ub  = min upper (lb' - 1)


-- div combined with a ceiling (rather than rounding down)
cdiv :: (Integral a) => a -> a -> a
cdiv p q | r == 0    = k
         | otherwise = k + 1 where (k, r) = divMod p q


type Time = NominalDiffTime

-- time an IO action
timeIO :: IO a -> IO (a, Time)
timeIO action = do t0 <- getCurrentTime
                   x <- action
                   t1 <- getCurrentTime
                   return (x, diffUTCTime t1 t0)


-----------------------------------------------------------------------------
-- Static declaration (just before 'main')

-- Empty splice; TH hack to make all environment abstractions visible.
$(return [])

-- orphan ToClosure instances (unavoidably so)
instance ToClosure Integer where locToClosure = $(here)
instance ToClosure (Integer,Integer) where locToClosure = $(here)

declareStatic :: StaticDecl
declareStatic =
  mconcat
    [HdpH.declareStatic,         -- declare Static deserialisers
     Strategies.declareStatic,   -- from imported modules
     declare (staticToClosure :: StaticToClosure Integer),
     declare (staticToClosure :: StaticToClosure (Integer,Integer)),
     declare $(static 'sum_totient_seq)]


-----------------------------------------------------------------------------
-- initialisation, argument processing and 'main'

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
-- * lower bound for Euler's totient function
-- * upper bound for Euler's totient function
-- * number of sequential tasks
parseArgs :: [String] -> (Int, Integer, Integer, Integer)
parseArgs []     = (defVers, defLower, defUpper, defTasks)
parseArgs (s:ss) =
  let go :: Int -> [String] -> (Int, Integer, Integer, Integer)
      go v []           = (v, defLower, defUpper, defTasks)
      go v [s1]         = (v, defLower, read s1,  defTasks)
      go v [s1,s2]      = (v, read s1,  read s2,  defTasks)
      go v (s1:s2:s3:_) = (v, read s1,  read s2,  read s3)
  in case stripPrefix "v" s of
       Just s' -> go (read s') ss
       Nothing -> go defVers (s:ss)


defVers :: Int
defVers  = 1

defLower, defUpper, defTasks :: Integer
defLower =     1
defUpper = 51200
defTasks =   512


main :: IO ()
main = do
  hSetBuffering stdout LineBuffering
  hSetBuffering stderr LineBuffering
  register declareStatic
  opts_args <- getArgs
  (conf, seed, args) <- parseOpts opts_args
  let (version, lower, upper, gran_arg) = parseArgs args
  initrand seed
  -- Output using one tag per line to make it easy for grep and HdpHBencher.
  case version of
      0 -> do (x, t) <- timeIO $ evaluate (sum_totient lower upper)
              outputResults ["v0",show lower,show upper,show gran_arg,show x,"0",show t]
      1 -> do (output,t0) <- timeIO $ evaluate =<< runParIO conf
                               (sum_totient_1level lower upper gran_arg)
              case output of
                Just (x,t) ->
                  outputResults ["v1",show lower,show upper,show gran_arg,show x,show (t0 - t),show t]
                Nothing    -> return ()
      2 -> do (output,t0) <- timeIO $ evaluate =<< runParIO conf
                               (sum_totient_2level lower upper gran_arg)
              case output of
                Just (x,t) ->
                  outputResults ["v2",show lower,show upper,show gran_arg,show x,show (t0 - t),show t]
                Nothing    -> return ()
      3 -> do (output,t0) <- timeIO $ evaluate =<< runParIO conf
                               (sum_totient_2level_relax lower upper gran_arg)
              case output of
                Just (x,t) ->
                  outputResults ["v3",show lower,show upper,show gran_arg,show x,show (t0 - t),show t]
                Nothing    -> return ()
      _ -> return ()

outputResults :: [String] -> IO ()
outputResults [version, boundl, boundu, gran, output, overhead, runtime] =
  mapM_ printTags $ zip tags [version,boundl,boundu,gran,output,overhead,runtime]
    where tags = ["Version: ","LowerBound: ","UpperBound: ","TaskSize: ","Result: ","Overhead: ","Runtime: "]
          printTags (a,b) = putStrLn (a ++ b)
outputResults _ = putStrLn "Not enough arguments to output results"
