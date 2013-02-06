-- Sum of totients in HdpH
--
-- Author: Patrick Maier
-----------------------------------------------------------------------------

{-# LANGUAGE FlexibleInstances #-}  -- req'd for some ToClosure instances
{-# LANGUAGE TemplateHaskell #-}    -- req'd for mkClosure, etc

module Main where

import Prelude
import Control.Exception (evaluate)
import Control.Monad (when, (<=<))
import Data.List (elemIndex, stripPrefix)
import Data.Functor ((<$>))
import Data.List (transpose)
import Data.Maybe (fromJust)
import Data.Monoid (mconcat)
import Data.Time.Clock (NominalDiffTime, diffUTCTime, getCurrentTime)
import System.Environment (getArgs)
import System.IO (stdout, stderr, hSetBuffering, BufferMode(..))
import System.Random (mkStdGen, setStdGen)

import Control.Parallel.HdpH 
       (RTSConf(..), defaultRTSConf,
        Par, runParIO,
        force, fork, spark, new, get, put, glob, rput,
        IVar, GIVar,
        Closure, unClosure, mkClosure,
        toClosure, ToClosure(locToClosure),
        static, static_, StaticToClosure, staticToClosure,
        StaticDecl, declare, register, here)
import qualified Control.Parallel.HdpH as HdpH (declareStatic)
import Control.Parallel.HdpH.Strategies 
       (Strategy, ForceCC(locForceCC),
        parMapNF, parMapChunkedNF, parMapSlicedNF,
        StaticForceCC, staticForceCC)
import qualified Control.Parallel.HdpH.Strategies as Strategies (declareStatic)


-----------------------------------------------------------------------------
-- Static declaration

instance ToClosure Int where locToClosure = $(here)
instance ToClosure [Int] where locToClosure = $(here)
instance ToClosure Integer where locToClosure = $(here)
instance ForceCC Integer where locForceCC = $(here)

declareStatic :: StaticDecl
declareStatic =
  mconcat
    [HdpH.declareStatic,         -- declare Static deserialisers
     Strategies.declareStatic,   -- from imported modules
     declare (staticToClosure :: StaticToClosure Int),
     declare (staticToClosure :: StaticToClosure [Int]),
     declare (staticToClosure :: StaticToClosure Integer),
     declare (staticForceCC :: StaticForceCC Integer),
     declare $(static 'spark_sum_euler_abs),
     declare $(static_ 'sum_totient),
     declare $(static_ 'totient)]


-----------------------------------------------------------------------------
-- Euler's totient function (for positive integers)

totient :: Int -> Integer
totient n = toInteger $ length $ filter (\ k -> gcd n k == 1) [1 .. n]


-----------------------------------------------------------------------------
-- sequential sum of totients

sum_totient :: [Int] -> Integer
sum_totient = sum . map totient


-----------------------------------------------------------------------------
-- parallel sum of totients; shared memory

par_sum_totient_chunked :: Int -> Int -> Int -> Par Integer
par_sum_totient_chunked lower upper chunksize =
  sum <$> (mapM get =<< (mapM fork_sum_euler $ chunked_list))
    where
      chunked_list = chunk chunksize [upper, upper - 1 .. lower] :: [[Int]]


par_sum_totient_sliced :: Int -> Int -> Int -> Par Integer
par_sum_totient_sliced lower upper slices =
  sum <$> (mapM get =<< (mapM fork_sum_euler $ sliced_list))
    where
      sliced_list = slice slices [upper, upper - 1 .. lower] :: [[Int]]


fork_sum_euler :: [Int] -> Par (IVar Integer)
fork_sum_euler xs = do v <- new
                       fork $ force (sum_totient xs) >>= put v
                       return v


-----------------------------------------------------------------------------
-- parallel sum of totients; distributed memory

dist_sum_totient_chunked :: Int -> Int -> Int -> Par Integer
dist_sum_totient_chunked lower upper chunksize = do
  sum <$> (mapM get_and_unClosure =<< (mapM spark_sum_euler $ chunked_list))
    where
      chunked_list = chunk chunksize [upper, upper - 1 .. lower] :: [[Int]]


dist_sum_totient_sliced :: Int -> Int -> Int -> Par Integer
dist_sum_totient_sliced lower upper slices = do
  sum <$> (mapM get_and_unClosure =<< (mapM spark_sum_euler $ sliced_list))
    where
      sliced_list = slice slices [upper, upper - 1 .. lower] :: [[Int]]


spark_sum_euler :: [Int] -> Par (IVar (Closure Integer))
spark_sum_euler xs = do 
  v <- new
  gv <- glob v
  spark $(mkClosure [| spark_sum_euler_abs (xs, gv) |])
  return v

spark_sum_euler_abs :: ([Int], GIVar (Closure Integer)) -> Par ()
spark_sum_euler_abs (xs, gv) =
  force (sum_totient xs) >>= rput gv . toClosure

get_and_unClosure :: IVar (Closure a) -> Par a
get_and_unClosure = return . unClosure <=< get

-----------------------------------------------------------------------------
-- parallel sum of totients; distributed memory (using plain task farm)

farm_sum_totient_chunked :: Int -> Int -> Int -> Par Integer
farm_sum_totient_chunked lower upper chunksize =
  sum <$> parMapNF $(mkClosure [| sum_totient |]) chunked_list
    where
      chunked_list = chunk chunksize [upper, upper - 1 .. lower] :: [[Int]]


farm_sum_totient_sliced :: Int -> Int -> Int -> Par Integer
farm_sum_totient_sliced lower upper slices =
  sum <$> parMapNF $(mkClosure [| sum_totient |]) sliced_list
    where
      sliced_list = slice slices [upper, upper - 1 .. lower] :: [[Int]]


-----------------------------------------------------------------------------
-- parallel sum of totients; distributed memory (chunking/slicing task farms)

chunkfarm_sum_totient :: Int -> Int -> Int -> Par Integer
chunkfarm_sum_totient lower upper chunksize =
  sum <$> parMapChunkedNF chunksize $(mkClosure [| totient |]) list
    where
      list = [upper, upper - 1 .. lower] :: [Int]


slicefarm_sum_totient :: Int -> Int -> Int -> Par Integer
slicefarm_sum_totient lower upper slices =
  sum <$> parMapSlicedNF slices $(mkClosure [| totient |]) list
    where
      list = [upper, upper - 1 .. lower] :: [Int]


-----------------------------------------------------------------------------
-- chunking up lists; inverse of 'chunk n' is 'concat'

chunk :: Int -> [a] -> [[a]]
chunk n [] = []
chunk n xs = ys : chunk n zs where (ys,zs) = splitAt n xs


-----------------------------------------------------------------------------
-- slicing lists; inverse of 'slice n' is 'unslice'

slice :: Int -> [a] -> [[a]]
slice n = transpose . chunk n

unslice :: [[a]] -> [a]
unslice = concat . transpose


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
-- * lower bound for Euler's totient function
-- * upper bound for Euler's totient function
-- * size of chunks (evaluated sequentially)
parseArgs :: [String] -> (Int, Int, Int, Int)
parseArgs []     = (defVers, defLower, defUpper, defChunk)
parseArgs (s:ss) =
  let go :: Int -> [String] -> (Int, Int, Int, Int)
      go v []           = (v, defLower, defUpper, defChunk)
      go v [s1]         = (v, defLower, read s1,  defChunk)
      go v [s1,s2]      = (v, read s1,  read s2,  defChunk)
      go v (s1:s2:s3:_) = (v, read s1,  read s2,  read s3)
  in case stripPrefix "v" s of
       Just s' -> go (read s') ss
       Nothing -> go defVers (s:ss)


defVers  =     7 :: Int
defLower =     1 :: Int
defUpper = 20000 :: Int
defChunk =   100 :: Int


main :: IO ()
main = do
  hSetBuffering stdout LineBuffering
  hSetBuffering stderr LineBuffering
  register declareStatic
  opts_args <- getArgs
  let (conf, seed, args) = parseOpts opts_args
  let (version, lower, upper, gran_arg) = parseArgs args
  initrand seed
  case version of
      0 -> do (x, t) <- timeIO $ evaluate
                          (sum_totient [upper, upper - 1 .. lower])
              putStrLn $
                "{v0} sum $ map totient [" ++ show lower ++ ".." ++
                show upper ++ "] = " ++ show x ++
                " {runtime=" ++ show t ++ "}"
      1 -> do (output, t) <- timeIO $ evaluate =<< runParIO conf
                               (par_sum_totient_chunked lower upper gran_arg)
              case output of
                Just x  -> putStrLn $
                             "{v1, chunksize=" ++ show gran_arg ++ "} " ++
                             "sum $ map totient [" ++ show lower ++ ".." ++
                             show upper ++ "] = " ++ show x ++
                             " {runtime=" ++ show t ++ "}"
                Nothing -> return ()
      2 -> do (output, t) <- timeIO $ evaluate =<< runParIO conf
                               (dist_sum_totient_chunked lower upper gran_arg)
              case output of
                Just x  -> putStrLn $
                             "{v2, chunksize=" ++ show gran_arg ++ "} " ++
                             "sum $ map totient [" ++ show lower ++ ".." ++
                             show upper ++ "] = " ++ show x ++
                             " {runtime=" ++ show t ++ "}"
                Nothing -> return ()
      3 -> do (output, t) <- timeIO $ evaluate =<< runParIO conf
                               (farm_sum_totient_chunked lower upper gran_arg)
              case output of
                Just x  -> putStrLn $
                             "{v3, chunksize=" ++ show gran_arg ++ "} " ++
                             "sum $ map totient [" ++ show lower ++ ".." ++
                             show upper ++ "] = " ++ show x ++
                             " {runtime=" ++ show t ++ "}"
                Nothing -> return ()
      4 -> do (output, t) <- timeIO $ evaluate =<< runParIO conf
                               (chunkfarm_sum_totient lower upper gran_arg)
              case output of
                Just x  -> putStrLn $
                             "{v4, chunksize=" ++ show gran_arg ++ "} " ++
                             "sum $ map totient [" ++ show lower ++ ".." ++
                             show upper ++ "] = " ++ show x ++
                             " {runtime=" ++ show t ++ "}"
                Nothing -> return ()
      5 -> do (output, t) <- timeIO $ evaluate =<< runParIO conf
                               (par_sum_totient_sliced lower upper gran_arg)
              case output of
                Just x  -> putStrLn $
                             "{v5, slices=" ++ show gran_arg ++ "} " ++
                             "sum $ map totient [" ++ show lower ++ ".." ++
                             show upper ++ "] = " ++ show x ++
                             " {runtime=" ++ show t ++ "}"
                Nothing -> return ()
      6 -> do (output, t) <- timeIO $ evaluate =<< runParIO conf
                               (dist_sum_totient_sliced lower upper gran_arg)
              case output of
                Just x  -> putStrLn $
                             "{v6, slices=" ++ show gran_arg ++ "} " ++
                             "sum $ map totient [" ++ show lower ++ ".." ++
                             show upper ++ "] = " ++ show x ++
                             " {runtime=" ++ show t ++ "}"
                Nothing -> return ()
      7 -> do (output, t) <- timeIO $ evaluate =<< runParIO conf
                               (farm_sum_totient_sliced lower upper gran_arg)
              case output of
                Just x  -> putStrLn $
                             "{v7, slices=" ++ show gran_arg ++ "} " ++
                             "sum $ map totient [" ++ show lower ++ ".." ++
                             show upper ++ "] = " ++ show x ++
                             " {runtime=" ++ show t ++ "}"
                Nothing -> return ()
      8 -> do (output, t) <- timeIO $ evaluate =<< runParIO conf
                               (slicefarm_sum_totient lower upper gran_arg)
              case output of
                Just x  -> putStrLn $
                             "{v8, slices=" ++ show gran_arg ++ "} " ++
                             "sum $ map totient [" ++ show lower ++ ".." ++
                             show upper ++ "] = " ++ show x ++
                             " {runtime=" ++ show t ++ "}"
                Nothing -> return ()

      _ -> return ()
