-- Testing latency and throughput with a ping/pong message exchange
--
-- Visibility: HdpH test suite
-- Author: Patrick Maier <P.Maier@hw.ac.uk>
-- Created: 15 Mar 2012
--
-----------------------------------------------------------------------------

{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE FlexibleInstances #-}  -- req'd for some ToClosure instances
{-# LANGUAGE TemplateHaskell #-}    -- req'd for mkClosure, etc
{-# LANGUAGE CPP #-}                -- for conditional compilation

module Main where

import Prelude
import Control.Applicative ((<$>))
import Control.Monad (when, unless)
import Control.DeepSeq (NFData, deepseq)
import Data.Bits (xor)
import Data.List (elemIndex, stripPrefix)
import Data.List (foldl')
import Data.Maybe (fromJust)
import Data.Time.Clock (NominalDiffTime, diffUTCTime, getCurrentTime)
import Data.Word (Word8)
import System.Environment (getArgs)
import System.IO (stdout, stderr, hSetBuffering, BufferMode(..))
import System.Posix.Env (getEnv)
import System.Random (Random(randomR, random, randoms),
                      mkStdGen, getStdGen, setStdGen)

import qualified MP.MPI_ByteString as MPI
import HdpH (RTSConf(..), defaultRTSConf,
             Par, runParIO,
             NodeId, GIVar,
             allNodes, io, force, new, get, put, glob, rput, fork, pushTo,
             Closure, mkClosure, static, toClosure, unClosure,
             StaticId, staticIdTD, register)


-----------------------------------------------------------------------------
-- 'Static' registration

instance StaticId [Word8]

registerStatic :: IO ()
registerStatic = do
  register $ staticIdTD (undefined :: [Word8])
  register $(static 'pong_abs)
  register $(static 'fork_par_fib_abs)


-----------------------------------------------------------------------------
-- ping/pong protocol

pingpong :: Int -> Int -> Int -> Int -> Int -> Par (Bool, NominalDiffTime)
pingpong size steps n k buffersize = do
  nodes <- allNodes
  let remote = cycle nodes !! 1
  io $ putStrLn $
    "ping " ++
    show size ++ " " ++ show steps ++ " " ++ show n ++ " " ++ show k ++
    " {remote=" ++ show remote ++ ", buffersize=" ++ show buffersize ++ "}"
  -- generate random closure of given 'size'
  bytes <- force =<< take size <$> randoms <$> io getStdGen :: Par [Word8]
  payload <- force $ toClosure bytes
  io $ putStrLn $
    "ping " ++ show size ++ " " ++ show steps ++
    " {checksum=" ++ show (foldl' xor 0 bytes) ++ "}"
  -- start Fibonacci computation on 'remote'
  pushTo $(mkClosure [| fork_par_fib_abs (k, n) |]) remote
  -- start Fibonacci computation on this node
  fork_par_fib k n
  -- start ping/pong protocol
  (reply, t) <- timePar $ iterateM' steps (ping remote) payload
  return (unClosure reply == bytes, t)

fork_par_fib_abs :: (Int, Int) -> Par ()
fork_par_fib_abs (k, n) = fork_par_fib k n


ping :: NodeId -> Closure [Word8] -> Par (Closure [Word8])
ping remote payload = do
  v <- new
  gv <- glob v
  pushTo $(mkClosure [| pong_abs (payload, gv) |]) remote
  reply <- get v
  force reply

pong_abs :: (Closure [Word8], GIVar (Closure [Word8])) -> Par ()
pong_abs (payload, gv) = pong payload gv


pong :: Closure [Word8] -> GIVar (Closure [Word8]) -> Par ()
pong payload gv = force payload >>= rput gv


-----------------------------------------------------------------------------
-- parallel Fibonacci to keep cores busy

fib :: Int -> Integer
fib n | n <= 1    = 1
      | otherwise = fib (n-1) + fib (n-2)


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


fork_par_fib :: Int -> Int -> Par ()
fork_par_fib seqThreshold n = fork $ par_fib seqThreshold n >> return ()


-----------------------------------------------------------------------------
-- initialisation, argument processing and 'main'

-- initialize random number generator
initrand :: Int -> IO ()
initrand seed = do
  when (seed /= 0) $ do
    ranks <- MPI.allRanks
    self <- MPI.myRank
    let i = fromJust $ elemIndex self ranks
    setStdGen (mkStdGen (seed + i))


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
           case stripPrefix "-maxNoWork=" s of
           Just s  -> go (conf { maxFishDly = read s }, seed, ss)
           Nothing ->
            (conf, seed, s:ss)


-- parse (optional) arguments in this order: 
-- * size of message in bytes, default 1024
-- * #ping/pong message pairs, default 1
-- * argument of Fibonacci function to be computed in parallel on each node
-- * sequential threshold for Fibonacci function
parseArgs :: [String] -> (Int, Int, Int, Int)
parseArgs []              = (defSize, defSteps, defFibN, defFibK)
parseArgs [s1]            = (read s1, defSteps, defFibN, defFibK)
parseArgs [s1,s2]         = (read s1, read s2,  defFibN, defFibK)
parseArgs [s1,s2,s3]      = (read s1, read s2,  read s3, defFibK)
parseArgs (s1:s2:s3:s4:_) = (read s1, read s2,  read s3, read s4)

defSize  = 1024 :: Int
defSteps =    1 :: Int
defFibN  =   50 :: Int
defFibK  =   30 :: Int


main :: IO ()
main = do
  hSetBuffering stdout LineBuffering
  hSetBuffering stderr LineBuffering
  registerStatic
  buffersize <- getEnvInt "BUFSIZE" MPI.defaultBufferSize
  MPI.withMPI buffersize $ do
    opts_args <- getArgs
    let (conf, seed, args) = parseOpts opts_args
    let (size, steps, n, k) = parseArgs args
    initrand seed
    output <- runParIO conf $ pingpong size steps n k buffersize
    case output of
      Just (ok, t) -> do putStrLn $
                           "ping " ++ show size ++ " " ++ show steps ++
                           " {runtime=" ++ show t ++ "}"
                         unless ok $ putStrLn $
                           "ping " ++ show size ++ " " ++ show steps ++
                           " {MESSAGE CORRUPTED}"
      Nothing      -> return ()


-----------------------------------------------------------------------------
-- auxiliary functions and instances

-- strict iteration
iterate' :: Int -> (a -> a) -> a -> a
iterate' n f x | n <= 0    = x
               | otherwise = let fx = f x in iterate' (n-1) f $! fx


-- strict iteration in a monad
iterateM' :: (Monad m) => Int -> (a -> m a) -> a -> m a
iterateM' n f x | n <= 0    = return x
                | otherwise = do { fx <- f x; iterateM' (n-1) f $! fx }


-- extracting an integer from an environment variable
getEnvInt :: String -> Int -> IO Int
getEnvInt var deflt = do
  s <- getEnv var
  case s of
    Nothing -> return deflt
    Just s  -> case reads s of
                 [(i,"")] -> return i
                 _        -> return deflt


-- measure the time taken for an action in the Par monad
timePar :: Par a -> Par (a, NominalDiffTime)
timePar action = do t0 <- io getCurrentTime
                    x <- action
                    t1 <- io getCurrentTime
                    return (x, diffUTCTime t1 t0)


#if __GLASGOW_HASKELL__ < 702
-- random bytes (instance already defined for GHC >= 7.2, at least)
instance Random Word8 where
  randomR (a, b) g = case randomR (toInteger a, toInteger b) g of
                       (i, g') -> (fromInteger i, g')
  random g = randomR (minBound, maxBound) g
#endif
