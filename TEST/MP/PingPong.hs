-- Testing latency and throughput with a ping/pong message exchange;
-- potentially while under load
--
-- Visibility: HdpH test suite
-- Author: Patrick Maier <P.Maier@hw.ac.uk>
-- Created: 11 May 2012
--
-----------------------------------------------------------------------------

{-# LANGUAGE FlexibleInstances #-}  -- req'd for some ToClosure instances
{-# LANGUAGE TemplateHaskell #-}    -- req'd for mkClosure, etc
{-# LANGUAGE CPP #-}                -- for conditional compilation

module Main where

import Prelude
import Control.Applicative ((<$>))
import Control.Concurrent (forkIO)
import Control.Exception (evaluate)
import Control.Monad (when, unless, replicateM_)
import Data.Bits (xor)
import Data.ByteString.Lazy (ByteString, foldl')
import Data.List (elemIndex, stripPrefix)
import Data.Maybe (fromJust)
import Data.Serialize (encodeLazy, decodeLazy)
import Data.Time.Clock (NominalDiffTime, diffUTCTime, getCurrentTime)
import Data.Word (Word8)
import System.Environment (getArgs)
import System.IO (stdout, stderr, hSetBuffering, BufferMode(..))
import System.Posix.Env (getEnv)
import System.Random
       (Random(randomR, random, randoms), mkStdGen, getStdGen, setStdGen)

import qualified MP.MPI_ByteString as MPI


-----------------------------------------------------------------------------
-- ping/pong protocol

pings :: Int -> Int -> Int -> IO (Bool, NominalDiffTime)
pings size steps buffersize = do
    ranks <- MPI.allRanks
    let remote = cycle ranks !! 1
    putStrLn $
      "PingPong " ++
      show size ++ " " ++ show steps ++
      " {remote=" ++ show remote ++ ", buffersize=" ++ show buffersize ++ "}"
    -- generate random bytestring of given 'size'
    payload <- MPI.pack =<< take size <$> randoms <$> getStdGen
    putStrLn $
      "PingPong " ++ show size ++ " " ++ show steps ++
      " {checksum=" ++ show (chksm payload) ++ "}"
    -- first ping/pong exchange to establish connection to pong server
    let payload0 = encodeLazy steps
    ping remote payload0
    -- time ping/pong exchanges
    putStrLn $
      "PingPong " ++ show size ++ " " ++ show steps ++
      " {starting ...}"
    (reply, t) <- timeIO $ iterateM' steps (ping remote) payload
    putStrLn $
      "PingPong " ++ show size ++ " " ++ show steps ++
      " {... finished}"
    -- return whether messages were transmitted correctly and duration
    return (reply == payload, t)


ping :: MPI.Rank -> MPI.Msg -> IO MPI.Msg
ping remote payload = do
  MPI.send remote payload
  reply <- MPI.getMsg =<< MPI.recv
  evaluate (chksm reply)
  return reply


pong :: MPI.Rank -> IO ()
pong sender = do
  payload <- MPI.getMsg =<< MPI.recv
  evaluate (chksm payload)
  MPI.send sender payload


pongServer :: IO ()
pongServer = do
  hdl <- MPI.recv
  payload0 <- MPI.getMsg hdl
  let sender = MPI.sender hdl
  let steps = fromRight $ decodeLazy payload0
  MPI.send sender payload0
  replicateM_ steps (pong sender)


-----------------------------------------------------------------------------
-- Fibonacci and Sum-of-Totients to keep cores busy

fib :: Integer -> Integer
fib n | n <= 1    = 1
      | otherwise = fib (n-1) + fib (n-2)

totient :: Integer -> Integer
totient n = toInteger $ length $ filter (\ k -> gcd n k == 1) [1 .. n]

totients :: Integer -> Integer -> [Integer]
totients m n = map totient [m .. n]
{-# NOINLINE totients #-}

sumTotient :: Integer -> Integer -> Integer
sumTotient m n = sum $ totients m n


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


-- parse optional seed for random number generator
parseOpts :: [String] -> (Int, [String])
parseOpts args = go (0, args) where
  go :: (Int, [String]) -> (Int, [String])
  go (seed, [])   = (seed, [])
  go (seed, s:ss) =
    case stripPrefix "-rand=" s of
    Just s  -> go (read s, ss)
    Nothing -> (seed, s:ss)


-- parse (optional) arguments in this order: 
-- * size of message in bytes, default 1024
-- * #ping/pong message pairs, default 1
-- * argument of Fibonacci function to be computed sequentially by sender
-- * #threads computing Fibonacci by receiver
-- * argument of Fibonacci function to be computed sequentially by receiver
-- * #threads computing Fibonacci on receiver
parseArgs :: [String] -> (Int, Int, Integer, Int, Integer, Int)
parseArgs []                    = (defSize, defSteps, defFibS, defThrS, defFibR, defThrR)
parseArgs [s1]                  = (read s1, defSteps, defFibS, defThrS, defFibR, defThrR)
parseArgs [s1,s2]               = (read s1, read s2,  defFibS, defThrS, defFibR, defThrR)
parseArgs [s1,s2,s3]            = (read s1, read s2,  read s3, defThrS, read s3, defThrR)
parseArgs [s1,s2,s3,s4]         = (read s1, read s2,  read s3, read s4, read s3, read s4)
parseArgs [s1,s2,s3,s4,s5]      = (read s1, read s2,  read s3, read s4, read s5, read s4)
parseArgs (s1:s2:s3:s4:s5:s6:_) = (read s1, read s2,  read s3, read s4, read s5, read s6)

defSize  = 1024 :: Int
defSteps =    1 :: Int
defFibS  = 5000 :: Integer
defThrS  =    1 :: Int
defFibR  = 5000 :: Integer
defThrR  =    1 :: Int


main :: IO ()
main = do
  hSetBuffering stdout LineBuffering
  hSetBuffering stderr LineBuffering
  buffersize <- getEnvInt "BUFSIZE" MPI.defaultBufferSize
  MPI.withMPI buffersize $ do
    -- parse args
    opt_args <- getArgs
    let (seed, args) = parseOpts opt_args
    let (size, steps, fs, ts, fr, tr) = parseArgs args
    initrand seed
    myRank <- MPI.myRank
    if myRank /= MPI.rank0
      then do -- non-main rank: fork work threads then start pong server
        mapM_ 
          (\ m -> forkIO $ putStrLn $
                    "PingPong sumTotient/receiver [" ++
                    show m ++ ".." ++ show fr ++ "] = " ++
                    show (sumTotient m fr))
          [1 .. toInteger tr]
        pongServer
      else do -- main rank: fork work threads, ping repeatedly, print results
        mapM_ 
          (\ m -> forkIO $ putStrLn $
                    "PingPong sumTotient/sender [" ++
                    show m ++ ".." ++ show fs ++ "] = " ++
                    show (sumTotient m fs))
          [1 .. toInteger ts]
        (ok, t) <- pings size steps buffersize
        putStrLn $
          "PingPong " ++ show size ++ " " ++ show steps ++
          " {sumTotient/sender " ++ show ts ++ "x" ++ show fs ++
          ", sumTotient/receiver" ++ show tr ++ "x" ++ show fr ++
          ", runtime=" ++ show t ++ "}"
        unless ok $ putStrLn $
          "PingPong " ++ show size ++ " " ++ show steps ++
          " {MESSAGE CORRUPTED}"


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


-- measure the time taken for an action in the IO monad
timeIO :: IO a -> IO (a, NominalDiffTime)
timeIO action = do t0 <- getCurrentTime
                   x <- action
                   t1 <- getCurrentTime
                   return (x, diffUTCTime t1 t0)


-- check summing a lazy bytestring (thereby forcing evaluation)
chksm :: ByteString -> Word8
chksm = foldl' xor 0


fromRight :: Either a b -> b
fromRight (Right b) = b
fromRight _         = error "PingPong.fromRight"


#if __GLASGOW_HASKELL__ < 702
-- random bytes (instance already defined for GHC >= 7.2, at least)
instance Random Word8 where
  randomR (a, b) g = case randomR (toInteger a, toInteger b) g of
                       (i, g') -> (fromInteger i, g')
  random g = randomR (minBound, maxBound) g
#endif
