module Main where

import Prelude
import Control.Applicative ((<$>))
import Control.Monad (when)
import qualified Data.ByteString.Lazy as Lazy (ByteString)
import Data.List (genericTake)
import Data.Word (Word8)
import Network.BSD (HostName, getHostName)
import System.Environment (getArgs)
import System.IO (stdout, stderr, hSetBuffering, BufferMode(..))
import System.Posix.Env (getEnv)
import System.Random (mkStdGen, setStdGen, randomRIO)
import qualified MP.MPI_ByteString as MPI


main :: IO ()
main = do
  hSetBuffering stdout LineBuffering
  hSetBuffering stderr LineBuffering
  -- get buffer size from env var BUFSIZE (can't use getArgs before withMPI)
  bufferSize <- getEnvInt "BUFSIZE" MPI.defaultBufferSize
  MPI.withMPI bufferSize (mainMPI bufferSize)


mainMPI :: Int -> IO ()
mainMPI bufferSize = do
  procs <- MPI.procs
  myRank <- MPI.myRank
  ranks <- MPI.allRanks
  let next :: MPI.Rank -> MPI.Rank
      next rank = head $ tail $ dropWhile (/= rank) $ cycle ranks
  if (myRank == MPI.rank0)
     then start_stop next myRank bufferSize procs
     else relay next myRank


start_stop :: (MPI.Rank -> MPI.Rank) -> MPI.Rank -> Int -> Int -> IO ()
start_stop next myRank bufferSize procs = do
  host <- getHostName

  -- read arguments
  args <- getArgs
  let (n,seed) -- :: (Integer,Int)
               | length args >= 2 = (read (args!!0), read (args!!1))
               | length args == 1 = (read (args!!0), 0)   -- no seed
               | otherwise        = (1024 * 1024,    42)  -- fixed seed
  putStrLn $ "TestMP-echo-ring" ++
             ": procs = " ++ show procs ++
             ", buf size = " ++ show bufferSize ++
             ", msg size = " ++ show (n :: Integer) ++
             ", seed = " ++ show seed

  -- seed random number gen and gen random byte string of length n
  when (seed /= 0) $ setStdGen (mkStdGen seed)
  msg <- genericTake n <$> randomBytes
  
  -- send msg out
  putStrLn $ showRankAt myRank host ++ ": sending message to " ++
             show (next myRank)
  MPI.send (next myRank) =<< MPI.pack msg

  -- receive echo
  hdl <- MPI.recv
  putStrLn $ showRankAt myRank host ++ ": receiving echo"
  echo <- MPI.unpack =<< MPI.getMsg hdl

  -- test and print echo integrity
  case diffAt msg echo of
    Nothing -> putStrLn $ "TestMP-echo-ring: echo == msg"
    Just n  -> putStrLn $ "TestMP-echo-ring: echo/msg diff after " ++ 
                          show n ++ " bytes"


relay :: (MPI.Rank -> MPI.Rank) -> MPI.Rank -> IO ()
relay next myRank = do
  host <- getHostName

  -- receive message
  hdl <- MPI.recv
  putStrLn $ showRankAt myRank host ++ ": relaying to " ++ show (next myRank)
  msg <- MPI.getMsg hdl

  -- relay to next process
  MPI.send (next myRank) msg


diffAt :: Eq a => [a] -> [a] -> Maybe Integer
diffAt xs ys = go_compare xs ys 0 where
  go_compare []     []     _ = Nothing
  go_compare []     _      n = Just n
  go_compare _      []     n = Just n
  go_compare (x:xs) (y:ys) n | x == y    = go_compare xs ys $! (n + 1)
                             | otherwise = Just n

randomBytes :: IO [Word8]
randomBytes = build 257 [] where  -- 257 is a nice prime
  build 0 bytes = return (cycle bytes)
  build n bytes = do b <- randomByte
                     build (n - 1) (b:bytes)

randomByte :: IO Word8
randomByte = fromIntegral <$> randomRIO (zero, twofivefive) where
                zero        = fromIntegral (minBound :: Word8) :: Int
                twofivefive = fromIntegral (maxBound :: Word8) :: Int

-- extracting an integer from an environment variable
getEnvInt :: String -> Int -> IO Int
getEnvInt var deflt = do
  s <- getEnv var
  case s of
    Nothing -> return deflt
    Just s  -> case reads s of
                 [(i,"")] -> return i
                 _        -> return deflt

showRankAt :: MPI.Rank -> HostName -> String
showRankAt rank host = shows rank $ showString ("@" ++ host) $ ""
