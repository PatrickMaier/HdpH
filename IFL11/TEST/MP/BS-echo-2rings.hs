module Main where

import Prelude
import Control.Applicative ((<$>))
import Control.Concurrent (forkIO)
import Control.Concurrent.MVar (newEmptyMVar, putMVar, takeMVar)
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


type AdjacentMap = MPI.Rank -> (MPI.Rank, MPI.Rank)


mainMPI :: Int -> IO ()
mainMPI bufferSize = do
  procs <- MPI.procs
  myRank <- MPI.myRank
  ranks <- MPI.allRanks
  let succ :: [MPI.Rank] -> MPI.Rank -> MPI.Rank
      succ ranks rank = head $ tail $ dropWhile (/= rank) $ cycle ranks
  let adj :: AdjacentMap
      adj rank = (succ (reverse ranks) rank, succ ranks rank)
  if procs < 3
     then putStrLn "TestMP-echo-2rings: need at least 3 processes"
     else if (myRank == MPI.rank0)
             then start_stop adj myRank bufferSize procs
             else relay adj myRank


start_stop :: AdjacentMap -> MPI.Rank -> Int -> Int -> IO ()
start_stop adj myRank bufferSize procs = do
  host <- getHostName
  let (prev,next) = adj myRank

  -- read arguments
  args <- getArgs
  let (n,seed) -- :: (Integer,Int)
               | length args >= 2 = (read (args!!0), read (args!!1))
               | length args == 1 = (read (args!!0), 0)   -- no seed
               | otherwise        = (1024 * 1024,    42)  -- fixed seed
  putStrLn $ "TestMP-echo-2rings" ++
             ": procs = " ++ show procs ++
             ", buf size = " ++ show bufferSize ++
             ", msg size = " ++ show (n :: Integer) ++
             ", seed = " ++ show seed

  -- seed random number gen and gen 2 random byte strings of length n
  when (seed /= 0) $ setStdGen (mkStdGen seed)
  msgUp <- genericTake n <$> randomBytes
  msgDn <- genericTake n <$> randomBytes
  let toCheck :: MPI.Rank -> ([Word8],Int)
      toCheck sender | sender == prev = (msgUp,1)
                     | sender == next = (msgDn,2)
                     | otherwise      = error "start_stop: unexpected sender"

  -- send msgs out
  putStrLn $ showRankAt myRank host ++ ": sending 1st message to " ++ show next
  MPI.send next =<< MPI.pack msgUp
  putStrLn $ showRankAt myRank host ++ ": sending 2nd message to " ++ show prev
  MPI.send prev =<< MPI.pack msgDn

  -- receive 1st echo
  hdl1 <- MPI.recv
  let src1 = MPI.sender hdl1
  putStrLn $ showRankAt myRank host ++ ": receiving 1st echo via " ++ show src1

  -- fork thread to actually receive echo
  sig1 <- newEmptyMVar
  forkIO (checkEcho (toCheck src1) hdl1 >> putMVar sig1 ())

  -- receive 2nd echo
  hdl2 <- MPI.recv
  let src2 = MPI.sender hdl2
  putStrLn $ showRankAt myRank host ++ ": receiving 2nd echo via " ++ show src2

  -- fork thread to actually receive echo
  sig2 <- newEmptyMVar
  forkIO (checkEcho (toCheck src2) hdl2 >> putMVar sig2 ())

  -- wait for both receiver threads to finish
  takeMVar sig1
  takeMVar sig2

checkEcho :: ([Word8],Int) -> MPI.MsgHandle -> IO ()
checkEcho (msg,i) hdl = do
  -- read echo
  echo <- MPI.unpack =<< MPI.getMsg hdl

  -- test and print echo integrity
  case diffAt msg echo of
    Nothing -> putStrLn $ "TestMP-echo-2rings: echo == msg" ++ show i
    Just n  -> putStrLn $ "TestMP-echo-2rings: echo/msg " ++ show i ++
                          " diff after " ++ show n ++ " bytes"


relay :: AdjacentMap -> MPI.Rank -> IO ()
relay adj myRank = do
  host <- getHostName
  let (prev,next) = adj myRank
  let relayTo :: MPI.Rank -> MPI.Rank
      relayTo sender | sender == prev = next
                     | sender == next = prev
                     | otherwise      = error "relay: unexpected sender"

  -- receive first message
  hdl1 <- MPI.recv
  let dest1 = relayTo (MPI.sender hdl1)
  putStrLn $ showRankAt myRank host ++ ": 1st relaying to " ++ show dest1

  -- fork thread to actually relay
  sig1 <- newEmptyMVar
  forkIO (MPI.getMsg hdl1 >>=  MPI.send dest1 >> putMVar sig1 ())

  -- receive second message
  hdl2 <- MPI.recv
  let dest2 = relayTo (MPI.sender hdl2)
  putStrLn $ showRankAt myRank host ++ ": 2nd relaying to " ++ show dest2

  -- fork thread to actually relay
  sig2 <- newEmptyMVar
  forkIO (MPI.getMsg hdl2 >>=  MPI.send dest2 >> putMVar sig2 ())

  -- wait for both relay threads to finish
  takeMVar sig1
  takeMVar sig2


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
