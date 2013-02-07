module Main where

import Prelude
import Control.Applicative ((<$>))
import Control.Concurrent (forkIO)
import Control.Concurrent.MVar
         (MVar, newMVar, newEmptyMVar, takeMVar, putMVar, modifyMVar_)
import Control.Monad (when, unless)
import qualified Data.ByteString.Lazy as Lazy (ByteString, length)
import Data.List (findIndex, genericTake)
import Data.Maybe (fromJust)
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
  host <- getHostName
  let getPos rank = fromJust $ findIndex (== rank) ranks
  let serverRank = head ranks
  putStrLn $ "Starting " ++ showRankAt myRank host ++ 
             ": serverRank = " ++ show serverRank ++
             ", bufferSize = " ++ show bufferSize
  when (myRank /= serverRank) $ client myRank (getPos myRank) serverRank
  when (myRank == serverRank) $ server myRank (procs - 1)


client :: MPI.Rank -> Int -> MPI.Rank -> IO ()
client myRank myPos serverRank = do
  -- read arguments
  args <- getArgs
  let (n,seed) -- :: (Integer,Int)
               | length args >= 2 = (read (args!!0), read (args!!1) + myPos)
               | length args == 1 = (read (args!!0), 0)           -- no seed
               | otherwise        = (1024 * 1024,    42 + myPos)  -- fixed seed
  putStrLn $ "Client " ++ show myRank ++ ": n = " ++ show (n :: Integer) ++
                                         ", seed = " ++ show seed

  -- seed random number gen
  when (seed /= 0) $ setStdGen (mkStdGen seed)

  -- send random byte string of length n
  msg <- MPI.pack =<< (genericTake n <$> randomBytes)
  MPI.send serverRank msg

  -- receive reply
  hdl <- MPI.recv
  echo <- MPI.getMsg hdl

  -- check sender identity and echo integrity
  let senderOK = MPI.sender hdl == serverRank
  let echoOK = echo == msg
  let echoLen = Lazy.length echo

  -- print test results
  putStrLn $ "Client " ++ show myRank ++ ": senderOK = " ++ show senderOK
  putStrLn $ "Client " ++ show myRank ++ ": echoOK = " ++ show echoOK
  unless echoOK $
    putStrLn $ "Client " ++ show myRank ++ ": echoLen = " ++ show echoLen


server :: MPI.Rank -> Int -> IO ()
server myRank clients = do
  putStrLn $ "Server " ++ show myRank ++ ": clients = " ++ show clients
  handlerCtr   <- newMVar clients
  handlersDone <- newEmptyMVar
  when (clients > 0) $ do
    server_loop clients handlerCtr handlersDone
    -- wait for all handlers to finish
    takeMVar handlersDone
  putStrLn "Server: all handlers finished"

server_loop :: Int -> MVar Int -> MVar () -> IO ()
server_loop 0 _          _            = putStrLn "Server: all handlers forked"
server_loop n handlerCtr handlersDone = do
  hdl <- MPI.recv
  forkIO $ message_handler hdl handlerCtr handlersDone
  server_loop (n - 1) handlerCtr handlersDone

message_handler :: MPI.MsgHandle -> MVar Int -> MVar () -> IO ()
message_handler hdl handlerCtr handlersDone = do
  let clientRank = MPI.sender hdl
  putStrLn $ "Server: receiving from client " ++ show clientRank
  msg <- MPI.getMsg hdl
  putStrLn $ "Server: responding to client " ++ show clientRank
  MPI.send clientRank msg
  putStrLn $ "Server: finished client " ++ show clientRank
  decHandlerCtr handlerCtr handlersDone

decHandlerCtr :: MVar Int -> MVar () -> IO ()
decHandlerCtr handlerCtr handlersDone = do
  modifyMVar_ handlerCtr $ \ n ->
    if n == 1
       then putMVar handlersDone () >> return 0
       else return (n - 1)


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
