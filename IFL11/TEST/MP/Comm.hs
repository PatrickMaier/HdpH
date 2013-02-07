module Main where

import Prelude
import Control.Concurrent (threadDelay)
import Data.Functor ((<$>))
import System.Environment (getArgs)
import System.IO (stdout, stderr, hSetBuffering, BufferMode(..))

import HdpH.Internal.Misc (encodeLazy, decodeLazy, fork)
import HdpH.Internal.Location (NodeId, debug, dbgNone)
import HdpH.Internal.Comm (CommM)
import qualified HdpH.Internal.Comm_MPI as Comm
       (runWithMPI_, liftIO, nodes, allNodes, myNode, isMain,
        send, receive, shutdown, waitShutdown)


type Sender = NodeId
type RequestMessage = (Sender, Int)
type ReplyMessage = Int


server :: CommM ()
server = do
  -- dbg $ "server.0"
  request <- Comm.receive
  -- dbg $ "server.1"
  let (client, m) = decodeLazy request :: RequestMessage
  let n = m * m :: ReplyMessage
  let reply = encodeLazy n
  Comm.send client reply
  -- dbg $ "server.2"
  server


client :: NodeId -> [NodeId] -> Int -> CommM Int
client self servers m = sum <$> mapM client1 servers where
  client1 :: NodeId -> CommM Int
  client1 server = do
    -- dbg $ "client.0 <-> " ++ show server
    let request = encodeLazy ((self, m) :: RequestMessage)
    Comm.send server request
    -- dbg $ "client.1 <-> " ++ show server
    reply <- Comm.receive
    -- dbg $ "client.2 <-> " ++ show server
    let n = decodeLazy reply :: ReplyMessage
    return n


main :: IO ()
main = do
  hSetBuffering stdout LineBuffering
  hSetBuffering stderr LineBuffering
  Comm.runWithMPI_ $ do
    isMain <- Comm.isMain
    if isMain
       then do
         nodes    <- Comm.nodes
         allNodes <- Comm.allNodes
         myNode   <- Comm.myNode
         let servers = tail allNodes
         Comm.liftIO $ putStrLn $ "Servers: " ++ show servers
         Comm.liftIO $ putStrLn $ "Client: " ++ show myNode
         args <- Comm.liftIO $ getArgs
         let m = read (head args) :: Int  -- Client 1st arg must be integer
         Comm.liftIO $ putStrLn $ "m = " ++ show m
         n <- client myNode servers m
         Comm.liftIO $ putStrLn $ show (nodes - 1) ++ " * m^2 = " ++ show n
         Comm.shutdown
       else do
         fork $ server
         return ()
    Comm.waitShutdown


-- only use emergency
dbg :: String -> CommM ()
dbg = Comm.liftIO . debug dbgNone
