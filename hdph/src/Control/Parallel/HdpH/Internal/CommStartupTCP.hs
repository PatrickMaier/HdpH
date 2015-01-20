{-# LANGUAGE OverloadedStrings #-}

-- Startup sync via a known master host/port and TCP connections.
--
-- Author: Blair Archibald
-----------------------------------------------------------------

module Control.Parallel.HdpH.Internal.CommStartupTCP
  ( startupTCP ) where

import Control.Monad (forM_, replicateM)
import Control.Concurrent (threadDelay)

import Data.ByteString (ByteString, hGetLine)
import Data.ByteString.Char8 (hPutStrLn)

import Network.Socket
import Network.BSD

import System.IO.Error
import System.IO (IOMode(..), Handle, hClose)
import System.Timeout (timeout)

import Control.Parallel.HdpH.Conf (RTSConf(..))

-- | Use the TCP startupBackend to discover all nodes in the system. Should be
-- provided with a RTSConf with the startupHost and startupPort parameters
-- set. The Bytestring should be the encoding of the calling node.
startupTCP :: RTSConf -> ByteString -> IO [ByteString]
startupTCP conf meEnc = let valid = validateConf in
                        if valid then startupTCP' conf meEnc else reportError
  where validateConf   = not (startupHost conf == "" || startupPort conf == "")
        reportError    = moduleError "startupTCP"
                                     "You must specify the options -\
                                     \ \"startupHost\" and \"startupPort\" to\
                                     \ use the TCP startup backend."

-- Internal main startupTCP method
startupTCP' :: RTSConf -> ByteString -> IO [ByteString]
startupTCP' conf nodeEnc = do hn <- getHostName
                              if hn /= startupHost conf
                               then sendDetails
                               else handleStartupRootNode

  where
    handleStartupRootNode :: IO [ByteString]
    handleStartupRootNode = withSocketsDo $ do
      masterSock <- tryBind
      case masterSock of
        Nothing -> sendDetails
        Just s  -> rootProcStartup s

    -- TODO: Should check all the addresses returned by getAddrInfo
    tryBind :: IO (Maybe Socket)
    tryBind = do (hostAddr:_) <- getAddrInfo
                                (Just (defaultHints {addrFamily = AF_INET}))
                                (Just (startupHost conf))
                                (Just (startupPort conf))
                 s <- socket AF_INET Stream defaultProtocol
                 (bindSocket s (addrAddress hostAddr) >> return (Just s))
                                                         `catchIOError`
                                                         (\_ -> return Nothing)

    -- Start the root node listening,
    -- wait to receive 'numProc' bytestrings (or timeout),
    --  then broadcast the universe to all nodes
    rootProcStartup :: Socket -> IO [ByteString]
    rootProcStartup s = do
      let numproc = numProcs conf
      listen s numproc
      universe <- recvNByteStrings conf s numproc nodeEnc
      case universe of
        Nothing  -> moduleError "rootProcStartup"
                                "Failed to recieve messages from all nodes."
        Just uni -> broadcastUniverse uni >> return (map snd uni)

    -- Send node information back to the 'slave' nodes using their connection
    -- handles.
    broadcastUniverse :: [(Handle,ByteString)] -> IO ()
    broadcastUniverse universe = do
        let otherNodes = tail universe
            rootNodeH  = fst (head universe)

        forM_ otherNodes $ \(h,_) ->
          forM_ universe $ \(_,bs) -> hPutStrLn h bs

        hClose rootNodeH

    -- 'slave' nodes connect, send their bytestring and then wait for the
    -- universe details to be sent to them.
    sendDetails:: IO [ByteString]
    sendDetails = withSocketsDo $ do

      -- TODO: Should check all the addresses returned from getAddrInfo
      (rootAddr:_) <- getAddrInfo
                      (Just (defaultHints {addrFamily = AF_INET}))
                      (Just (startupHost conf))
                      (Just (startupPort conf))
      s <- socket (addrFamily rootAddr) Stream defaultProtocol

      connSuc <- timeOut conf $ let retry = connect s (addrAddress rootAddr)
                                              `catchIOError`
                                              (\_ -> threadDelay 1000 >> retry)
                                  in retry
      case connSuc of
        Nothing  -> moduleError "sendDetails" "Failed to connect to root node."
        Just _ -> return ()

      h <- socketToHandle s ReadWriteMode
      hPutStrLn h nodeEnc

      universe <- recvNodeDetails conf h (numProcs conf)
      case universe of
        Nothing  -> moduleError "sendDetails" "Failed to receive from root node."
        Just uni -> hClose h >> return uni

-- Handle the connections (sequentially, could also be done in parallel).
recvNByteStrings :: RTSConf
                 -> Socket
                 -> Int
                 -> ByteString
                 -> IO (Maybe [(Handle,ByteString)])
recvNByteStrings conf s numprocs meEnc = timeOut conf $ go 1 []
  where go i nodes
          | i == numprocs = do
            meH <- socketToHandle s ReadWriteMode
            return $ (meH, meEnc):nodes
          | otherwise = do
              (clientSock, _) <- accept s
              h  <- socketToHandle clientSock ReadWriteMode
              bs <- hGetLine h
              go (i+1) $ (h,bs):nodes


-- Receive Bytestrings for all other processes.
recvNodeDetails :: RTSConf -> Handle -> Int -> IO (Maybe [ByteString])
recvNodeDetails cfg h numprocs = timeOut cfg $ replicateM numprocs (hGetLine h)

-- 10 seconds default timeout
timeOut :: RTSConf -> IO a -> IO (Maybe a)
timeOut cfg = let t = startupTimeout cfg
              in timeout (1000000 * t)

-- Print an error message from this module
-- Params: fn = function name
--          s = error string.
moduleError :: String -> String -> IO a
moduleError fn s = let m = "Control.Parallel.HdpH.Internal.CommstartupTCP"
                        in error $ m ++ "." ++ fn ++ ": " ++ s
