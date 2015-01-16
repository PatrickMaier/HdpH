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

startupTCP :: RTSConf -> ByteString -> IO [ByteString]
startupTCP conf meEnc = let valid = validateConf in
                        if valid then startupTCP' conf meEnc else reportError
  where validateConf   = not (startupHost conf == "" || startupPort conf == "")
        reportError    = error "Control.Parallel.HdpH.Internal.CommStartupTCP.startupTCP:\
                               \ You must specify the optons \"startupHost\"\
                               \ and \"startupPort\" to use the TCP startup backend."

-- TODO: More robust error handling.
startupTCP' :: RTSConf -> ByteString -> IO [ByteString]
startupTCP' conf nodeEnc = do hn <- getHostName
                              if hn /= startupHost conf then sendDetails else handleStartupRootNode

  where
    handleStartupRootNode = withSocketsDo $ do
      masterSock <- tryBind
      case masterSock of
        Nothing -> sendDetails
        Just s  -> rootProcStartup s

    tryBind = do (hostAddr:_) <- getAddrInfo (Just (defaultHints {addrFamily = AF_INET})) (Just (startupHost conf)) (Just (startupPort conf))
                 s <- socket AF_INET Stream defaultProtocol
                 (bindSocket s (addrAddress hostAddr) >> return (Just s)) `catchIOError` (\_ -> return Nothing)

    -- Start the root node listening and wait to receive 'numProc' bytestrings (or timeout).
    rootProcStartup s = do
      let numproc = numProcs conf
      listen s numproc
      universe <- recvNByteStrings s numproc nodeEnc
      case universe of
        Nothing  -> error "Control.Parallel.HdpH.Internal.CommStartupTCP.rootProcStartup:\
                          \ Failed to receive messages from all nodes."
        Just uni -> broadcastUniverse uni >> return (map snd uni)

    broadcastUniverse universe = do
        let otherNodes = tail universe
            rootNodeH  = fst (head universe)

        forM_ otherNodes $ \(h,_) ->
          forM_ universe $ \(_,bs) -> hPutStrLn h bs

        hClose rootNodeH

    -- Non root nodes attempt to send data
    sendDetails = withSocketsDo $ do

      (rootAddr:_) <- getAddrInfo (Just (defaultHints {addrFamily = AF_INET})) (Just (startupHost conf)) (Just (startupPort conf))
      s <- socket (addrFamily rootAddr) Stream defaultProtocol

      connSuc <- defaultTimeOut $ let retry = connect s (addrAddress rootAddr)
                                              `catchIOError`
                                              (\_ -> threadDelay 1000 >> retry)
                                  in retry
      case connSuc of
        Nothing  -> error "Control.Parallel.HdpH.Internal.CommStartupTCP.rootProcStartup:\
                          \ Failed to connect to root node."
        Just _ -> return ()

      h <- socketToHandle s ReadWriteMode
      hPutStrLn h nodeEnc

      universe <- recvNodeDetails h (numProcs conf)
      case universe of
        Nothing  -> error "Control.Parallel.HdpH.Internal.CommStartupTCP.rootProcStartup:\
                          \ Failed to receive ByteStrings from root node."
        Just uni -> hClose h >> return uni

-- Handle the connections (sequentially, could also be done in parallel).
recvNByteStrings :: Socket -> Int -> ByteString -> IO (Maybe [(Handle,ByteString)])
recvNByteStrings s numprocs meEnc = defaultTimeOut $ go 1 []
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
recvNodeDetails :: Handle -> Int -> IO (Maybe [ByteString])
recvNodeDetails h numprocs = defaultTimeOut $ replicateM numprocs (hGetLine h)

-- 10 seconds default timeout
defaultTimeOut :: IO a -> IO (Maybe a)
defaultTimeOut = let defaultTimeoutSec = 10 in timeout (1000000 * defaultTimeoutSec)
