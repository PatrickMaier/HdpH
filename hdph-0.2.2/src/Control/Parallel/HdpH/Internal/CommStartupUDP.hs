-- Node to node communication (startup sync via UDP)
--
-- NOTE: This module works for now but needs hardening:
--       Proof broadcasts against cross-talk;
--       make sure that every participant agrees on outcome.
--
-- Author: Rob Stewart, Patrick Maier
-----------------------------------------------------------------------------

{-# LANGUAGE CPP #-}
{-# LANGUAGE BangPatterns #-}

module Control.Parallel.HdpH.Internal.CommStartupUDP
  ( -- * all-to-all synchronisation by exchanging bytestrings
    allgatherByteStrings,
    defaultAllgatherByteStrings,
    defaultMaxSize,
    defaultBcastTime,
    defaultBcastDelay,
    defaultRecvTime
  ) where

import Prelude
import Control.Concurrent (forkIO, threadDelay)
import Control.Monad (forever)
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS (empty, length, null)
import Network.Multicast (multicastSender, multicastReceiver)
import Network.Socket.ByteString (sendTo, recvFrom)
import System.Timeout (timeout)


-- fixed host name and port number used for UDP broadcast
host :: String
host = "224.0.0.99"
port :: Int
port = 9999


-- | Default maximal byte string size.
defaultMaxSize :: Int
defaultMaxSize = 1024


-- | Default timeout (in seconds) for broadcasting byte strings.
defaultBcastTime :: Int
defaultBcastTime = 10


-- | Default delay (in milliseconds) for between broadcasts.
defaultBcastDelay :: Int
defaultBcastDelay = 100


-- | Default timeout (in seconds) for receiving byte strings;
--   should be greater than broadcast timeout.
defaultRecvTime :: Int
defaultRecvTime = 20 :: Int


-- | Distribute the given byte string via UDP broadcast and gather 'n - 1'
--   other byte strings also transmitted via UDP broadcast. Returns a list
--   of 'n' byte strings if successful, the empty list otherwise.
--   All participating byte strings must be non-empty, unique and smaller
--   than 'max_size', otherwise the operation will fail. It may also fail
--   by timeout if at least one of the participating nodes does not reply.
--   TODO: If one node received a timeout, how will other lear of that?
allgatherByteStrings :: Int -> Int -> Int -> Int -> Int -> ByteString
                     -> IO [ByteString]
allgatherByteStrings max_size bcast_time bcast_delay recv_time n bs = do
  -- Broadcast empty bytestring if 'bs' is too big
  let bs0 | BS.length bs > max_size = BS.empty
          | otherwise               = bs
  _ <- forkIO $ bcastByteString bcast_time bcast_delay bs0
  maybe_bss <- recvByteStrings max_size recv_time n
  case maybe_bss of
    Nothing  -> return []
    Just bss -> -- NOTE: length bss == n; now check for empty bytestring
                if any BS.null bss
                  then return []
                  else return bss


-- | Same as 'allgatherByteStrings'; with all parameters at default value.
defaultAllgatherByteStrings :: Int -> ByteString -> IO [ByteString]
defaultAllgatherByteStrings =
  allgatherByteStrings
    defaultMaxSize defaultBcastTime defaultBcastDelay defaultRecvTime


-- | Broadcast given byte string every 'delay' milliseconds to fixed
--   multicast address, until 'time' (in seconds) expires.
--   Ignores send errors. (TODO: Check errors as they could be catastrophic).
bcastByteString :: Int -> Int -> ByteString -> IO ()
bcastByteString time delay bs = do
  (sock, addr) <- multicastSender host (fromIntegral port)
  _ <- timeout (1000000 * time) $ forever $ do
    _ <- sendTo sock bs addr
    threadDelay (1000 * delay)
  return ()


-- | Receive 'n' byte strings from the fixed multicast address, until 'time'
--   (in seconds) expires. Returns Nothing or a list of 'n' byte strings.
--   All byte strings are expected to be unique and smaller than 'max_size'.
recvByteStrings :: Int -> Int -> Int -> IO (Maybe [ByteString])
recvByteStrings max_size time n = do
  sock <- multicastReceiver host (fromIntegral port)
  let receive 0  accu = return accu
      receive !k accu = do
        (bs, _) <- recvFrom sock max_size
        if bs `elem` accu
          then receive k accu
          else receive (k - 1) (bs:accu)
  timeout (1000000 * time) $ receive n []
