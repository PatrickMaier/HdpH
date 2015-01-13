-- Startup sync via a known master host/port and TCP connections.
--
-- Author: Blair Archibald
-----------------------------------------------------------------

module Control.Parallel.HdpH.Internal.CommStartupTCP
  ( startupTCP ) where

import Data.ByteString (ByteString)

import Control.Parallel.HdpH.Conf (RTSConf(..))

startupTCP :: RTSConf -> IO [ByteString]
startupTCP conf = let valid = validateConf conf in
                  if valid then startupTCP' else reportError
  where validateConf c = not (startupHost c == "" || startupPort c == "")
        reportError    = error "Control.Parallel.HdpH.Conf.startupTCP:\
                               \ You must specify the optons \"startupHost\"\
                               \ and \"startupPort\" to use the TCP startup backend."

startupTCP' :: IO [ByteString]
startupTCP' = undefined
