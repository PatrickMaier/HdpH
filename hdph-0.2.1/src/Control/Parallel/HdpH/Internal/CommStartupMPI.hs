-- Node to node communication (startup sync via MPI)
--
-- Author: Patrick Maier
-----------------------------------------------------------------------------

{-# LANGUAGE BangPatterns #-}

module Control.Parallel.HdpH.Internal.CommStartupMPI
  ( -- * all-to-all synchronisation by exchanging bytestrings
    defaultAllgatherByteStrings
  ) where

import Prelude
import Control.Concurrent.MVar (MVar, newMVar, modifyMVar)
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS (null)
import System.IO.Unsafe (unsafePerformIO)

import Control.Parallel.HdpH.MPI.Allgather
       (Size(..), withMPI, allgatherByteString)

-------------------------------------------------------------------------------
-- Allgather operation

-- | Ignores the first argument and distributes the given non-empty byte string
--   via MPI Allgather, synchronising all nodes along the way.
--   Returns a non-empty strict list of non-empty byte strings (one per node)
--   if successful, the empty list otherwise.
--   Note that subsequent calls return the cached result of the first call
--   (ignoring arguments to subsequent calls).
defaultAllgatherByteStrings :: Int -> ByteString -> IO [ByteString]
defaultAllgatherByteStrings _ bs0 =
  modifyMVar bssRef $ \ maybe_bss ->
    case maybe_bss of
      Just bss -> return (Just bss, bss)
      Nothing  -> withMPI $ \ _ _ -> do
                    bss <- allgatherByteString bs0
                    -- check for empty bytestring (also forces list)
                    if any BS.null bss
                      then return (Just [],  [])
                      else return (Just bss, bss)


-------------------------------------------------------------------------------
-- state (recording result of first call of defaultAllgatherByteStrings)

-- MVar stores Nothing before first first call, is empty during first call,
-- and stores Just result after first call.
bssRef :: MVar (Maybe [ByteString])
bssRef = unsafePerformIO $ newMVar Nothing
{-# NOINLINE bssRef #-}    -- required to protect unsafePerformIO hack
