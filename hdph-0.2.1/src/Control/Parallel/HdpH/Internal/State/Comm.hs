-- TCP-based communication layer; state
--
-- Author: Rob Stewart, Patrick Maier
-----------------------------------------------------------------------------

module Control.Parallel.HdpH.Internal.State.Comm
  ( -- * reference to comm layer's internal state
    stateRef
  ) where

import Prelude
import Data.IORef (IORef, newIORef)
import System.IO.Unsafe (unsafePerformIO)

import Control.Parallel.HdpH.Internal.Type.Comm (State, state0)


-----------------------------------------------------------------------------
-- reference to this Comm state

stateRef :: IORef State
stateRef = unsafePerformIO $ newIORef state0
{-# NOINLINE stateRef #-}    -- required to protect unsafePerformIO hack
