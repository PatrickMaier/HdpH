-- Global references; state
--
-- Visibility: HdpH.Internal.GRef
-- Author: Patrick Maier <P.Maier@hw.ac.uk>
-- Created: 25 Sep 2011
--
-----------------------------------------------------------------------------

{-# OPTIONS_GHC -fno-cse #-}  -- to protect unsafePerformIO hack

module HdpH.Internal.State.GRef
  ( -- * reference to this node's registry
    regRef  -- :: IORef GRefReg
  ) where

import Prelude
import Data.IORef (IORef, newIORef)
import qualified Data.Map as Map (empty)
import System.IO.Unsafe (unsafePerformIO)

import HdpH.Internal.Type.GRef (GRefReg(GRefReg), lastSlot, table)


-----------------------------------------------------------------------------
-- reference to this node's registry, initially empty

regRef :: IORef GRefReg
regRef = unsafePerformIO $
           newIORef $ GRefReg { lastSlot = 0, table = Map.empty }
{-# NOINLINE regRef #-}   -- required to protect unsafePerformIO hack
