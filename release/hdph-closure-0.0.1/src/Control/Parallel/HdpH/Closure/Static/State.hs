-- 'Static' support; state
--
-- Author: Patrick Maier
-----------------------------------------------------------------------------

{-# OPTIONS_GHC -fno-cse #-}  -- to protect unsafePerformIO hack

module Control.Parallel.HdpH.Closure.Static.State
  ( -- * reference to 'Static' declaration
    sTblRef  -- :: IORef StaticTable
  ) where

import Prelude
import Data.Array (array)
import Data.IORef (IORef, newIORef)
import System.IO.Unsafe (unsafePerformIO)

import Control.Parallel.HdpH.Closure.Static.Type (StaticTable)


-----------------------------------------------------------------------------
-- reference to global static table, initially empty

sTblRef :: IORef StaticTable
sTblRef = unsafePerformIO $ newIORef $ array (1,0)  []
{-# NOINLINE sTblRef #-}   -- required to protect unsafePerformIO hack
