-- 'Static' support; state
--
-- Visibility: HdpH.Closure.Static
-- Author: Patrick Maier <P.Maier@hw.ac.uk>
-- Created: 26 Sep 2011
--
-----------------------------------------------------------------------------

{-# OPTIONS_GHC -fno-cse #-}  -- to protect unsafePerformIO hack

module HdpH.Closure.Static.State
  ( -- * reference to 'Static' declaration
    sTblRef  -- :: IORef StaticTable
  ) where

import Prelude
import Data.Array (array)
import Data.IORef (IORef, newIORef)
import System.IO.Unsafe (unsafePerformIO)

import HdpH.Closure.Static.Type (StaticTable)


-----------------------------------------------------------------------------
-- reference to global static table, initially empty

sTblRef :: IORef StaticTable
sTblRef = unsafePerformIO $ newIORef $ array (1,0)  []
{-# NOINLINE sTblRef #-}   -- required to protect unsafePerformIO hack
