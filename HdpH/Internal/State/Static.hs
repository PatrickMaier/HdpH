-- 'Static' support; state
--
-- Visibility: HdpH.Internal.Static
-- Author: Patrick Maier <P.Maier@hw.ac.uk>
-- Created: 26 Sep 2011
--
-----------------------------------------------------------------------------

{-# OPTIONS_GHC -fno-cse #-}  -- to protect unsafePerformIO hack

module HdpH.Internal.State.Static
  ( -- * reference to 'Static' declaration
    sdRef  -- :: IORef StaticDecl
  ) where

import Prelude
import Data.IORef (IORef, newIORef)
import qualified Data.Map as Map (empty)
import System.IO.Unsafe (unsafePerformIO)

import HdpH.Internal.Type.Static (StaticDecl(StaticDecl))


-----------------------------------------------------------------------------
-- reference to global 'Static' declaration, initially empty

sdRef :: IORef StaticDecl
sdRef = unsafePerformIO $ newIORef $ StaticDecl Map.empty
{-# NOINLINE sdRef #-}   -- required to protect unsafePerformIO hack
