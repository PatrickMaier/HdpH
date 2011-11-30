-- Locations; state
--
-- Visibility: HdpH.Internal.Location, HdpH.Internal.Comm
-- Author: Patrick Maier <P.Maier@hw.ac.uk>
-- Created: 22 Sep 2011
--
-----------------------------------------------------------------------------

{-# OPTIONS_GHC -fno-cse #-}  -- to protect unsafePerformIO hack

module HdpH.Internal.State.Location
  ( -- * reference to this node's ID
    myNodeRef,    -- :: IORef NodeId

    -- * reference to IDs of all nodes
    allNodesRef,  -- :: IORef [NodeId]

    -- * reference to system debug level
    debugRef      -- :: IORef Int
  ) where

import Prelude
import Control.Exception (throw)
import Data.IORef (IORef, newIORef)
import System.IO.Unsafe (unsafePerformIO)

import HdpH.Internal.Type.Location (NodeId, MyNodeException(NodeIdUnset))


-----------------------------------------------------------------------------
-- reference to ID of this node;
-- will be set in module HdpH.Internal.Comm;
-- if unset, access will raise a 'MyNodeException'

myNodeRef :: IORef NodeId
myNodeRef = unsafePerformIO $ newIORef $ throw NodeIdUnset
{-# NOINLINE myNodeRef #-}    -- required to protect unsafePerformIO hack


-----------------------------------------------------------------------------
-- reference to list of IDs of all nodes;
-- will be set (to non-empty list) in module HdpH_IO

allNodesRef :: IORef [NodeId]
allNodesRef = unsafePerformIO $ newIORef $ []
{-# NOINLINE allNodesRef #-}  -- required to protect unsafePerformIO hack


-----------------------------------------------------------------------------
-- reference to system debug level;
-- may be set in module HdpH.Internal.Comm;
-- referenced value must be non-negative (0 means no debug output)

debugRef :: IORef Int
debugRef  = unsafePerformIO $ newIORef $ 0
{-# NOINLINE debugRef #-}     -- required to protect unsafePerformIO hack
