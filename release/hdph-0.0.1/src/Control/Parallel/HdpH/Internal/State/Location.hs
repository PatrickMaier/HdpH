-- Locations; state
--
-- Author: Rob Stewart, Patrick Maier
-----------------------------------------------------------------------------

{-# OPTIONS_GHC -fno-cse #-}  -- to protect unsafePerformIO hack

module Control.Parallel.HdpH.Internal.State.Location
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

import Control.Parallel.HdpH.Internal.Type.Location
       (NodeId, MyNodeException(NodeIdUnset))


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
