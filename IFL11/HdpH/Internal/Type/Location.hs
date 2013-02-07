-- Locations; types; wrapper module
--
-- Visibility: HdpH.Internal.{Location,State.Location}
-- Author: Patrick Maier <P.Maier@hw.ac.uk>
-- Created: 22 Sep 2011
--
-----------------------------------------------------------------------------

{-# LANGUAGE DeriveDataTypeable #-}  -- for defining exceptions

module HdpH.Internal.Type.Location
  ( -- re-exports from HdpH.Internal.Type.Location_MPI

    -- * node IDs (and their constitutent parts)
    NodeId(..),
    NetworkAddr(..),

    -- end of re-exports

    -- * node ID exception
    MyNodeException(..)  -- instances: Exception, Show, Typeable
  ) where

import Prelude
import Control.Exception (Exception)
import Data.Typeable (Typeable)

-- import what's re-exported above
import HdpH.Internal.Type.Location_MPI


-----------------------------------------------------------------------------
-- exception raised when ID of this node is not set

data MyNodeException = NodeIdUnset
                       deriving (Show, Typeable)

instance Exception MyNodeException
