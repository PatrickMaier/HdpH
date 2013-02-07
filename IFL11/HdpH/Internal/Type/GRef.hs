-- Global references; types
--
-- Visibility: HdpH.Internal.{GRef,State.GRef}
-- Author: Patrick Maier <P.Maier@hw.ac.uk>
-- Created: 19 Sep 2011
--
-----------------------------------------------------------------------------

module HdpH.Internal.Type.GRef
  ( -- * global references
    GRef(..),

    -- * registry for global references
    GRefReg(..)
  ) where

import Prelude
import Data.Map (Map)

import HdpH.Internal.Location (NodeId)
import HdpH.Internal.Misc (AnyType)


-----------------------------------------------------------------------------
-- global references

-- Global references, comprising a locally unique slot on the hosting node
-- and the hosting node's ID. Note that the slot, represented by a positive
-- integer, must be unique over the life time of the hosting node (ie. it
-- can not be reused). Note also that the type constructor 'GRef' takes
-- a phantom type parameter, tracking the type of the referenced object.
data GRef a = GRef { slot :: !Integer,
                     at   :: !NodeId }


-----------------------------------------------------------------------------
-- registry for global references

-- Registry, comprising of the most recently allocated slot and a table
-- mapping slots to objects (wrapped in an existential type).
data GRefReg = GRefReg { lastSlot :: !Integer,
                         table    :: Map Integer AnyType }
