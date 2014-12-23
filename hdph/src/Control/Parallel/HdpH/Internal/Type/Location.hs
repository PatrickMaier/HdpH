-- Locations; types
--
-- Author: Rob Stewart, Patrick Maier
-----------------------------------------------------------------------------

{-# LANGUAGE DeriveDataTypeable #-}  -- for defining exceptions

module Control.Parallel.HdpH.Internal.Type.Location
  ( -- * node IDs (and their constitutent parts)
    Node(..),

    -- * node ID exception
    MyNodeException(..)  -- instances: Exception, Show, Typeable
  ) where

import Prelude
import Control.Exception (Exception)
import Data.Typeable (Typeable)
import qualified Network.Transport as NT (EndPointAddress)


-----------------------------------------------------------------------------
-- node IDs

-- | A 'Node' identifies an HdpH node (that is, an OS process running HdpH).
-- A 'Node' should be thought of as an abstract identifier which can be
-- compared, displayed and serialised.
-- Internally, a 'Node' is a hyperstrict record consisting of an 'address'
-- field (uniquely identifying the node, and used for establishing network
-- connections), a 'path' field for computing distance in the topology and
-- a 'hash' field (for quick comparison, and for hash table access nodes).
data Node = Node { nodeHash :: !Int,  -- Data.Hashable uses Int for hash keys.
                   address  :: !NT.EndPointAddress, 
                   path     :: ![String] }

-- Notes:
-- * Currently, 'address' is the key field, ie. comparisons could ignore 
--   'hash' and 'path'. 
-- * An alternative design might consider making 'path' the key field. 
-- * A more optimal representation of 'path' should be considered, eg. as
--   an array of unboxed Word16s. For all practical purposes, such a path
--   could be serialised into two 64-bit words (into one for flatter 
--   hierarchies).


-----------------------------------------------------------------------------
-- exception raised when ID of this node is not set

data MyNodeException = MyNodeUnset
                       deriving (Show, Typeable)

instance Exception MyNodeException
