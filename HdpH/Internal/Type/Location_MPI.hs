-- Locations (via MPI); types
--
-- Visibility: HdpH.Internal.{Location_MPI,State.Location_MPI}
-- Author: Patrick Maier <P.Maier@hw.ac.uk>
-- Created: 22 Sep 2011
--
-----------------------------------------------------------------------------

module HdpH.Internal.Type.Location_MPI
  ( -- * node IDs (and their constitutent parts)
    NodeId(..),
    NetworkAddr(..)
  ) where

import Prelude
import Data.Word (Word32)

import qualified MP.MPI as MPI (Rank)


-----------------------------------------------------------------------------
-- node IDs

-- node IDs, comprised of MPI rank (which identifies the node) and
-- additional information (currently the node's network address)
data NodeId = NodeId { rank :: !MPI.Rank,     -- MPI rank of node
                       host :: !NetworkAddr } -- network address of node


-----------------------------------------------------------------------------
-- network addresses (typically identify hosts, not nodes)

-- network addresses (currently IPv4, represented as 32 big endian bit words)
data NetworkAddr = IPv4Addr !Word32
