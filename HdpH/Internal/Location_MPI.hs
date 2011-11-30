-- Locations (via MPI)
--
-- Visibility: HdpH.Internal
-- Author: Patrick Maier <P.Maier@hw.ac.uk>
-- Created: 21 Sep 2011
--
-----------------------------------------------------------------------------

{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE DeriveDataTypeable #-}

module HdpH.Internal.Location_MPI
  ( -- * node IDs (and their constitutent parts)
    NodeId,           -- instances: Eq, Ord, Show, NFData, Serialize, Typeable
    rank,             -- :: NodeId -> MP.MPI.Rank
    host,             -- :: NodeId -> IPv4Addr
    mkMyNodeId,       -- :: IO NodeId
    NetworkAddr,      -- instances: Eq, Ord, Show, NFData, Serialize
    validNetworkAddr, -- :: NetworkAddr -> Bool
    fromNetworkAddr   -- :: NetworkAddr -> [Word8]
  ) where

import Prelude hiding (error)
import Control.DeepSeq (NFData(rnf))
import Data.Bits ((.&.), shiftR)
import Data.Functor ((<$>))
import Data.List (intercalate)
import Data.Serialize (Serialize)
import qualified Data.Serialize (put, get, putWord32be, getWord32be)
import Data.Typeable (Typeable)
import Data.Word (Word8)
import Network.BSD (getHostName, getHostByName, hostAddresses)

import qualified MP.MPI as MPI (myRank)

import HdpH.Internal.Type.Location_MPI
  (NodeId(NodeId, rank, host), NetworkAddr(IPv4Addr))


-----------------------------------------------------------------------------
-- node IDs (abstract outwith this module)
-- NOTE: Node IDs are hyperstrict.

-- Constructs the node ID of the current node;
-- ensures the resulting ID is hyperstrict;
-- this constructor is only to be exported to module HdpH.Internal.Comm.
mkMyNodeId :: IO NodeId
mkMyNodeId = do
  i <- MPI.myRank
  addrs <- hostAddresses <$> (getHostByName =<< getHostName)
  let addr = case addrs of
               []    -> invalidIPv4Addr
               rep:_ -> IPv4Addr rep
  -- force i and addr to make resulting NodeId hyperstrict
  rnf i `seq` rnf addr `seq` return $ NodeId { rank = i, host = addr }


deriving instance Eq NodeId
deriving instance Ord NodeId
deriving instance Typeable NodeId

instance Show NodeId where
  showsPrec _ node = showChar '<' . shows (rank node) . showChar '@' . 
                     shows (host node) . showChar '>'

instance NFData NodeId  -- default instance suffices (due to hyperstrictness)

instance Serialize NodeId where  -- 'get' ensures the result is hyperstrict
  put node = Data.Serialize.put (rank node) >>
             Data.Serialize.put (host node)
  get = do
    i    <- Data.Serialize.get
    addr <- Data.Serialize.get
    -- force i and addr to make resulting NodeId hyperstrict
    rnf i `seq` rnf addr `seq` return $ NodeId { rank = i, host = addr }


-----------------------------------------------------------------------------
-- network addresses (abstract outwith this module)

deriving instance Eq NetworkAddr
deriving instance Ord NetworkAddr

instance Show NetworkAddr where
  show addr@(IPv4Addr _) = intercalate "." $ map show $ fromNetworkAddr addr

instance NFData NetworkAddr where
  rnf (IPv4Addr rep) = rnf rep

instance Serialize NetworkAddr where
  put (IPv4Addr rep) = Data.Serialize.putWord32be rep
  get = IPv4Addr <$> Data.Serialize.getWord32be


-- Constructor for an invalid network address;
-- used as a dummy value when proper network addresses are unavailable;
-- not to be exported.
invalidIPv4Addr :: NetworkAddr
invalidIPv4Addr = IPv4Addr 0


-- Predicate indicating whether a network address is supposed to be valid.
validNetworkAddr :: NetworkAddr -> Bool
validNetworkAddr addr@(IPv4Addr _) = addr /= invalidIPv4Addr


-- Convert a network address into a list of bytes.
fromNetworkAddr :: NetworkAddr -> [Word8]
fromNetworkAddr (IPv4Addr rep) = [fromIntegral $ rep `shiftR` (0 * 8) .&. 255,
                                  fromIntegral $ rep `shiftR` (1 * 8) .&. 255,
                                  fromIntegral $ rep `shiftR` (2 * 8) .&. 255,
                                  fromIntegral $ rep `shiftR` (3 * 8) .&. 255]
