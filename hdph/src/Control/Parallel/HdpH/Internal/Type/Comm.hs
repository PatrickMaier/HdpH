-- TCP-based communication layer; types
--
-- Author: Rob Stewart, Patrick Maier
-----------------------------------------------------------------------------

module Control.Parallel.HdpH.Internal.Type.Comm
  ( -- * internal state of comm layer, incl pre-initial state
    State(..),
    state0,

    -- * payload messages, message queue, and connection cache
    Payload,
    PayloadQ,
    ConnCache
  ) where

import Prelude
import Control.Concurrent (Chan)
import Data.ByteString.Lazy as Lazy (ByteString)
import Network.Transport (Transport, EndPoint, Connection)

import Control.Parallel.HdpH.Conf (RTSConf, defaultRTSConf)
import Control.Parallel.HdpH.Internal.Data.CacheMap.Strict (CacheMap)
import Control.Parallel.HdpH.Internal.Location (Node)
import Control.Parallel.HdpH.Internal.Topology (Bases)


-----------------------------------------------------------------------------
-- state representation

data State =
  State {
    s_conf      :: RTSConf,    -- config data
    s_nodes     :: Int,        -- number of nodes of parallel machine
    s_allNodes  :: [Node],     -- all nodes of parallel machine;
                                 -- head of list is this node
    s_bases     :: Bases,      -- distance-indexed map of equidistant bases
    s_root      :: Maybe Node, -- root node (Nothing if this is the root)
    s_msgQ      :: PayloadQ,   -- queue holding received payload messages
    s_tp        :: Transport,  -- this node's transport
    s_ep        :: EndPoint,   -- this node's end point
    s_connCache :: ConnCache } -- connections to all nodes (not scalable)
--
-- NOTE: The field 's_allNodes' should be phased out; storing a list of all
--       nodes at every node does not scale. The information can be recovered
--       using a recursive computation over the equidistant bases stored in
--       field 's_bases'.

-- concurrent message queue storing payload messages
type PayloadQ = Chan Payload

-- payload message
type Payload = Lazy.ByteString

-- connection cache
type ConnCache = CacheMap Node Connection


-----------------------------------------------------------------------------
-- pre-initial state

-- pre-initial state (as evident from the fields 's_nodes' and 's_allNodes')
-- for the comm layer; used to initialise the IORef
state0 :: State
state0 =
  State {
    s_conf      = defaultRTSConf,
    s_nodes     = 0,       -- 0 nodes; obviously pre-initial
    s_allNodes  = [],      -- empty list; obviously pre-initial
    s_root      = Nothing,
    s_bases     = error $ pref ++ "field 's_bases' not initialised",
    s_msgQ      = error $ pref ++ "field 's_msgQ' not initialised",
    s_tp        = error $ pref ++ "field 's_tp' not initialised",
    s_ep        = error $ pref ++ "field 's_ep' not initialised",
    s_connCache = error $ pref ++ "field 's_connCache' not initialised" }
      where
        pref = "Control.Parallel.HdpH.Internal.State.Comm.stateRef: "
