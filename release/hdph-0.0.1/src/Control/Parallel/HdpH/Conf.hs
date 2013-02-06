-- HpdH runtime configuration parameters
--
-- Author: Patrick Maier
-----------------------------------------------------------------------------

module Control.Parallel.HdpH.Conf
  ( -- * HdpH runtime system configuration parameters
    RTSConf(..),
    defaultRTSConf  -- :: RTSConf
  ) where

import Prelude

import Control.Parallel.HdpH.Internal.Location (dbgNone)


-----------------------------------------------------------------------------
-- Runtime configuration parameters (for RTS monad stack)

-- | 'RTSConf' is a record data type collecting a number of parameter
-- governing the behaviour of the HdpH runtime system.
data RTSConf =
  RTSConf {
    debugLvl :: Int,
        -- ^ Debug level, a number defined in module
        -- "Control.Parallel.HdpH.Internal.Location".
        -- Default is 0 (corresponding to no debug output).

    scheds :: Int,
        -- ^ Number of concurrent schedulers per node. Must be positive and 
        -- should be @<=@ to the number of HECs (as set by GHC RTS option 
        -- @-N@). Default is 1.

    wakeupDly :: Int,
        -- ^ Interval in microseconds to wake up sleeping schedulers
        -- (which is necessary to recover from a race condition between
        -- concurrent schedulers). Must be positive. 
        -- Default is 1000 (corresponding to 1 millisecond).

    maxHops :: Int,
        -- ^ Number of hops a FISH message may travel before being considered
        -- failed. Must be non-negative. Default is 7.

    maxFish :: Int,
        -- ^ Low sparkpool watermark for fishing. RTS will send FISH message 
        -- unless size of spark pool is greater than 'maxFish' (or unless 
        -- a FISH is outstanding). Must be non-negative;
        -- should be @<@ 'minSched'. Default is 1.

    minSched :: Int,
        -- ^ Low sparkpool watermark for scheduling. RTS will respond to FISH 
        -- messages by SCHEDULEing sparks unless size of spark pool is less
        -- than 'minSched'. Must be non-negative; should be @>@ 'maxFish'.
        -- Default is 2.

    minFishDly :: Int,
        -- ^ After a failed FISH, minimal delay in microseconds before
        -- sending another FISH message; the actual delay is chosen randomly
        -- between 'minFishDly' and 'maxFishDly'. Must be non-negative; should
        -- be @<=@ 'maxFishDly'.
        -- Default is 10000 (corresponding to 10 milliseconds).

    maxFishDly :: Int,
        -- ^ After a failed FISH, maximal delay in microseconds before
        -- sending another FISH message; the actual delay is chosen randomly
        -- between 'minFishDly' and 'maxFishDly'. Must be non-negative; should
        -- be @>=@ 'minFishDly'.
        -- Default is 1000000 (corresponding to 1 second).

    numProcs   :: Int,
        -- ^ Number of nodes constituting the distributed runtime system.
        -- Must be positive. Default is 1.

    networkInterface :: String
        -- ^ Network interface, required to autodetect a node's
        -- IP address. The string must be one of the interface names 
        -- returned by the POSIX command @ifconfig@. 
        -- Default is @eth0@ (corresponding to the first Ethernet interface).
    }


-- | Default runtime system configuration parameters.
defaultRTSConf :: RTSConf
defaultRTSConf =
  RTSConf {
    debugLvl   = dbgNone,  -- no debug information
    scheds     = 1,        -- only 1 scheduler by default
    wakeupDly  = 1000,     -- wake up one sleeping scheduler every millisecond
    maxHops    = 7,        -- no more than 7 hops per FISH
    maxFish    = 1,        -- send FISH when <= 1 spark in pool
    minSched   = 2,        -- reply with SCHEDULE when >= 2 sparks in pool
    minFishDly = 10000,    -- delay at least 10 milliseconds after failed FISH
    maxFishDly = 1000000,  -- delay up to 1 second after failed FISH
    numProcs   = 1,        -- only 1 node by default
    networkInterface = "eth0" }  -- first Ethernet adapter default inferface
