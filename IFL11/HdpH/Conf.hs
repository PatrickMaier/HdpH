-- HpdH runtime configuration parameters
--
-- Visibility: public
-- Author: Patrick Maier <P.Maier@hw.ac.uk>
-- Created: 12 Oct 2011
--
-----------------------------------------------------------------------------

module HdpH.Conf
  ( -- * monad stack configuration parameters
    RTSConf(..),
    defaultRTSConf  -- :: RTSConf
  ) where

import Prelude

import HdpH.Internal.Location (dbgNone)


-----------------------------------------------------------------------------
-- Runtime configuration parameters (for RTS monad stack)

data RTSConf =
  RTSConf {
    debugLvl   :: Int,  -- non-negative;
                        -- debug level (see module HdpH.Internal.Location)
    scheds     :: Int,  -- positive;
                        -- #schedulers
    wakeupDly  :: Int,  -- positive;
                        -- interval (#microseconds) to wake up racey schedulers
    maxHops    :: Int,  -- non-negative;
                        -- #hops FISH may travel
    maxFish    :: Int,  -- non-negative;
                        -- max #sparks for triggering FISHing for more sparks
    minSched   :: Int,  -- non-negative (and typically > maxFish)
                        -- min #sparks necessary for SCHEDULEing any away
    minFishDly :: Int,  -- non-negative;
                        -- min #microseconds delay after failed FISH
    maxFishDly :: Int } -- non-negative (and typically >= minFishDly);
                        -- max #microseconds delay after failed FISH

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
    maxFishDly = 1000000 } -- delay up to 1 second after failed FISH
