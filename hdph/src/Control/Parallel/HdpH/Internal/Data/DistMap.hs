-- Distance-indexed maps, mapping non-zero distances to values
--
-- Author: Patrick Maier
-----------------------------------------------------------------------------

{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module Control.Parallel.HdpH.Internal.Data.DistMap
  ( -- * abstract type of distance-indexed maps
    DistMap,     -- kind * -> *; instances: NFData

    -- * operations
    new,         -- :: [a] -> DistMap a
    toDescList,  -- :: DistMap a -> [(Dist, a)]
    update,      -- :: Dist -> a -> DistMap a -> DistMap a
    lookup,      -- :: Dist -> DistMap a -> a
    keys,        -- :: DistMap a -> [Dist]
    keysFromTo,  -- :: Dist -> Dist -> DistMap a -> [Dist]
    minDist      -- :: DistMap a -> Dist
  ) where

import Prelude hiding (lookup)
import Control.DeepSeq (NFData)
import Data.Array (Array, assocs, bounds, range, (!), listArray, (//))
import Data.List (foldl')

import Control.Parallel.HdpH.Dist (Dist, zero, one, div2)

thisModule :: String
thisModule = "Control.Parallel.HdpH.Internal.Data.DistMap"

-------------------------------------------------------------------------------
-- distance-indexed lookup table

-- A distance-indexed lookup table (for mapping non-zero distances) is
-- a distance-indexed array, the lower bound of which is 'one'.
-- (Note that the class 'Ix' reverses the order for distance indices /= 'zero'.)
newtype DistMap a = DistMap (Array Dist a) deriving (NFData)


-- Create a new distance-indexed map from a sorted list of values.
-- The argument is assumed to be sorted contiguously in descending order of
-- distances, starting at distance 'one'.
new :: [a] -> DistMap a
new vs = DistMap $! listArray (one, smallestIndex vs) vs
  where
    smallestIndex :: [a] -> Dist
    smallestIndex []     = zero
    smallestIndex (_:xs) = foldl' (const . div2) one xs


-- Convert the distance-indexed map into a list of distance/value pairs,
-- sorted in descending order of distance.
toDescList :: DistMap a -> [(Dist, a)]
toDescList (DistMap arr) = assocs arr


-- Update given distance-indexed map at distance 'r' with value 'u'.
-- Input map must be defined at 'r', or else 'update' aborts with an error.
update :: Dist -> a -> DistMap a -> DistMap a
update r v (DistMap arr)
  | r < snd (bounds arr) = error $ thisModule ++ ".update: out of bounds"
  | otherwise            = DistMap $! arr // [(r, v)]


-- Look up distance 'r' in given distance-indexed map.
-- If 'r' is 'zero' or too small for the map, returns the value at 'minDist'.
lookup :: Dist -> DistMap a -> a
lookup r (DistMap arr) = arr ! (max r $ snd $ bounds arr)


-- Distances at which given distance-indexed map is defined;
-- starting from 'one' and sorted in descending order.
keys :: DistMap a -> [Dist]
keys (DistMap arr) = range $ bounds arr


-- Distances at which given distance-indexed map is defined;
-- in descending order, starting from upper bound 'ub' and going down
-- to lower bound 'lb' (or the minDist, whichever comes first).
keysFromTo :: Dist -> Dist -> DistMap a -> [Dist]
keysFromTo ub lb (DistMap arr)
  | lb > ub   = []
  | otherwise = from ub
                  where
                    r_min = max lb $ snd $ bounds arr
                    from !r | r_min > r = []
                            | otherwise = r : from (div2 r)


-- Minimal distance occuring as key in the given distance-indexed map.
minDist :: DistMap a -> Dist
minDist (DistMap arr)
  | r_min == zero = error $ thisModule ++ ".minDist: map empty"
  | otherwise     = r_min
      where
        r_min = snd $ bounds arr
