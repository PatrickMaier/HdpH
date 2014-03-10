-- Topology
-- TODO: Improve doc and error messages

module Control.Parallel.HdpH.Internal.Topology
  ( -- * ultrametric distance
    dist,        -- :: Node -> Node -> Dist

    -- * equidistant bases
    Basis,       -- synonym: [(Node,Int)]
    Bases,       -- synonym: DistMap Basis
    equiDist,    -- :: [Node] -> Dist -> Basis
    equiDistMap  -- :: [Node] -> Bases
  ) where

import Prelude
import Control.DeepSeq (($!!))

import Control.Parallel.HdpH.Dist (Dist, zero, one, div2)
import Control.Parallel.HdpH.Internal.Location (Node, path)
import Control.Parallel.HdpH.Internal.Data.DistMap (DistMap, new)
import Control.Parallel.HdpH.Internal.Misc (takeUntil)

thisModule :: String
thisModule = "Control.Parallel.HdpH.Internal.Topology"


-------------------------------------------------------------------------------
-- ultrametric distance

dist :: Node -> Node -> Dist
dist p q =
  if p == q then zero else diff (path p) (path q) one
    where
   -- diff :: (Eq a) => [a] -> [a] -> Dist -> Dist
      diff (x:xs) (y:ys) d = if x == y then diff xs ys (div2 d) else d
      diff _      _      d = d  -- Note that 'diff [] [] d' is impossible!


-------------------------------------------------------------------------------
-- equidistant bases

-- equidistant basis
type Basis = [(Node,Int)]


-- distance-indexed map of equidistant bases
type Bases = DistMap Basis


-- Size-enriched equidistant basis of radius 'r' around first node of 'ps';
-- the first node 'p0' is always the first node on the output list.
equiDist :: [Node] -> Dist -> Basis
equiDist []        _ = error $ thisModule ++ ".equiDist: empty argument"
equiDist ps@(p0:_) r | r == zero = [(p0, 1)]
                     | otherwise = sieve ps
  where
    sieve []      = []
    sieve (q0:qs) = (q0, n) : sieve [q | q <- qs, dist q0 q == r]
                      where
                        n = length [p | p <- ps, dist q0 p < r]


-- Distance-indexed map of equidistant bases around first node of 'ps'.
-- Note that the map is fully normalised.
equiDistMap :: [Node] -> Bases
equiDistMap ps =
  new $!! takeUntil smallestBasis $ map (equiDist ps) $ iterate div2 one
    where
      smallestBasis [(_, 1)] = True
      smallestBasis _        = False
