-- Distances and distance arithmetic.
-- TODO: Improve doc and error messages

{-# LANGUAGE GeneralizedNewtypeDeriving #-}  -- for deriving Serialize instance

module Control.Parallel.HdpH.Dist
  ( -- * distance type
    Dist            -- instances: Eq, Ord, Show, NFData, Serialize
  , isDist          -- :: Dist -> Bool
  , toRational      -- :: Dist -> Rational
  , fromRational    -- :: Rational -> Dist

    -- * distance arithmetic
  , zero            -- :: Dist
  , one             -- :: Dist
  , mul2            -- :: Dist -> Dist
  , div2            -- :: Dist -> Dist
  , plus            -- :: Dist -> Dist -> Dist
  ) where

import Prelude hiding (fromRational, toRational)
import Control.DeepSeq (NFData)
import Data.Ix (Ix)
import Data.Ratio ((%), numerator, denominator)
import Data.Serialize (Serialize)


-------------------------------------------------------------------------------
-- distance type

-- The value 0 represents 0, every positive value n represents 1/2^{n-1};
-- negative values are illegal.
newtype Dist = Dist Integer deriving (Eq, Ix, NFData, Serialize)
-- NOTE: The Ix instance of Dist is not compatible with the ordering because
--       'range (half, one) == []' even though 'half == div2 one < one'.
--       This is admissible according to the contract specified in 'Data.Ix';
--       it should not matter as long as Ix is only used for indexing arrays,
--       and it is remembered that the order for the purpose of indexing is: 
--       zero < one < div2 one < div2 (div2 one) < ...

isDist :: Dist -> Bool
isDist (Dist n) = n >= 0

instance Ord Dist where
  Dist m <= Dist n | m == 0          = True
                   | m > 0 && n == 0 = False
                   | otherwise       = n <= m

instance Show Dist where
  show r@(Dist n) | n <= 1    = show n
                  | otherwise = show $ toRational r

-- destructor
toRational :: Dist -> Rational
toRational (Dist n) | n == 0    = 0
                    | otherwise = 1 % (2 ^ (n - 1))

-- smart constructor
fromRational :: Rational -> Dist
fromRational r | numerator r == 0           = Dist 0
               | numerator r == 1 && z == 0 = Dist (n + 1)
               | otherwise                  = error "fromRational: wrong rep"
               where (n,z) = log2 (denominator r)

-- @log2 x@ is defined if @x@ is positive. If @log2 x = (n,z)@ then @z@ is
-- the smallest natural number such that @x = 2^n + z.
log2 :: (Integral a) => a -> (a, a)
log2 x | x <= 0    = error "log2: non-positive argument"
       | otherwise = (n - 1, x - 2 ^ (n - 1))
       where n = head $ dropWhile (\ k -> 2^k <= x) [1 ..]


-------------------------------------------------------------------------------
-- distance arithmetic

zero :: Dist
zero = Dist 0

one :: Dist
one = Dist 1

-- If {r == zero || r == one} then {mul2 r == r}, otherwise {mul2 r > r}
-- and there is no {r' :: Dist} such that {mul2 r > r' > r}.
mul2 :: Dist -> Dist
mul2 r@(Dist n) | n > 1     = Dist (n - 1)
                | otherwise = r

-- If {r == zero} then {div2 r == r}, otherwise {div2 r < r}
-- and there is no {r' :: Dist} such that {div2 r < r' < r}.
div2 :: Dist -> Dist
div2 r@(Dist n) | n > 0     = Dist (n + 1)
                | otherwise = r

-- Add two distances together.
plus :: Dist -> Dist -> Dist
plus = max
