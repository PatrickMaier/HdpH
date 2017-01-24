-- Mutable bit vectors in the ST monad.

{-# LANGUAGE BangPatterns  #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Test.HdpH.STBitVector
  ( -- * mutable bit set type
    STBitVector

    -- * construction
  , new

    -- * queries
  , support
  , test
  , unsafeTest
  , size
  , findMin

    -- * updates contents (support is fixed)
  , insert
  , unsafeInsert
  , remove
  , unsafeRemove
  , union
  , unionWith
  , intersect
  , intersectWith
  , shiftL
  , shiftR

    -- * immutable bit set type (incl membership test)
  , BitVector
  , elem

    -- * conversions mutable/immutable bit vectors
  , freeze
  , thaw
  , unsafeFreeze
  , unsafeThaw

    -- * conversions array/list representation of bit vectors
  , fromList
  , toList
  ) where

import Prelude hiding (elem)
import Control.DeepSeq (NFData(rnf))
import Control.Monad (foldM, when)
import Control.Monad.ST (ST)
import Data.Array.Base (unsafeAt, unsafeRead, unsafeWrite)
import Data.Array.IArray (listArray, (!), elems)
import qualified Data.Array.MArray as Array (freeze, thaw)
import Data.Array.ST (STUArray, newArray)
import Data.Array.Unboxed (UArray)
import qualified Data.Array.Unsafe as Array (unsafeFreeze, unsafeThaw)
import Data.Bits ((.&.), (.|.), complement, testBit, setBit,
                  unsafeShiftR, unsafeShiftL, countTrailingZeros, popCount,
                  finiteBitSize, zeroBits)
import Data.List (foldl', sort)
import Data.Serialize (Serialize)
import Data.Word (Word64)
import GHC.Generics (Generic)

{-
-- FOR TESTING
--
-- Replace unsafe read and writes with bounds-checking reads and writes
-- Must comment out 'import Data.Array.Base' when using this.
--
import Data.Array.IArray (IArray)
import Data.Array.MArray (MArray, readArray, writeArray)
unsafeAt :: (IArray a e) => a Int e -> Int -> e
unsafeAt = (!)
unsafeRead :: (MArray a e m) => a Int e -> Int -> m e
unsafeRead = readArray
unsafeWrite :: (MArray a e m) => a Int e -> Int -> e -> m ()
unsafeWrite = writeArray
-}

------------------------------------------------------------------------------
-- basic bit container, packing and index transformations

-- must be an instance of classes FiniteBits and UArray
type Block = Word64

blockWidth :: Int
blockWidth = finiteBitSize (zeroBits :: Block)

logBlockWidth :: Int
logBlockWidth = countTrailingZeros blockWidth

packBits :: [Int] -> Block
{-# INLINE packBits #-}
packBits = foldl' setBit zeroBits . filter (\ i -> 0 <= i && i < blockWidth)

-- Shows bits as string; least significant bit is to the left.
showBits :: Block -> String
showBits x = map (showBit . testBit x) [0 .. blockWidth - 1]
  where
    showBit b = if b then '1' else '0'

fromIndex :: Int -> (Int, Int)
{-# INLINE fromIndex #-}
fromIndex i = (blockIdx, bitIdx)
  where
    !blockIdx = i `unsafeShiftR` logBlockWidth
    !bitIdx   = i .&. (blockWidth - 1)

toIndex :: (Int, Int) -> Int
{-# INLINE toIndex #-}
toIndex (blockIdx, bitIdx) = i
  where
    !i = (blockIdx `unsafeShiftL` logBlockWidth) + bitIdx


------------------------------------------------------------------------------
-- mutable bit vector in ST thread s

data STBitVector s = STBV
                       {-# UNPACK #-} !Int      -- support
                       !(STUArray s Int Block)  -- mutable array of blocks

-- Invariant:
-- * The msb (most significant block) may only be partially filled.
--   Bits in that block beyond the most significant bit must be 0.
--   Operations that potentially break this invariant must re-establish it
--   by calling 'clearSpareBits'.

clearSpareBits :: STBitVector s -> ST s ()
{-# INLINE clearSpareBits #-}
clearSpareBits (STBV supp a) = when (supp > 0) $ do
  let (maxBlockIdx, _) = fromIndex (supp - 1)
  let (_, msb) = fromIndex (supp - 1)
  let maxBlockMask | msb + 1 == blockWidth = complement zeroBits
                   | otherwise             = (1 `unsafeShiftL` (msb + 1)) - 1
  x <- unsafeRead a maxBlockIdx
  unsafeWrite a maxBlockIdx (x .&. maxBlockMask)


------------------------------------------------------------------------------
-- construction; supp >= 0

new :: Int -> ST s (STBitVector s)
{-# INLINABLE new #-}
new supp = do
  let (maxBlockIdx, _) = fromIndex (supp - 1)
  a <- newArray (0, maxBlockIdx) zeroBits
  return (STBV supp a)


------------------------------------------------------------------------------
-- immutable bit vector

data BitVector = BV
                   {-# UNPACK #-} !Int  -- support
                   !(UArray Int Block)  -- array of blocks, backing support bits
                   deriving (Generic)

instance Show BitVector where
  show (BV supp a) =
    "[" ++ show supp ++ "]" ++ (take supp $ concat $ map showBits $ elems a)

instance NFData BitVector where
  rnf x = x `seq` ()

instance Serialize BitVector

elem :: Int -> BitVector -> Bool
{-# INLINABLE elem #-}
elem i (BV supp a) = 0 <= i && i < supp && testBit (a ! blockIdx) bitIdx
  where
    (blockIdx, bitIdx) = fromIndex i


------------------------------------------------------------------------------
-- safe, copying conversions between mutable and immutable bit vectors

freeze :: STBitVector s -> ST s BitVector
{-# INLINABLE freeze #-}
freeze (STBV supp a) = do
  a_frozen <- Array.freeze a
  return (BV supp a_frozen)

thaw :: BitVector -> ST s (STBitVector s)
{-# INLINABLE thaw #-}
thaw (BV supp a_frozen) = do
  a <- Array.thaw a_frozen
  return (STBV supp a)


------------------------------------------------------------------------------
-- unsafe, non-copying conversions between mutable and immutable bit sets;
-- beware of restrictions as on unsafe freezing and thawing arrays.

unsafeFreeze :: STBitVector s -> ST s BitVector
{-# INLINABLE unsafeFreeze #-}
unsafeFreeze (STBV supp a) = do
  a_frozen <- Array.unsafeFreeze a
  return (BV supp a_frozen)

unsafeThaw :: BitVector -> ST s (STBitVector s)
{-# INLINABLE unsafeThaw #-}
unsafeThaw (BV supp a_frozen) = do
  a <- Array.unsafeThaw a_frozen
  return (STBV supp a)


------------------------------------------------------------------------------
-- conversions between array and list representations

-- Returns a pair with the support and the elements of `bv` in ascending order.
toList :: BitVector -> (Int, [Int])
toList bv@(BV supp _) = (supp, filter (\ i -> elem i bv) [0 .. supp - 1])

-- The inverse of `toList`.
fromList :: (Int, [Int]) -> BitVector
fromList (supp, bits) = BV supp $ listArray (0, maxBlockIdx) packed_bits
  where
    (maxBlockIdx, _) = fromIndex (supp - 1)
    packed_bits = map packBits grouped_bits
    (grouped_bits,_) = foldr groupBits ([], rev_sorted_bits) [0 .. maxBlockIdx]
    rev_sorted_bits = reverse $ sort $ filter (\ i -> 0 <= i && i < supp) bits
    groupBits j (groups, bits') = (map (\ i -> i - base) group : groups, bits'')
      where
        (group, bits'') = span (>= base) bits'
        base = j * blockWidth


------------------------------------------------------------------------------
-- queries; 0 <= i < supp

support :: STBitVector s -> Int
{-# INLINABLE support #-}
support (STBV supp _) = supp

test :: STBitVector s -> Int -> ST s Bool
{-# INLINABLE test #-}
test bv@(STBV supp _) i | i < 0 || i >= supp = return False
                        | otherwise          = unsafeTest bv i

unsafeTest :: STBitVector s -> Int -> ST s Bool
{-# INLINABLE unsafeTest #-}
unsafeTest (STBV _ a) i = do
  let (blockIdx, bitIdx) = fromIndex i
  block <- unsafeRead a blockIdx
  return $! testBit block bitIdx

-- Returns the number of elements in the bit set; linear in supp.
size :: forall s . STBitVector s -> ST s Int
{-# INLINABLE size #-}
size (STBV supp a) = do
  let (maxBlockIdx, _) = fromIndex (supp - 1)
  foldM popCnt 0 [0 .. maxBlockIdx]
    where
      popCnt :: Int -> Int -> ST s Int
      popCnt count blockIdx = do
        x <- unsafeRead a blockIdx
        return $! count + popCount x

-- Returns the smallest i < supp in the bit set, or -1 if empty; linear in supp.
findMin :: forall s . STBitVector s -> ST s Int
findMin (STBV supp a) = go 0
  where
    (maxBlockIdx, _) = fromIndex (supp - 1)
    go :: Int -> ST s Int
    go blockIdx =
      if blockIdx > maxBlockIdx
        then return (-1)
        else do
          x <- unsafeRead a blockIdx
          if x == 0
            then go (blockIdx + 1)
            else return $! toIndex (blockIdx, countTrailingZeros x)


------------------------------------------------------------------------------
-- insertion and removal; 0 <= i < supp
-- NOTE: The unsafe functions don't check their arguments are within support
--       bounds; they may crash when accessing bits out of bounds.

insert :: Int -> STBitVector s -> ST s ()
{-# INLINABLE insert #-}
insert i bv@(STBV supp _) = when (0 <= i && i < supp) $ unsafeInsert i bv

unsafeInsert :: Int -> STBitVector s -> ST s ()
{-# INLINABLE unsafeInsert #-}
unsafeInsert i (STBV _ a) = do
  let (blockIdx, bitIdx) = fromIndex i
  block <- unsafeRead a blockIdx
  unsafeWrite a blockIdx $ unsafeSetBit block bitIdx

remove :: Int -> STBitVector s -> ST s ()
{-# INLINABLE remove #-}
remove i bv@(STBV supp _) = when (0 <= i && i < supp) $ unsafeRemove i bv

unsafeRemove :: Int -> STBitVector s -> ST s ()
{-# INLINABLE unsafeRemove #-}
unsafeRemove i (STBV _ a) = do
  let (blockIdx, bitIdx) = fromIndex i
  block <- unsafeRead a blockIdx
  unsafeWrite a blockIdx $ unsafeClearBit block bitIdx


------------------------------------------------------------------------------
-- union/intersection;
-- modifying the first argument but not the second;
-- the support of the second argument need not match the first

unionWith :: forall s . STBitVector s -> BitVector -> ST s ()
{-# INLINABLE unionWith #-}
unionWith bv@(STBV supp a) (BV supp2 a2) = do
  let (maxBlockIdx, _)  = fromIndex (supp - 1)
  let (maxBlockIdx2, _) = fromIndex (supp2 - 1)
  mapM_ unionAt [0 .. min maxBlockIdx maxBlockIdx2]
  -- clear extraneous bits (that might have appeared due to support mismatch)
  clearSpareBits bv
    where
      unionAt :: Int -> ST s ()
      unionAt blockIdx = do
        x <- unsafeRead a blockIdx
        let y = unsafeAt a2 blockIdx
        unsafeWrite a blockIdx $ x .|. y

union :: forall s . STBitVector s -> STBitVector s -> ST s ()
{-# INLINABLE union #-}
union bv bv2 = unionWith bv =<< unsafeFreeze bv2

intersectWith :: forall s . STBitVector s -> BitVector -> ST s ()
{-# INLINABLE intersectWith #-}
intersectWith bv@(STBV supp a) (BV supp2 a2) = do
  let (maxBlockIdx, _)  = fromIndex (supp - 1)
  let (maxBlockIdx2, _) = fromIndex (supp2 - 1)
  mapM_ intersectAt [0 .. min maxBlockIdx maxBlockIdx2]
  -- clear blocks (that might not have been cleared due to support mismatch)
  clearBlocksR bv (min maxBlockIdx maxBlockIdx2)
    where
      intersectAt :: Int -> ST s ()
      intersectAt blockIdx = do
        x <- unsafeRead a blockIdx
        let y = unsafeAt a2 blockIdx
        unsafeWrite a blockIdx $ x .&. y

intersect :: forall s . STBitVector s -> STBitVector s -> ST s ()
{-# INLINABLE intersect #-}
intersect bv bv2 = intersectWith bv =<< unsafeFreeze bv2


------------------------------------------------------------------------------
-- shiftL/shiftR; i >= 0
--
-- Orientation:
-- * Geometrically, the msb (most significant block) is the rightmost
--   and the lsb the leftmost. Thus, shifting to the right shifts towards
--   the msb, and shifting to the left towards the lsb.
-- * Counter-intuitively, the orientation of blocks is reversed:
--   the least significant bit is the rightmost. Thus, shifting a bitvector
--   to the right involves left shifts at the block level, and vice versa.

shiftL :: forall s . STBitVector s -> Int -> ST s ()
shiftL bv@(STBV supp a) i = do
  let (maxBlockIdx, _)   = fromIndex (supp - 1)
  let (blockOfs, bitOfs) = fromIndex i
  let bitOfs_inv = blockWidth - bitOfs
  let shiftBlocks :: Int -> ST s ()
      shiftBlocks blockIdx = do
        x <- unsafeRead a (blockIdx + blockOfs)
        unsafeWrite a blockIdx x
  let shiftBlocksWithCarry :: Int -> ST s ()
      shiftBlocksWithCarry blockIdx = do
        carry <- if blockIdx + blockOfs == maxBlockIdx
                   then return zeroBits
                   else do xc <- unsafeRead a (blockIdx + blockOfs + 1)
                           return $ xc `unsafeShiftL` bitOfs_inv
        x <- unsafeRead a (blockIdx + blockOfs)
        unsafeWrite a blockIdx $ (x `unsafeShiftR` bitOfs) .|. carry
  -- shift left, starting "from the left"
  if bitOfs == 0
    then mapM_ shiftBlocks          [0 .. maxBlockIdx - blockOfs]
    else mapM_ shiftBlocksWithCarry [0 .. maxBlockIdx - blockOfs]
  -- zero remaining blocks "to the right"
  clearBlocksR bv (maxBlockIdx - blockOfs)

shiftR :: forall s . STBitVector s -> Int -> ST s ()
shiftR bv@(STBV supp a) i = do
  let (maxBlockIdx, _)   = fromIndex (supp - 1)
  let (blockOfs, bitOfs) = fromIndex i
  let bitOfs_inv = blockWidth - bitOfs
  let shiftBlocks :: Int -> ST s ()
      shiftBlocks blockIdx = do
        x <- unsafeRead a (blockIdx - blockOfs)
        unsafeWrite a blockIdx x
  let shiftBlocksWithCarry :: Int -> ST s ()
      shiftBlocksWithCarry blockIdx = do
        carry <- if blockIdx - blockOfs == 0
                   then return zeroBits
                   else do xc <- unsafeRead a (blockIdx - blockOfs - 1)
                           return $ xc `unsafeShiftR` bitOfs_inv
        x <- unsafeRead a (blockIdx - blockOfs)
        unsafeWrite a blockIdx $ (x `unsafeShiftL` bitOfs) .|. carry
  -- shift right, starting "from the right"
  if bitOfs == 0
    then mapM_ shiftBlocks          [maxBlockIdx, maxBlockIdx - 1 .. blockOfs]
    else mapM_ shiftBlocksWithCarry [maxBlockIdx, maxBlockIdx - 1 .. blockOfs]
  -- zero remaining blocks "to the left"
  clearBlocksL bv blockOfs
  -- clear extraneous bits (that may have been shifted beyond support)
  clearSpareBits bv


------------------------------------------------------------------------------
-- Auxiliary: clear/set bit without the usual bounds check

unsafeSetBit :: Block -> Int -> Block
{-# INLINE unsafeSetBit #-}
unsafeSetBit w i = w .|. (1 `unsafeShiftL` i)

unsafeClearBit :: Block -> Int -> Block
{-# INLINE unsafeClearBit #-}
unsafeClearBit w i = w .&. complement (1 `unsafeShiftL` i)


------------------------------------------------------------------------------
-- Auxiliary: clear blocks to the left (< pivotIdx) or right (> pivotIdx)

clearBlocksL :: STBitVector s -> Int -> ST s ()
{-# INLINE clearBlocksL #-}
clearBlocksL (STBV supp a) pivotIdx = do
  let (maxBlockIdx, _) = fromIndex (supp - 1)
  let idxs = [0 .. min (pivotIdx - 1) maxBlockIdx]
  mapM_ (\ blockIdx -> unsafeWrite a blockIdx zeroBits) idxs

clearBlocksR :: STBitVector s -> Int -> ST s ()
{-# INLINE clearBlocksR #-}
clearBlocksR (STBV supp a) pivotIdx = do
  let (maxBlockIdx, _) = fromIndex (supp - 1)
  let idxs = [max 0 (pivotIdx + 1) .. maxBlockIdx]
  mapM_ (\ blockIdx -> unsafeWrite a blockIdx zeroBits) idxs
