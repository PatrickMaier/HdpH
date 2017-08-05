-- Finite Incidence Geometry in 2 dimensions

{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}

-- module Main where  -- Uncomment for TESTING
module Test.HdpH.FinInG2
  ( -- * projective space
    Space
  , isSpace    -- predicate
  , fromPoints -- constructors
  , fromLines
  , points     -- accessors
  , lines
  , pointsOf
  , linesOf
  , span
  , meet
  , thruPoint  -- queries
  , onLine
  , collinear

    -- * points
  , Point
  , isPoint    -- predicate
  , PointSet   -- instance of Set
  , Points     -- instance of VecSet
  , Span       -- instance of Mat

    -- * lines
  , Line
  , isLine     -- predicate
  , LineSet    -- instance of Set
  , Lines      -- instance of VecSet
  , Meet       -- instance of Mat

    -- * small sets of 16-bit words
  , Set(
      newSet
    , sizeSet
    , elemsSet
    , elemSet
    , andSet)

    -- * 1-d maps of sets of 16-bit words, indexed by 16-bit words
  , VecSet(
      newVecSet
    , sizeVecSet
    , indicesVecSet
    , atVecSet
    , incidentVecSet)

    -- * square 2-d maps of 16-bit words, indexed by 16-bit words
  , Mat(
      newMat
    , rankMat
    , atMat)

  -- * parsing
  , readSpace
  , parseSpace
  , parseLine
  , parsePoint
  , parseListOfPoint
  , parseSortedListOfPoint
  ) where

import Prelude hiding (span, lines)
import Control.DeepSeq (NFData(rnf))
import Data.Array.Base (unsafeAt)
import Data.Array.IArray (Array, accumArray, listArray, bounds, elems, (!))
import Data.Array.Unboxed (UArray)
import Data.Function (on)
import Data.List (sort, group, groupBy)
import qualified Data.List (lines)
import Data.Word (Word16)


------------------------------------------------------------------------------
-- space

data Space = Space Lines Points Span Meet deriving (Show)

-- Predicate to check regularity and exclude some trivial cases
isSpace :: Space -> Bool
isSpace space =
  -- there are at least 4 points and 4 lines
  length (points space) >= 4 && length (lines space) >= 4 &&
  -- every line has n points and every point is on m lines
  and [sizeSet (pointsOf space l) == n | l <- lines  space] &&
  and [sizeSet (linesOf  space a) == m | a <- points space]
    where
      n = sizeSet $ pointsOf space (head $ lines  space)
      m = sizeSet $ linesOf  space (head $ points space)

instance NFData Space where
  rnf (Space lns pts sp mt) =
    rnf lns `seq` rnf pts `seq` rnf sp `seq` rnf mt `seq` ()

fromPoints :: Points -> Space
fromPoints pts = Space lns pts (makeSpan lns) (makeMeet pts)
                   where lns = makeLines pts

fromLines :: Lines -> Space
fromLines lns = Space lns pts (makeSpan lns) (makeMeet pts)
                   where pts = makePoints lns

points :: Space -> [Point]
points (Space lns _ _ _) = indicesVecSet lns

pointsOf :: Space -> Line -> PointSet
pointsOf (Space _ pts _ _) = atVecSet pts

lines :: Space -> [Line]
lines (Space _ pts _ _) = indicesVecSet pts

linesOf :: Space -> Point -> LineSet
linesOf (Space lns _ _ _) = atVecSet lns

-- cheap on points; not so cheap on lines
thruPoint :: Space -> Point -> Line -> Bool
thruPoint (Space lns _ _ _) a l = incidentVecSet lns a l

-- cheap on lines; not so cheap on points
onLine :: Space -> Line -> Point -> Bool
onLine (Space _ pts _ _) l a = incidentVecSet pts l a

span :: Space -> Point -> Point -> Maybe Line
span (Space _ _ sp _) = atMat sp

meet :: Space -> Line -> Line -> Maybe Point
meet (Space _ _ _ mt) = atMat mt

-- For efficiency reasons c should be smaller than a and b in the Ord order.
collinear :: Space -> Point -> Point -> Point -> Bool
collinear space a b c = case span space a b of
                          Just l  -> onLine space l c
                          Nothing -> True


------------------------------------------------------------------------------
-- parsing a plane as a set of lines

readSpace :: String -> IO Space
readSpace filename = do
  input <- if null filename then getContents else readFile filename
  return $ parseSpace $ Data.List.lines input

parseSpace :: [String] -> Space
parseSpace =
  check . fromPoints . newVecSet . map parseLine .
  filter (\ s -> case s of { 'l':_ -> True; _ -> False })
    where
      check space | isSpace space = space
                  | otherwise     = error "parseSpace: check failed"

parseLine :: String -> PointSet
parseLine ('l':s) = newSet $ parseSortedListOfPoint s
parseLine s       = error $ "parseLine: wrong prefix on " ++ show s

parseListOfPoint :: String -> [Point]
parseListOfPoint = map parsePoint . words

parseSortedListOfPoint :: String -> [Point]
parseSortedListOfPoint s =
  if as == map head (group $ sort as)
    then as
    else error $ "parseSortedListOfPoint: points not sorted on " ++ show s
      where as = parseListOfPoint s

parsePoint :: String -> Point
parsePoint s =
  if 0 < k && k <= fromIntegral (maxBound :: Word16)
    then Point (fromIntegral k)
    else error $ "parsePoint: out of range " ++ show s
      where
        k = case reads s of
              [(k, "")] -> k :: Integer
              _         -> error $ "parsePoint: no parse on " ++ show s


------------------------------------------------------------------------------
-- points and small sets of points
--
-- Points are represented as postive integers.

newtype Point = Point Word16 deriving (Eq, Ord, NFData, Enum)

isPoint :: Point -> Bool
isPoint (Point a) = a > 0

instance Show Point where
    show (Point a) = show a

instance Read Point where
    readsPrec d = filter (isPoint . fst) .
                  map (\ (a, rest) -> (Point a, rest)) .
                  readsPrec d


newtype PointSet = PointSet Word16Set deriving (NFData, Show)

instance Set PointSet where
  type Elem PointSet = Point
  newSet as = PointSet $ newWord16Set [a | Point a <- as]
  sizeSet (PointSet as) = sizeWord16Set as
  elemsSet (PointSet as) = map Point $ elemsWord16Set as
  elemSet (Point a) (PointSet as) = elemWord16Set a as
  PointSet as `andSet` PointSet bs = fmap Point $ andWord16Set as bs


------------------------------------------------------------------------------
-- lines and small sets of lines
--
-- Lines are represented as postive integers.

newtype Line = Line Word16 deriving (Eq, Ord, NFData, Enum)

isLine :: Line -> Bool
isLine (Line l) = l > 0

instance Show Line where
    show (Line l) = show l

instance Read Line where
    readsPrec d = filter (isLine . fst) .
                  map (\ (l, rest) -> (Line l, rest)) .
                  readsPrec d


newtype LineSet = LineSet Word16Set deriving (NFData, Show)

instance Set LineSet where
  type Elem LineSet = Line
  newSet ls = LineSet $ newWord16Set [l | Line l <- ls]
  sizeSet (LineSet ls) = sizeWord16Set ls
  elemsSet (LineSet ls) = map Line $ elemsWord16Set ls
  elemSet (Line l) (LineSet ls) = elemWord16Set l ls
  LineSet ls `andSet` LineSet ks = fmap Line $ andWord16Set ls ks


------------------------------------------------------------------------------
-- mapping points to the set of all lines they are on

newtype Lines = Lines VecWord16Set deriving (NFData, Show)

instance VecSet Lines where
  type Idx Lines = Point
  type Val Lines = LineSet
  newVecSet lss = Lines $ newVecWord16Set [ls | LineSet ls <- lss]
  sizeVecSet (Lines v) = sizeVecWord16Set v
  indicesVecSet (Lines v) = map Point $ indicesVecWord16Set v
  atVecSet (Lines v) (Point a) = LineSet $ atVecWord16Set v a
  incidentVecSet (Lines v) (Point a) (Line l) = incidentVecWord16Set v a l

-- Construct Lines from Points;
-- assumes the set of points occuring in Points is dense (ie. there is no gap)
makeLines :: Points -> Lines
makeLines pts = newVecSet lss
  where
    als = [(a,l) | l <- indicesVecSet pts, a <- elemsSet $ atVecSet pts l]
    lss = map (newSet . map snd) $ groupBy ((==) `on` fst) $ sort als


------------------------------------------------------------------------------
-- mapping lines to the set of all their points

newtype Points = Points VecWord16Set deriving (NFData, Show)

instance VecSet Points where
  type Idx Points = Line
  type Val Points = PointSet
  newVecSet ass = Points $ newVecWord16Set [as | PointSet as <- ass]
  sizeVecSet (Points v) = sizeVecWord16Set v
  indicesVecSet (Points v) = map Line $ indicesVecWord16Set v
  atVecSet (Points v) (Line l) = PointSet $ atVecWord16Set v l
  incidentVecSet (Points v) (Line l) (Point a) = incidentVecWord16Set v l a

-- Construct Points from Lines;
-- assumes the set of lines occuring in Lines is dense (ie. there is no gap)
makePoints :: Lines -> Points
makePoints lns = newVecSet ass
  where
    las = [(l,a) | a <- indicesVecSet lns, l <- elemsSet $ atVecSet lns a]
    ass = map (newSet . map snd) $ groupBy ((==) `on` fst) $ sort las


------------------------------------------------------------------------------
-- mapping 2 points to the line they span

newtype Span = Span MatWord16 deriving (NFData, Show)

instance Mat Span where
  type Ix Span = Point
  type Im Span = Line
  newMat rank abls =
    Span $ newMatWord16 rank [(a,b,l) | (Point a, Point b, Line l) <- abls]
  rankMat (Span m) = rankMatWord16 m
  atMat (Span m) (Point a) (Point b) = fmap Line $ atMatWord16 m a b

makeSpan :: Lines -> Span
makeSpan lines = newMat rank abls
  where
    as = indicesVecSet lines
    rank = length as
    abls = [(a,b,l) | a <- as, b <- as, a /= b,
                      Just l <- [atVecSet lines a `andSet` atVecSet lines b]]


------------------------------------------------------------------------------
-- mapping 2 lines to the point where they meet

newtype Meet = Meet MatWord16 deriving (NFData, Show)

instance Mat Meet where
  type Ix Meet = Line
  type Im Meet = Point
  newMat rank lkas =
    Meet $ newMatWord16 rank [(l,k,a) | (Line l, Line k, Point a) <- lkas]
  rankMat (Meet m) = rankMatWord16 m
  atMat (Meet m) (Line l) (Line k) = fmap Point $ atMatWord16 m l k

makeMeet :: Points -> Meet
makeMeet points = newMat rank abls
  where
    ls = indicesVecSet points
    rank = length ls
    abls = [(l,k,a) | l <- ls, k <- ls, l /= k,
                      Just a <- [atVecSet points l `andSet` atVecSet points k]]


------------------------------------------------------------------------------
-- internal: small sets of 16-bit words

-- type class for small sets
class Set s where
  type Elem s :: *
  newSet :: (Ord (Elem s)) => [Elem s] -> s
  sizeSet :: s -> Int
  elemsSet :: s -> [Elem s]
  elemSet :: (Ord (Elem s)) => Elem s -> s -> Bool
  andSet :: (Ord (Elem s)) => s -> s -> Maybe (Elem s)

-- A small set of 16-bit words is represented as a 0-based array of Word16.
-- Entries are stored in ascending order; 0 is not allowed as an entry.
type Word16Set = UArray Int Word16

-- trivial instance for Word16Set (unboxed arrays are hyperstrict)
instance NFData Word16Set where
  rnf s = s `seq` ()

-- construction
newWord16Set :: [Word16] -> Word16Set
newWord16Set xs = listArray (0, length xs' - 1) xs'
                    where xs' = map head $ group $ sort $ filter (> 0) xs

-- size query
sizeWord16Set :: Word16Set -> Int
{-# INLINE sizeWord16Set #-}
sizeWord16Set s = snd (bounds s) + 1

-- list conversion
elemsWord16Set :: Word16Set -> [Word16]
{-# INLINE elemsWord16Set #-}
elemsWord16Set s = elems s

-- membership test
elemWord16Set :: Word16 -> Word16Set -> Bool
{-# INLINE elemWord16Set #-}
elemWord16Set x s = go 0
  where
    n = sizeWord16Set s
    go i | i == n    = False
         | otherwise = case compare (unsafeAt s i) x of
                         LT -> go (i + 1)
                         GT -> False
                         EQ -> True

-- intersection witness
andWord16Set :: Word16Set -> Word16Set -> Maybe Word16
{-# INLINE andWord16Set #-}
andWord16Set s1 s2 = go 0 0
  where
    n1 = sizeWord16Set s1
    n2 = sizeWord16Set s2
    go i1 i2 | i1 == n1  = Nothing
             | i2 == n2  = Nothing
             | otherwise = case compare (unsafeAt s1 i1) (unsafeAt s2 i2) of
                             LT -> go (i1 + 1) i2
                             GT -> go i1 (i2 + 1)
                             EQ -> Just (unsafeAt s1 i1)


------------------------------------------------------------------------------
-- internal: 1-d maps of sets of 16-bit words, indexed by 16-bit words

-- type class for 1-d maps of sets
class VecSet v where
  type Idx v :: *
  type Val v :: *
  newVecSet :: [Val v] -> v
  sizeVecSet :: v -> Int
  indicesVecSet :: v -> [Idx v]
  atVecSet :: v -> Idx v -> Val v
  incidentVecSet :: (Ord (Elem (Val v))) => v -> Idx v -> Elem (Val v) -> Bool

-- A vector of small sets of 16-bit words is represented as a 0-based array
-- of Word16Set. Entry 0 is ignored.
type VecWord16Set = Array Word16 Word16Set

-- construction
newVecWord16Set :: [Word16Set] -> VecWord16Set
newVecWord16Set xs = listArray (0, fromIntegral $ length xs) (x0:xs)
                       where x0 = newWord16Set []

-- size query
sizeVecWord16Set :: VecWord16Set -> Int
{-# INLINE sizeVecWord16Set #-}
sizeVecWord16Set v = fromIntegral $ snd (bounds v)

-- indices
indicesVecWord16Set :: VecWord16Set -> [Word16]
{-# INLINE indicesVecWord16Set #-}
indicesVecWord16Set v = [1 .. snd (bounds v)]

-- lookup (unsafe; no bounds check)
atVecWord16Set :: VecWord16Set -> Word16 -> Word16Set
{-# INLINE atVecWord16Set #-}
atVecWord16Set v i = unsafeAt v (fromIntegral i)  -- fast, unsafe version
-- atVecWord16Set = (!)                              -- safe version for test

-- incidence query
incidentVecWord16Set :: VecWord16Set -> Word16 -> Word16 -> Bool
{-# INLINE incidentVecWord16Set #-}
incidentVecWord16Set v i j = elemWord16Set j $ atVecWord16Set v i


------------------------------------------------------------------------------
-- internal: square 2-d maps of 16-bit words, indexed by 16-bit words

-- type class for square 2-d maps
class Mat m where
  type Ix m :: *
  type Im m :: *
  newMat :: Int -> [(Ix m, Ix m, Im m)] -> m
  rankMat :: m -> Int
  atMat :: m -> Ix m -> Ix m -> Maybe (Im m)

-- A square matrix of 16-bit words is represented as a 2-d array of Word16.
-- Indices in both dimensions are positive.
-- Entries are positive; entries of values 0 are ignored.
type MatWord16 = UArray (Word16, Word16) Word16

-- trivial instance for MatWord16 (unboxed arrays are hyperstrict)
instance NFData MatWord16 where
  rnf m = m `seq` ()

-- construction
newMatWord16 :: Int -> [(Word16, Word16, Word16)] -> MatWord16
newMatWord16 rank ijks =
  accumArray (flip const) 0 ((1,1), (n,n)) [((i,j), k) | (i,j,k) <- ijks]
    where n = fromIntegral rank

-- rank query
rankMatWord16 :: MatWord16 -> Int
{-# INLINE rankMatWord16 #-}
rankMatWord16 = fromIntegral . fst . snd . bounds

-- lookup
atMatWord16 :: MatWord16 -> Word16 -> Word16 -> Maybe Word16
{-# INLINE atMatWord16 #-}
atMatWord16 m i j = case m ! (i,j) of  -- can't use `unsafeAt` here
                      0 -> Nothing
                      k -> Just k


------------------------------------------------------------------------------
-- TESTING

{-
main :: IO ()
main = do
  sp <- readSpace ""
  putStrLn $ show $ rnf sp
-}
