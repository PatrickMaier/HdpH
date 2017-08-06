-- Enumerating arcs in PG(2,q), without symmetry breaking.
-- Refers to Open Problem 4.10(f) in [1].
--
-- Author: Patrick Maier
--
-- References:
-- [1] Hirschfeld, Thas. Open Problems in Finite Projective Spaces. 2015.
--
---------------------------------------------------------------------------

{-# LANGUAGE TemplateHaskell #-}  -- req'd for mkClosure, etc
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE BangPatterns #-}

module Main where

import Prelude hiding (lines, span)
import Control.DeepSeq (NFData(rnf), ($!!))
import Control.Exception (evaluate)
import Control.Monad (when, zipWithM_)
import qualified Data.IntSet as Set
import qualified Data.IntMap.Strict as Map
import Data.List (foldl', isPrefixOf, sortOn)
import qualified Data.List as List (lines)
import Data.Maybe (fromJust)
import Data.Ord (Down(Down))
import Data.Time.Clock (NominalDiffTime, diffUTCTime, getCurrentTime)
import System.Environment (getArgs)
import System.IO (stdout, stderr, hSetBuffering, BufferMode(..))
import System.Random (mkStdGen, setStdGen)

import Control.Parallel.HdpH
       (RTSConf(..), defaultRTSConf, updateConf,
        Par, runParIO, spark, new, get, glob, rput, GIVar,
        Thunk(Thunk), Closure, unClosure, mkClosure,
        static, StaticDecl, declare, register)
import qualified Control.Parallel.HdpH as HdpH (declareStatic)
import Control.Parallel.HdpH.Dist (one)

import Test.HdpH.FinInG2
       (Space, points, pointsOf, span, readSpace,
        Point, parseListOfPoint, parseSortedListOfPoint, elemsSet)


---------------------------------------------------------------------------
-- sets of points represented as IntSets

type SetOfPoint = Set.IntSet

toAscListP :: SetOfPoint -> [Point]
toAscListP = map toEnum . Set.toAscList

fromListP :: [Point] -> SetOfPoint
fromListP = Set.fromList . map fromEnum

-- fromAscListP :: [Point] -> SetOfPoint
-- fromAscListP = Set.fromAscList . map fromEnum

nullP :: SetOfPoint -> Bool
nullP = Set.null

sizeP :: SetOfPoint -> Int
sizeP = Set.size

memberP :: Point -> SetOfPoint -> Bool
memberP a = Set.member (fromEnum a)

notMemberP :: Point -> SetOfPoint -> Bool
notMemberP a = Set.notMember (fromEnum a)

emptyP :: SetOfPoint
emptyP = Set.empty

insertP :: Point -> SetOfPoint -> SetOfPoint
insertP a = Set.insert (fromEnum a)

deleteP :: Point -> SetOfPoint -> SetOfPoint
deleteP a = Set.delete (fromEnum a)

-- intersectionP :: SetOfPoint -> SetOfPoint -> SetOfPoint
-- intersectionP = Set.intersection

-- unionP :: SetOfPoint -> SetOfPoint -> SetOfPoint
-- unionP = Set.union

-- differenceP :: SetOfPoint -> SetOfPoint -> SetOfPoint
-- differenceP = Set.difference


---------------------------------------------------------------------------
-- predicates about arcs

-- Given an arc 'arc' and a 'c' not in 'arc', return 'True' iff 'arc'
-- can be extended with 'c' (i.e. 'arc `union` {b}' is still an arc);
-- 'as' is a set representation of the points in 'arc'.
isNoCandidateForArc :: Space -> [Point] -> SetOfPoint -> Point -> Bool
isNoCandidateForArc sp arc as c =
  -- `c` is not a candidate iff some line thru `c` and some point `a` in `arc`
  -- also meets another point `b` in `arc`
  or [b `memberP` as |
      a <- arc, b <- elemsSet $ pointsOf sp $ fromJust $ span sp c a, b /= a]
     -- NOTE: Could shorten list by adding filter `b /= c`;
     --       however, this appears to result in less efficient code.


-- Return 'True' iff arc 'arc' is complete (i.e. maximal wrt set inclusion).
isCompleteArc :: Space -> [Point] -> Bool
isCompleteArc sp arc =
  and [isNoCandidateForArc sp arc as c | c <- points sp, c `notMemberP` as]
    where as = fromListP arc


---------------------------------------------------------------------------
-- search tree

-- Search tree node
data Node = Node {
              space       :: !Space,      -- ambient space
              k           :: !Int,        -- target arc size
              arc         :: ![Point],    -- current arc
              nArc        :: !Int,        -- size of current arc
              candidates  :: !SetOfPoint, -- candidates for extension of arc
              bound       :: !Int }       -- upper bound on arc of candidates

instance NFData Node where
  rnf node = rnf (space node) `seq` rnf (k node) `seq`
             rnf (arc node) `seq` rnf (nArc node) `seq`
             rnf (candidates node) `seq` rnf (bound node) `seq` ()


-- 'True' iff node represents a k-arc
final :: Node -> Bool
final node = nArc node == k node


-- 'True' iff node can possibly be extended to a k-arc
feasible :: Node -> Bool
feasible node = nArc node + bound node >= k node


-- Generate root node of the search tree
mkRoot :: Space -> Int -> Maybe ([Point], SetOfPoint) -> Node
mkRoot sp desired_k maybe_root = let
    node0 = Node { space = sp, k = desired_k, arc = [], nArc = 0,
                   candidates = emptyP, bound = 0 }
  in case maybe_root of
       Nothing      -> node0 { candidates = fromListP $ points sp,
                               bound = length $ points sp }
       Just (as,cs) -> node0 { arc = reverse as, nArc = length as,
                               candidates = cs, bound = sizeP cs }


-- Note: `c` is not in `cands` and not in `arc`.
shrinkCandsOnExtArc :: Space -> [Point] -> Point -> SetOfPoint -> SetOfPoint
shrinkCandsOnExtArc sp arc c cands =
  -- remove all points `b` on lines thru `c` and some point `a` in `arc`
  foldl' (flip deleteP) cands
  [b | a <- arc, b <- elemsSet $ pointsOf sp $ fromJust $ span sp c a]
  -- NOTE: Could shorten list by adding filters `b /= a` and `b /= c`;
  --       however, this results in less efficient code.


generate :: Node -> [Node]
generate parent =
  concat $ zipWith mkFeasibleNode (take (n_cs - k parent + n_as + 1) cs) cands
  -- NOTE: Dropping last `gap` candidates because remaining candidate sets would
  --       have less than `gap` elements, where `gap = k parent - (n_as + 1)`.
    where
      sp    = space parent
      as    = arc parent
      n_as  = nArc parent
      cs    = toAscListP $ candidates parent
      n_cs  = sizeP $ candidates parent
      cands = tail $ scanr insertP emptyP cs
      mkFeasibleNode :: Point -> SetOfPoint -> [Node]
      mkFeasibleNode c cands =
        if feasible child then [child] else []
          where
            cands' = shrinkCandsOnExtArc sp as c cands
            child = parent { arc  = c:as,
                             nArc = n_as + 1,
                             candidates = cands',
                             bound = sizeP cands' }


-- Options governing counting of arcs
type Opts = ()


-- Returns number of complete (1st component) and incomplete (2nd component)
-- 'k'-arcs below node 'current', and the call/depth histogram (3rd component).
countArcs :: Opts -> Node -> (Int, Int, [Int])
countArcs opts current =
  if final current
    then if complete
           then (1,0,[1])
           else (0,1,[1])
    else reduce $ map (countArcs opts) $ generate current
      where
        complete = nullP (candidates current) &&
                   isCompleteArc (space current) (arc current)
        reduce :: [(Int, Int, [Int])] -> (Int, Int, [Int])
        reduce xyzs = go 0 0 [] xyzs
          where
            go !sxs !sys !szs xyzs =
              case xyzs of
               (x,y,z):xyzs' -> go (sxs + x) (sys + y) (szs +* z) xyzs'
               []            -> (sxs, sys, 1:szs)


-- sum of call/depth histograms
(+*) :: [Int] -> [Int] -> [Int]
xs     +* []     = xs
[]     +* ys     = ys
(x:xs) +* (y:ys) = z:zs where { !z = x + y; !zs = xs +* ys }


---------------------------------------------------------------------------
-- parse roots file

-- Reads the 'i'-th root (arc + set of candidates) from file 'rfile'.
-- If 'i < 0', read the '(-i)'-th from the back.
readRoot :: RootsFile -> Root -> IO (Maybe ([Point], SetOfPoint))
readRoot ""    _ = return Nothing
readRoot _     0 = return Nothing
readRoot rfile i = do
  roots <- fmap (parseArcs . List.lines) $ readFile rfile
  if abs i > length roots
    then return Nothing
    else if i > 0
      then return $ Just (roots !! (i - 1))
      else return $ Just (roots !! (length roots + i))

parseArcs :: [String] ->  [([Point], SetOfPoint)]
parseArcs (('a':s):input) = parseCandidates (parseListOfPoint s) input
parseArcs (('r':_):_)     = error "parseArcs: no match"
parseArcs (_:input)       = parseArcs input
parseArcs []              = []

parseCandidates :: [Point] -> [String] -> [([Point], SetOfPoint)]
parseCandidates arc (('r':s):input) = (arc, cands) : parseArcs input
  where cands = (fromListP $ parseSortedListOfPoint s)
parseCandidates _   (('a':_):_)     = error $ "parseCandidates: no match"
parseCandidates arc (_:input)       = parseCandidates arc input
parseCandidates _   []              = error $ "parseCandidates: EOF"


---------------------------------------------------------------------------
-- main, auxiliaries, argument parsing, etc.

-- time an IO action
timeIO :: IO a -> IO (a, NominalDiffTime)
timeIO action = do t0 <- getCurrentTime
                   x <- action
                   t1 <- getCurrentTime
                   return (x, diffUTCTime t1 t0)


-- initialize random number generator
initrand :: Int -> IO ()
initrand seed = do
  when (seed /= 0) $ do
    setStdGen (mkStdGen seed)


-- Option: version to execute; default: V0 (sequential)
data Version = V0 | V1 deriving (Eq, Ord, Show)

dispVersion :: Version -> String
dispVersion ver = " " ++ show ver

parseVersion :: String -> Maybe Version
parseVersion "V0" = Just V0
parseVersion "V1" = Just V1
parseVersion _    = Nothing


-- Option: file to read ambient space from; default: "" (read from stdin)
type SpaceFile = String

dispSpaceFile :: SpaceFile -> String
dispSpaceFile sfile = " -space=" ++ sfile

parseSpaceFile :: String -> Maybe SpaceFile
parseSpaceFile s | isPrefixOf "-space=" s = Just $ drop (length "-space=") s
                 | otherwise              = Nothing


-- Option: target arc size; default: 0
type K = Int

dispK :: K -> String
dispK k = " -k" ++ show k

parseK :: String -> Maybe K
parseK s | isPrefixOf "-k" s = case reads $ drop (length "-k") s of
                                 [(k, "")] -> Just k
                                 _         -> Nothing
         | otherwise         = Nothing


-- Option: file with arcs and candidates for root node; default: "" (no file)
type RootsFile = String

dispRootsFile :: RootsFile -> String
dispRootsFile ""    = ""
dispRootsFile rfile = " -roots=" ++ rfile

parseRootsFile :: String -> Maybe RootsFile
parseRootsFile s | isPrefixOf "-roots=" s = Just $ drop (length "-roots=") s
                 | otherwise              = Nothing


-- Option: root selection, pos int; default: -1 (select last root)
type Root = Int

dispRoot :: Root -> String
dispRoot 0 = ""
dispRoot r = " -r" ++ show r

parseRoot :: String -> Maybe Root
parseRoot s | isPrefixOf "-r" s = case reads $ drop (length "-r") s of
                                    [(r, "")] -> Just r
                                    _         -> Nothing
            | otherwise         = Nothing


-- Option: upper bound on size of candidates for seq exec; default: 0 (no bound)
type SeqSize = Int

dispSeqSize :: SeqSize -> String
dispSeqSize u = " -s" ++ show u

parseSeqSize :: String -> Maybe SeqSize
parseSeqSize s | isPrefixOf "-s" s = case reads $ drop (length "-s") s of
                                       [(u, "")] -> Just u
                                       _         -> Nothing
               | otherwise         = Nothing


parseArgs :: [String] -> (Version, SpaceFile, K, RootsFile, Root, SeqSize)
parseArgs = foldl parse (V0, "", 0, "", -1, 0)
  where
    parse (ver, sfile, k, rfile, root, seqsz) s =
      maybe id upd1a (parseVersion s) $
      maybe id upd1b (parseSpaceFile s) $
      maybe id upd1c (parseK s) $
      maybe id upd1d (parseRootsFile s) $
      maybe id upd1e (parseRoot s) $
      maybe id upd1f (parseSeqSize s) $
      (ver, sfile, k, rfile, root, seqsz)
    upd1a z (ya,yb,yc,yd,ye,yf) = ( z,yb,yc,yd,ye,yf)
    upd1b z (ya,yb,yc,yd,ye,yf) = (ya, z,yc,yd,ye,yf)
    upd1c z (ya,yb,yc,yd,ye,yf) = (ya,yb, z,yd,ye,yf)
    upd1d z (ya,yb,yc,yd,ye,yf) = (ya,yb,yc, z,ye,yf)
    upd1e z (ya,yb,yc,yd,ye,yf) = (ya,yb,yc,yd, z,yf)
    upd1f z (ya,yb,yc,yd,ye,yf) = (ya,yb,yc,yd,ye, z)


printResults :: (Show a1, Show a2, Show a3, Show a4, Show a5, Show a6,
                 Show a7, Show a8, Show a9, Show a10, Show a11, Show a12)
             => (a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12) -> IO ()
printResults
  (v, seqsz, sfile, k, rfile, root, cmpl, incmpl, t, hist, tasks, leaves) =
  zipWithM_ printTagged tags stuff
    where printTagged tag val = putStrLn (tag ++ val)
          tags  = ["VERSION: ", "SEQUENTIAL_SIZE: ", "SPACE_FILE: ", "K: ",
                   "ROOTS_FILE: ", "ROOT: ",
                   "COMPLETE_ARCS: ", "INCOMPLETE_ARCS: ",
                   "RUNTIME: ",
                   "TASK_HISTOGRAM: ", "TASKS: ", "SEQUENTIAL_TASKS: "]
          stuff = [show v, show seqsz, show sfile, show k,
                   show rfile, show root, show cmpl, show incmpl,
                   show t, show hist, show tasks, show leaves]


main :: IO ()
main = do
  -- parsing command line arguments (no real error checking)
  args <- getArgs
  let (ver, sfile, k, rfile, root, seqsz) = parseArgs args
  -- read and construct ambient space and root node of search tree
  node0 <- do
    sp <- readSpace sfile
    maybe_root <- readRoot rfile root
    return $!! mkRoot sp k maybe_root
  putStrLn $ "arcs" ++ dispVersion ver ++ dispSpaceFile sfile ++ dispK k ++
             dispRootsFile rfile ++ dispRoot root ++
             dispSeqSize seqsz
  -- classify k-arcs
  let opts = ()
  case ver of
    V0 -> do ((compl, incompl, call_hist), t) <- timeIO $ evaluate $
                                                   countArcs opts node0
             printResults (V0, 0::SeqSize, sfile, k, rfile, root,
                           compl, incompl, t, []::[Int], 0::Int, 1::Int)
    _  -> return ()