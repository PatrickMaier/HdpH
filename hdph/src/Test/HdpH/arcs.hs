-- Counting complete k-arcs in PG(2,q), without symmetry breaking.
-- Refers to Open Problem 4.10(f) in [1].
--
-- Args:    -space=SPACEFILE -lLOWER -uUPPER  ## where 3 <= LOWER <= k <= UPPER
-- OptArgs: [V0|V1] [-roots=ROOTSFILE] [-rROOT] [-sSEQUENTIALSIZE]
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
import Control.DeepSeq (NFData(rnf), ($!!), force)
import Control.Exception (evaluate)
import Control.Monad (when, zipWithM_)
import qualified Data.IntSet as Set
import Data.IORef (IORef, newIORef, atomicWriteIORef, readIORef)
import Data.List
       (foldl', group, isPrefixOf, partition, sort, stripPrefix, tails)
import qualified Data.List as List (lines)
import Data.Maybe (fromJust)
import Data.Time.Clock (NominalDiffTime, diffUTCTime, getCurrentTime)
import Data.Serialize (Serialize)
import GHC.Generics (Generic)
import System.Environment (getArgs)
import System.IO (stdout, stderr, hSetBuffering, BufferMode(..))
import System.IO.Unsafe (unsafePerformIO)
import System.Random (mkStdGen, setStdGen)

import Control.Parallel.HdpH
       (RTSConf(..), defaultRTSConf, updateConf,
        Par, runParIO, io, spawn, get,
        Thunk(Thunk), Closure, unClosure, mkClosure,
        static, StaticDecl, declare, register)
import qualified Control.Parallel.HdpH as HdpH (declareStatic)
import Control.Parallel.HdpH.Dist (one)

import Test.HdpH.FinInG2
       (Space, points, pointsOf, span, readSpace,
        Point, parseListOfPoint, parseSortedListOfPoint, elemsSet)


---------------------------------------------------------------------------
-- global reference holding ambient space

spaceRef :: IORef Space
{-# NOINLINE spaceRef #-}
spaceRef = unsafePerformIO $ newIORef $ error "spaceRef not initialised"

setSpace :: Space -> IO ()
setSpace = atomicWriteIORef spaceRef

getSpace :: Par Space
getSpace = io $ readIORef spaceRef


---------------------------------------------------------------------------
-- unsigned integer histograms

type Hist = [Int]

-- empty histogram
emptyHist :: Hist
emptyHist = []

-- push zero to histogram
pushZeroHist :: Hist -> Hist
pushZeroHist hist = 0:hist

-- push one to histogram
pushOneHist :: Hist -> Hist
pushOneHist hist = 1:hist

-- zero histogram of given length
zeroHist :: Int -> Hist
zeroHist n = replicate n 0

-- histogram with `1` at the `n`-th position, `0` in leading positions.
oneHist :: Int -> Hist
oneHist n = zeroHist n ++ [1]

-- addition of two histograms
(|+|) :: Hist -> Hist -> Hist
xs     |+| []     = xs
[]     |+| ys     = ys
(x:xs) |+| (y:ys) = z:zs where { !z = x + y; !zs = xs |+| ys }

-- totaling of a histogram
totalHist :: Hist -> Int
totalHist = sum


---------------------------------------------------------------------------
-- sets of points represented as lists in strict ascending order

type SetOfPoint = [Point]

isSetOfPoint :: SetOfPoint -> Bool
isSetOfPoint = go where
  go (a:bs@(b:_)) = a < b && go bs
  go _            = True

-- Complexity of sorting
fromListP :: [Point] -> SetOfPoint
fromListP = fromAscListP . map head . group . sort

-- Constant complexity; unsafe (assumes list is already strictly sorted)
fromAscListP :: [Point] -> SetOfPoint
fromAscListP = id

-- Constant complexity
toAscListP :: SetOfPoint -> [Point]
toAscListP = id

-- Linear complexity
sizeP :: SetOfPoint -> Int
sizeP = length

-- Constant complexity
nullP :: SetOfPoint -> Bool
nullP = null

-- Linear complexity
memberP :: Point -> SetOfPoint -> Bool
memberP b = go where
  go (a:as) = if a < b then go as else a == b
  go []     = False

-- Linear complexity
subsetP :: SetOfPoint -> SetOfPoint -> Bool
subsetP []         _       = True
subsetP _          []      = False
subsetP as@(a:as') (b:bs') | a > b     = subsetP as bs'
                           | otherwise = a == b && subsetP as' bs'

-- Linear complexity
disjointP :: SetOfPoint -> SetOfPoint -> Bool
disjointP []         _          = True
disjointP _          []         = True
disjointP as@(a:as') bs@(b:bs') | a < b     = disjointP as' bs
                                | a > b     = disjointP as  bs'
                                | otherwise = False

-- Constant complexity
emptyP :: SetOfPoint
emptyP = []

-- Constant complexity
singletonP :: Point -> SetOfPoint
singletonP a = [a]

-- Constant complexity
pairP :: Point -> Point -> SetOfPoint
pairP a b | a < b     = [a,b]
          | b < a     = [b,a]
          | otherwise = [a]

-- Linear complexity
insertP :: Point -> SetOfPoint -> SetOfPoint
insertP b as = unionP as [b]

-- Linear complexity
deleteP :: Point -> SetOfPoint -> SetOfPoint
deleteP b as = differenceP as [b]

-- Linear complexity
unionP :: SetOfPoint -> SetOfPoint -> SetOfPoint
unionP []         bs         = bs
unionP as         []         = as
unionP as@(a:as') bs@(b:bs') | a < b     = a : unionP as' bs
                             | a > b     = b : unionP as  bs'
                             | otherwise = a : unionP as' bs'

-- Linear complexity
intersectionP :: SetOfPoint -> SetOfPoint -> SetOfPoint
intersectionP []         _          = []
intersectionP _          []         = []
intersectionP as@(a:as') bs@(b:bs') | a < b     = intersectionP as' bs
                                    | a > b     = intersectionP as  bs'
                                    | otherwise = a : intersectionP as' bs'

-- Linear complexity
differenceP :: SetOfPoint -> SetOfPoint -> SetOfPoint
differenceP []         _          = []
differenceP as         []         = as
differenceP as@(a:as') bs@(b:bs') | a < b     = a : differenceP as' bs
                                  | a > b     = differenceP as  bs'
                                  | otherwise = differenceP as' bs

-- Linear complexity
tailsP :: SetOfPoint -> [SetOfPoint]
tailsP = tails

-- Linear complexity (times complexity of filter predicate)
filterP :: (Point -> Bool) -> SetOfPoint -> SetOfPoint
filterP p = go where
  go []     = []
  go (a:as) | p a       = a : go as
            | otherwise = go as

-- Linear complexity (times complexity of partitioning predicate)
partitionP :: (Point -> Bool) -> SetOfPoint -> (SetOfPoint, SetOfPoint)
partitionP = partition


---------------------------------------------------------------------------
-- completeness test for arcs

-- Given an arc 'arc' and a 'c' not in 'arc', return 'True' iff 'arc'
-- can not be extended with 'c' (i.e. 'c:arc' is no arc);
-- 'as' is a set representation of the points in 'arc'.
isNoCandidateForArc :: Space -> [Point] -> SetOfPoint -> Point -> Bool
isNoCandidateForArc sp arc as c =
  -- `c` is not a candidate iff some line thru `c` and some point `a` in `arc`
  -- also meets another point `b` in `arc`
  or [b `memberP` as |
      a <- arc, b <- elemsSet $ pointsOf sp $ fromJust $ span sp c a, b /= a]
     -- NOTE: Could shorten list by adding filter `b /= c`;
     --       however, this appears to result in less efficient code.


-- Return 'True' iff arc 'arc' is complete (i.e. maximal wrt set inclusion)
-- when tested against list of rejected points `xs`.
isCompleteArc :: Space -> [Point] -> [Point] -> Bool
isCompleteArc sp arc xs =
  and [isNoCandidateForArc sp arc as c | c <- xs]
    where as = fromListP arc


---------------------------------------------------------------------------
-- candidates for extension of arcs

-- Given an arc `b:arc` and a set of candidates `cs` for extending point `arc`,
-- shrink `cs` such that the resulting set are the candidates for extending
-- `b:arc`. Note that `b` may be an element of `cs`
shrinkCandidates :: Space -> [Point] -> SetOfPoint -> SetOfPoint
shrinkCandidates sp [b]     cs = deleteP b cs
shrinkCandidates sp (b:arc) cs =
  -- remove all points on 2-secants thru `b` and some point `a` in `arc`
  foldl differenceP cs
  [fromAscListP $ elemsSet $ pointsOf sp $ fromJust $ span sp a b | a <- arc]


-- Given an `arc`, return the set of all candidates for extending `arc`.
candidates :: Space -> SetOfPoint -> SetOfPoint
candidates sp arc = snd $ foldr f ([], fromListP $ points sp) arc
  where
    f b (arc, cs) = (b:arc, shrinkCandidates sp (b:arc) cs)


---------------------------------------------------------------------------
-- search tree and sequential algorithm

-- Search tree node
data Node = Node {
              k_min  :: !Int,        -- lower bound on target arc size
              k_max  :: !Int,        -- upper bound on target arc size
              arc    :: ![Point],    -- current arc
              nArc   :: !Int,        -- size of current arc
              cand   :: SetOfPoint,  -- candidates for extension of arc
              nCand  :: !Int,        -- size of current candidate set
              reject :: [Point] }    -- list of rejected candidates
              deriving (Generic)

instance Serialize Node

instance NFData Node where
  rnf node = rnf (k_min node) `seq` rnf (k_max node) `seq`
             rnf (arc node) `seq` rnf (nArc node) `seq`
             rnf (cand node) `seq` rnf (nCand node) `seq`
             rnf (reject node) `seq` ()


-- 'True' iff node can possibly be extended to a k-arc
feasible :: Node -> Bool
feasible node = nArc node + nCand node >= k_min node


-- Generate root node of the search tree
mkRoot :: Space -> Lower -> Upper -> Maybe ([Point], SetOfPoint) -> Node
mkRoot sp lower upper maybe_root =
  case maybe_root of
    Just (as, cs) -> Node {
                       k_min = lower, k_max = upper,
                       arc = reverse as, nArc = length as,
                       cand = cs, nCand = sizeP cs,
                       reject = toAscListP $ differenceP (candidates sp as) cs }
    Nothing       -> mkRoot sp lower upper $ Just ([], candidates sp [])


generate :: Space -> Node -> [Node]
generate space this
  | k >= k_max this   = []  -- termination: max depth
  | nullP (cand this) = []  -- termination: no alternatives left
  | otherwise         = filter feasible $ children (reject this) cs (cand this)
  where
    k = nArc this
    gap = k_min this - (k + 1)
    cs  = take (nCand this - gap) $ toAscListP $ cand this
          -- NOTE: Can drop up to `gap` candidates at the end because candidate
          -- sets would have fewer than `gap` elts, so `k_min` is not reachable.
    children :: [Point] -> [Point] -> SetOfPoint -> [Node]
    children _  []     _     = []
    children xs (c:cs) cands = child : children (c:xs) cs (deleteP c cands)
      where
        arc'   = c : arc this
        cands' = shrinkCandidates space arc' cands
        child = this { arc  = arc',
                       nArc = nArc this + 1,
                       cand  = cands',
                       nCand = sizeP cands',
                       reject = xs }


-- Returns histogram of complete k-arcs, k_min <= k <= k_max, below `current`
countArcs :: Space -> Node -> Hist
countArcs space current =
  add1 $! foldl' (|+|) zero $ map (countArcs space) $ generate space current
    where
      is_complete_k_arc = k_min current <= nArc current &&
                          nArc current <= k_max current &&
                          nullP (cand current) &&
                          isCompleteArc space (arc current) (reject current)
      zero = zeroHist (k_max current - k_min current + 1)
      add1 h | is_complete_k_arc = h |+| oneHist (nArc current - k_min current)
             | otherwise         = h


---------------------------------------------------------------------------
-- parallel algorithm, direct implementation

data Result = Result
                !Hist  -- k-histogram of complete k-arcs
                !Hist  -- depth-histogram of tasks spawned
                !Int   -- total number of sequential tasks
                deriving (Generic)

instance Serialize Result

instance NFData Result where
  rnf (Result arc_hist spawn_hist seq_tasks) =
    rnf arc_hist `seq` rnf spawn_hist `seq` rnf seq_tasks `seq` ()


-- Reduce a non-empty list of results
reduceResults :: [Result] -> Result
reduceResults ((Result x y z):results) = go x y z results where
  go !x' !y' !z' results =
    case results of
      (Result x y z):rest -> go (x' |+| x) (y' |+| y) (z' + z) rest
      []                  -> Result x' (pushOneHist y') z'


toClosureResult :: Result -> Closure Result
toClosureResult result = $(mkClosure [| toClosureResult_abs result |])

toClosureResult_abs :: Result -> Thunk Result
toClosureResult_abs result = Thunk result


countArcsPar :: SeqSize -> Node -> Par Result
countArcsPar seqsz current = do
  space <- getSpace
  let k = nArc current
  let children = generate space current
  if nCand current <= seqsz || k >= k_min current || null children
    then -- NOTE: Parallel algorithm falls back on sequential as soon as
         --       it could detect a complete arc of target size.
         let !arc_hist = force $ countArcs space current in
         return $! Result arc_hist (pushOneHist emptyHist) 1
    else do
      let mkTask child = $(mkClosure [| countArcsPar_abs (seqsz, child) |])
      vs <- mapM (spawn one) [mkTask child | child <- children]
      result <- reduceResults . map unClosure <$> mapM get vs
      return $! result

countArcsPar_abs :: (SeqSize, Node) -> Thunk (Par (Closure Result))
countArcsPar_abs (seqsz, child) = Thunk $
  toClosureResult <$> countArcsPar seqsz child


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
-- argument parsing

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


-- Option: lower bound on target arc size; default: 0
type Lower = Int

dispLower :: Lower -> String
dispLower k = " -l" ++ show k

parseLower :: String -> Maybe Lower
parseLower s | isPrefixOf "-l" s = case reads $ drop (length "-l") s of
                                     [(k, "")] -> Just k
                                     _         -> Nothing
             | otherwise         = Nothing


-- Option: upper bound on target arc size; default: 0
type Upper = Int

dispUpper :: Upper -> String
dispUpper k = " -u" ++ show k

parseUpper :: String -> Maybe Upper
parseUpper s | isPrefixOf "-u" s = case reads $ drop (length "-u") s of
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


-- Option: root selection, pos int; default: 1 (select first root)
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


parseArgs :: [String]
          -> (Version, SpaceFile, Lower, Upper, RootsFile, Root, SeqSize)
parseArgs = foldl parse (V0, "", 0, 0, "", 1, 0)
  where
    parse (ver, sfile, lower, upper, rfile, root, seqsz) s =
      maybe id upd1a (parseVersion s) $
      maybe id upd1b (parseSpaceFile s) $
      maybe id upd1c (parseLower s) $
      maybe id upd1d (parseUpper s) $
      maybe id upd1e (parseRootsFile s) $
      maybe id upd1f (parseRoot s) $
      maybe id upd1g (parseSeqSize s) $
      (ver, sfile, lower, upper, rfile, root, seqsz)
    upd1a z (ya,yb,yc,yd,ye,yf,yg) = ( z,yb,yc,yd,ye,yf,yg)
    upd1b z (ya,yb,yc,yd,ye,yf,yg) = (ya, z,yc,yd,ye,yf,yg)
    upd1c z (ya,yb,yc,yd,ye,yf,yg) = (ya,yb, z,yd,ye,yf,yg)
    upd1d z (ya,yb,yc,yd,ye,yf,yg) = (ya,yb,yc, z,ye,yf,yg)
    upd1e z (ya,yb,yc,yd,ye,yf,yg) = (ya,yb,yc,yd, z,yf,yg)
    upd1f z (ya,yb,yc,yd,ye,yf,yg) = (ya,yb,yc,yd,ye, z,yg)
    upd1g z (ya,yb,yc,yd,ye,yf,yg) = (ya,yb,yc,yd,ye,yf, z)


printResults :: (Show a1, Show a2, Show a3, Show a4, Show a5, Show a6,
                 Show a7, Show a8, Show a9, Show a10, Show a11, Show a12)
             => (a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12) -> IO ()
printResults
  (v, seqsz, sfile, k_min, k_max, rfile, root, cmpl, t, hist, tasks, leaves) =
  zipWithM_ printTagged tags stuff
    where printTagged tag val = putStrLn (tag ++ val)
          tags  = ["VERSION: ", "SEQUENTIAL_SIZE: ",
                   "SPACE_FILE: ", "K_MIN: ", "K_MAX: ",
                   "ROOTS_FILE: ", "ROOT: ",
                   "COMPLETE_ARC_HISTOGRAM: ", "RUNTIME: ",
                   "TASK_HISTOGRAM: ", "TASKS: ", "SEQUENTIAL_TASKS: "]
          stuff = [show v, show seqsz,
                   show sfile, show k_min, show k_max,
                   show rfile, show root,
                   show cmpl, show t,
                   show hist, show tasks, show leaves]


-----------------------------------------------------------------------------
-- Static declaration (just before 'main')

-- Empty splice; TH hack to make all environment abstractions visible.
$(return [])

declareStatic :: StaticDecl
declareStatic =
  mconcat
    [HdpH.declareStatic,
     declare $(static 'toClosureResult_abs),
     declare $(static 'countArcsPar_abs)]


---------------------------------------------------------------------------
-- main, auxiliaries, etc.

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


-- parse runtime system config options (+ seed for random number generator)
-- abort if there is an error
parseOpts :: [String] -> IO (RTSConf, Int, [String])
parseOpts args = do
  either_conf <- updateConf args defaultRTSConf
  case either_conf of
    Left err_msg             -> error $ "parseOpts: " ++ err_msg
    Right (conf, [])         -> return (conf, 0, [])
    Right (conf, arg':args') ->
      case stripPrefix "-rand=" arg' of
        Just s  -> return (conf, read s, args')
        Nothing -> return (conf, 0,      arg':args')


main :: IO ()
main = do
  return ()
  hSetBuffering stdout LineBuffering
  hSetBuffering stderr LineBuffering
  register declareStatic
  opts_args <- getArgs
  (conf, seed, args) <- parseOpts opts_args
  initrand seed
  -- parsing command line arguments (no real error checking)
  let (ver, sfile, lower, upper, rfile, root, seqsz) = parseArgs args
  let (k_min, k_max) | 3 <= lower && lower <= upper = (lower, upper)
                     | otherwise                    = (0, 0)
  putStrLn $ "arcs" ++ dispVersion ver ++ dispSpaceFile sfile ++
             dispLower k_min ++ dispLower k_max ++
             dispRootsFile rfile ++ dispRoot root ++
             dispSeqSize seqsz
  -- read, construct and record ambient space
  !space <- do { (return $!!) =<< readSpace sfile }
  setSpace space
  -- construct root node of search tree
  !node0 <- do
    maybe_root <- readRoot rfile root
    return $!! mkRoot space k_min k_max maybe_root
  -- classify k-arcs
  putStrLn $ "Classifying k-arcs, " ++ show k_min ++ " <= k <= " ++ show k_max
  case ver of
    V0 -> do (arc_hist, t) <- timeIO $ evaluate $!!
                                countArcs space node0
             let full_spawn_hist =
                   foldl' (\ h _ -> pushZeroHist h) emptyHist (arc node0)
             let tasks = totalHist full_spawn_hist
             printResults (V0, 0::SeqSize, sfile, k_min, k_max, rfile, root,
                           arc_hist, t, full_spawn_hist, tasks, 0::Int)
    V1 -> do (!output, t) <- timeIO $ evaluate =<< runParIO conf
                               (countArcsPar seqsz node0)
             case output of
               Nothing -> return ()
               Just (Result arc_hist spawn_hist seq_tasks) -> do
                 let full_spawn_hist =
                       foldl' (\ h _ -> pushZeroHist h) spawn_hist (arc node0)
                 let tasks = totalHist full_spawn_hist
                 printResults (V1, seqsz, sfile, k_min, k_max, rfile, root,
                               arc_hist, t, full_spawn_hist, tasks, seq_tasks)
