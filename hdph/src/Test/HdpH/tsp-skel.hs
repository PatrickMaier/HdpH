-- tsp-skel.hs
--
-- Author: Patrick Maier <Patrick.Maier@glasgow.ac.uk>
-- Date: 04 Jan 2017
--
-- A simple parallel implementation of Euclidean TSP, with a simple
-- lower bound estimate based on minimum spanning trees.
-- * Skeleton-based implementation in HdpH
--
---------------------------------------------------------------------------

{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}

{-# OPTIONS_GHC -fno-warn-orphans #-}  -- for NFData instance of UArray

module Main where

import Prelude hiding (init)
import Control.DeepSeq (NFData(rnf), force)
import Control.Monad (forM_)
import Control.Monad.IO.Class (MonadIO(liftIO))
import Control.Monad.ST (ST, runST)
import Data.Array.ST (STUArray, newArray, readArray, writeArray)
import Data.Array.Unboxed (UArray, accumArray, bounds, (!))
import Data.Foldable (foldlM)
import Data.IntSet (IntSet)
import qualified Data.IntSet as NodeSet
import Data.List (sortOn, maximumBy, minimumBy, isPrefixOf)
import Data.Ord (comparing)
import Data.Serialize (Serialize)
import Data.Time.Clock (NominalDiffTime, diffUTCTime, getCurrentTime)
import GHC.Generics (Generic)
import System.Environment (getArgs)
import System.Exit (exitFailure, exitSuccess)
import System.IO (stdout, stderr, hSetBuffering, BufferMode(..))

import Control.Parallel.HdpH
       (RTSConf(selSparkFIFO), defaultRTSConf, updateConf,
        Par, runParIO_, allNodes, io,
        Thunk(Thunk), Closure, mkClosure,
        StaticDecl, static, declare, register)
import qualified Control.Parallel.HdpH as HdpH (declareStatic)
import Aux.ECRef
       (ECRef, ECRefDict(..), newECRef, freeECRef, readECRef')
import qualified Aux.ECRef as ECRef (declareStatic)
import Aux.BranchAndBound (Path, BBDict(..), seqBB, parBB, seqBufBB, parBufBB)
import qualified Aux.BranchAndBound as BranchAndBound (declareStatic)


---------------------------------------------------------------------------
-- NFData instance for UArrays (orphan instance)

instance NFData (UArray a i) where
  rnf !_ = ()


---------------------------------------------------------------------------
-- parsing TSP problem instance (TSPLib format; parser may crash on error)

-- should return an empty list if parsing fails
parseCoords :: [String] -> [(Int, Float, Float)]
parseCoords ls =
  parseNodes $ getCoordSection $ map words $ ls ++ ["NODE_COORD_SECTION"]
    where
      getCoordSection =
        takeWhile (/= ["EOF"]) . tail . dropWhile (/= ["NODE_COORD_SECTION"])
      parseNodes ([i,x,y]:rest) = (read i, read x, read y) : parseNodes rest
      parseNodes _              = []

parseTSP :: String -> Maybe TSP
parseTSP text = makeTSP $ parseCoords $ lines text

readTSP :: FilePath -> IO TSP
readTSP fp = do
  text <- if null fp then getContents else readFile fp
  case parseTSP text of
    Nothing  -> error "readTSP"
    Just tsp -> return tsp


---------------------------------------------------------------------------
-- TSP problem instance

type Node           = Int
type NodeSet        = IntSet
type Distance       = Int
type DistanceMatrix = UArray (Node, Node) Distance

data TSP = TSP {
             nodes  :: !NodeSet,         -- vertices of TSP instance
             matrix :: !DistanceMatrix,  -- symmetric distance matrix
             maxlen :: !Distance }       -- absolute worst case tour length
             deriving (Eq, Ord, Show, Generic)

instance NFData TSP
instance Serialize TSP

isTSP :: TSP -> Bool
isTSP tsp =
  not (NodeSet.null v) && NodeSet.findMin v == m1 && NodeSet.findMax v == n1 &&
  m1 == m2 && n1 == n2 && isDiag0 && isPos && isSymm &&
  maxlen tsp == worstTourLength tsp
    where
      v = nodes tsp
      a = matrix tsp
      ((m1,m2),(n1,n2)) = bounds a
      isDiag0 = and [a ! (i,i) == 0 | i <- [m1 .. n1]]
      isPos   = and [a ! (i,j) >= 0 | i <- [m1 .. n1], j <- [m2 .. n2]]
      isSymm  = and [a ! (i,j) == a ! (j,i) | i <- [m1 .. n1], j <- [m2 .. n2]]

makeTSP :: [(Int, Float, Float)] -> Maybe TSP
makeTSP []      = Nothing
makeTSP triples = if isTSP tsp then Just tsp else Nothing
  where
    tsp = tsp0 { maxlen = worstTourLength tsp0 }
    tsp0 = TSP { nodes = v, matrix = a, maxlen = 0 }
    v = NodeSet.fromList [i | (i,_,_) <- triples]
    m = NodeSet.findMin v
    n = NodeSet.findMax v
    d (x1,y1) (x2,y2) = round $ sqrt $ (x1 - x2)^2 + (y1 - y2)^2
    a = accumArray (flip const) 0 ((m,m),(n,n))
          [((i,j), d (u,v) (x,y)) | (i,u,v) <- triples, (j,x,y) <- triples]

worstTourLength :: TSP -> Distance
worstTourLength tsp = sum [dist tsp x (maxDistFrom tsp x xs) | x <- xs]
  where
    xs = NodeSet.elems $ nodes tsp

dist :: TSP -> Node -> Node -> Distance
dist tsp x y = (matrix tsp) ! (x,y)

sortByDistFrom :: TSP -> Node -> [Node] -> [Node]
sortByDistFrom tsp x0 = sortOn (dist tsp x0)

maxDistFrom :: TSP -> Node -> [Node] -> Node
maxDistFrom tsp x0 = maximumBy (comparing (dist tsp x0))

minDistFrom :: TSP -> Node -> [Node] -> Node
minDistFrom tsp x0 = minimumBy (comparing (dist tsp x0))

toClosureTSP :: TSP -> Closure TSP
toClosureTSP tsp = $(mkClosure [| toClosureTSP_abs tsp |])

toClosureTSP_abs :: TSP -> Thunk TSP
toClosureTSP_abs tsp = Thunk tsp


---------------------------------------------------------------------------
-- weight of minimum spanning tree (using Prim's algorithm)

type Weight      = Distance
type WeightMap s = STUArray s Node Weight

-- Returns the weight of an MST covering the set `vertices` plus `v0`;
-- `vertices` must not contain `v0`.
weightMST :: TSP -> Node -> NodeSet -> Weight
weightMST tsp v0 vertices = runST $ do
  weight <- initPrim tsp v0 (NodeSet.toList vertices)
  let p (_, _, vertices) = NodeSet.null vertices
  let f (!w, v0, !vertices) = do
        (v',w') <- stepPrim tsp weight v0 (NodeSet.toList vertices)
        return (w + w', v', NodeSet.delete v' vertices)
  (!w, _, _) <- untilM p f (0, v0, vertices)
  return w

untilM :: (Monad m) => (a -> Bool) -> (a -> m a) -> a -> m a
untilM p act = go
  where
    go x | p x       = return x
         | otherwise = act x >>= go

initPrim :: TSP -> Node -> [Node] -> ST s (WeightMap s)
initPrim tsp v0 vs = do
  let ((m,_),(n,_)) = bounds $ matrix tsp
  weight <- newArray (m,n) 0
  forM_ vs $ \ v -> writeArray weight v $ dist tsp v0 v
  return weight

stepPrim :: forall s . TSP -> WeightMap s -> Node -> [Node]
                    -> ST s (Node, Weight)
stepPrim tsp weight v0 (v:vs) = do
  w <- updateWeightMapPrim tsp weight v0 v
  foldlM f (v,w) vs
    where
      f :: (Node, Weight) -> Node -> ST s (Node, Weight)
      f z@(_,w) v = do
        w' <- updateWeightMapPrim tsp weight v0 v
        if w' < w then return (v,w') else return z

updateWeightMapPrim :: TSP -> WeightMap s -> Node -> Node -> ST s Weight
updateWeightMapPrim tsp weight v0 v = do
  w <- readArray weight v
  let w' = dist tsp v0 v
  if w' < w
    then writeArray weight v w' >> return w'
    else return w


---------------------------------------------------------------------------
-- types and operations to implement TSP as branch&bound

-- partial solution:
-- home node + (partial) tour + length of partial tour (as round trip)
type PartialSolution = (Node, Tour, Distance)

-- (partial) tour: non-empty list of nodes (in reverse order, home node last)
type Tour = [Node]

-- candidates for extension: set of nodes
type Candidates = NodeSet

-- search space
data X = X PartialSolution Candidates deriving (Generic, Show)

instance NFData X
instance Serialize X

-- initial search space
init :: TSP -> X
init tsp = X (x0,[x0],0) xs
  where
    x0 = NodeSet.findMin $ nodes tsp
    xs = NodeSet.delete x0 $ nodes tsp

-- split search space into subspaces
split :: Bool -> TSP -> X -> [X]
split sortCandidates tsp (X (x0,tour@(x:_),len) cands) =
  map extend cs
    where
      cs | sortCandidates = sortByDistFrom tsp x0 $ NodeSet.elems cands
         | otherwise      = NodeSet.elems cands
      extend c = X (x0,c:tour,len') cands'
        where
          !len'   = len - dist tsp x x0 + dist tsp x c + dist tsp c x0
          !cands' = NodeSet.delete c cands


-- Bounds are distances (tour lengths)
type Bound = Distance

-- lower bound heuristic on tour length of extensions: length of partial tour +
-- weight of MST covering candidates plus home node and last node of the tour
lb :: TSP -> X -> Bound
lb tsp (X (x0,x:_,len) cands) = len - dist tsp x x0 + w
  where
    w = weightMST tsp x0 (NodeSet.insert x cands)


---------------------------------------------------------------------------
-- global immutable reference for TSP instance (replicated everywhere)

newECRefTSP :: TSP -> Par (ECRef TSP)
newECRefTSP tsp = do
  nodes <- allNodes
  newECRef ecDictTSPC nodes tsp

ecDictTSP :: ECRefDict TSP
ecDictTSP = ECRefDict { toClosure = toClosureTSP,
                        joinWith  = \ _x _y -> Nothing }

ecDictTSPC :: Closure (ECRefDict TSP)
ecDictTSPC = $(mkClosure [| ecDictTSP |])


---------------------------------------------------------------------------
-- paths for TSP tree iterator

toClosurePath :: Path X -> Closure (Path X)
toClosurePath path = $(mkClosure [| toClosurePath_abs path |])

toClosurePath_abs :: Path X -> Thunk (Path X)
toClosurePath_abs path = Thunk path

-- path accessors
getX :: Path X -> X
getX []    = error "getX"
getX (x:_) = x

getTour :: Path X -> Tour
getTour []                   = error "getTour"
getTour (X (_,tour,_) _ : _) = tour

getTourlen :: Path X -> Distance
getTourlen []                  = error "getTourlen"
getTourlen (X (_,_,len) _ : _) = len

getCandidates :: Path X -> Candidates
getCandidates []              = error "getCandidates"
getCandidates (X _ cands : _) = cands

-- path compression (works as only last letter of path is ever accessed)
compressPath :: Path X -> Path X
compressPath []    = []
compressPath (x:_) = [x]


---------------------------------------------------------------------------
-- solution type (including bound)

data Y = Y Tour     -- current shortest tour ([] if one hasn't been found yet)
           Distance -- length of shortest tour (or "infinity" if tour == [])
         deriving (Eq, Ord, Generic, Show)

instance NFData Y
instance Serialize Y

toClosureY :: Y -> Closure Y
toClosureY y = $(mkClosure [| toClosureY_abs y |])

toClosureY_abs :: Y -> Thunk Y
toClosureY_abs y = Thunk y


---------------------------------------------------------------------------
-- global join-semilattice updateable solution
--
-- Note that the order on bounds is reversed, thus join is minimum.

ecDictY :: ECRefDict Y
ecDictY =
  ECRefDict {
    toClosure = toClosureY,
    joinWith  = \ (Y _ len1) y2@(Y _ len2) ->
                  if len2 >= len1 then Nothing else Just y2
  }

ecDictYC :: Closure (ECRefDict Y)
ecDictYC = $(mkClosure [| ecDictY |])


-----------------------------------------------------------------------------
-- HdpH TSP search using skeleton (with default task constructor)

-- BnB dictionary for depth-bounded task generation;
-- infty is a distance greater than any possible tour length;
-- sortCandidates indicates whether to sort candidate nodes
bbDict :: ECRef TSP -> Distance -> Bool -> Int -> BBDict X Y
bbDict tspRef infty sortCandidates task_depth =
  BBDict {
    bbToClosure = toClosurePath,
    bbGenerator =
      \ path -> do
        tsp <- readECRef' tspRef
        if null path
          then return [init tsp]
          else return $ split sortCandidates tsp (getX path),
    bbObjective =
      \ path -> if not (null path) && NodeSet.null (getCandidates path)
                  then Y (getTour path) (getTourlen path)
                  else Y [] infty,
    bbDoPrune =
      \ path yRef -> do
        tsp <- readECRef' tspRef
        Y _ shortest <- readECRef' yRef
        return $! not (null path) && lb tsp (getX path) >= shortest,
    bbIsTask =
      \ path -> length path == task_depth,
    bbMkTask = Nothing
  }

bbDictC :: ECRef TSP -> Distance -> Bool -> Int -> Closure (BBDict X Y)
bbDictC tspRef infty sortCandidates task_depth =
  $(mkClosure [| bbDictC_abs (tspRef, infty, sortCandidates, task_depth) |])

bbDictC_abs :: (ECRef TSP, Distance, Bool, Int) -> Thunk (BBDict X Y)
bbDictC_abs (tspRef, infty, sortCandidates, task_depth) =
  Thunk $ bbDict tspRef infty sortCandidates task_depth


-- sequential skeleton instantiation
tspSeq :: ECRef TSP -> Bool -> Int -> Par ([Node], Distance, Integer)
tspSeq tspRef sortCands depth = do
  tsp <- readECRef' tspRef
  let !infty = maxlen tsp
  (Y tour len, !tasks) <- seqBB (bbDictC tspRef infty sortCands depth) ecDictYC
  return (reverse tour, len, tasks)

-- parallel skeleton instantiation
tspPar :: ECRef TSP -> Bool -> Int -> Par ([Node], Distance, Integer)
tspPar tspRef sortCands depth = do
  tsp <- readECRef' tspRef
  let !infty = maxlen tsp
  (Y tour len, !tasks) <- parBB (bbDictC tspRef infty sortCands depth) ecDictYC
  return (reverse tour, len, tasks)


-- sequential skeleton instantiation, "delayed" task creation
tspSeqBuf :: ECRef TSP -> Bool -> Int -> Int
          -> Par ([Node], Distance, Integer)
tspSeqBuf tspRef sortCands depth buf_sz = do
  tsp <- readECRef' tspRef
  let !infty = maxlen tsp
  (Y tour len, !tasks) <-
    seqBufBB buf_sz (bbDictC tspRef infty sortCands depth) ecDictYC
  return (reverse tour, len, tasks)

-- parallel skeleton instantiation, delayed task creation.
-- NOTE: Risks deadlocking HdpH, at least with default HdpH RTS configuration.
--   To avoid deadlock, make sure that '1 <= ntf_freq < buf_sz' and either
--   * run at least two schedulers on the root node, or
--   * run on at least two nodes, and make sure that 'buf_sz >= 2'.
--   Consider spark pool watermarks; must have 'buf_sz >= minSched > maxFish'.
tspParBuf :: ECRef TSP -> Bool -> Int -> Int -> Int
          -> Par ([Node], Distance, Integer)
tspParBuf tspRef sortCands depth buf_sz ntf_freq = do
  tsp <- readECRef' tspRef
  let !infty = maxlen tsp
  (Y tour len, !tasks) <-
    parBufBB buf_sz ntf_freq (bbDictC tspRef infty sortCands depth) ecDictYC
  return (reverse tour, len, tasks)


-----------------------------------------------------------------------------
-- Static declaration (just before 'main')

-- Empty splice; TH hack to make all environment abstractions visible.
$(return [])

declareStatic :: StaticDecl
declareStatic = mconcat [HdpH.declareStatic,
                         ECRef.declareStatic,
                         BranchAndBound.declareStatic,
                         declare $(static 'toClosureTSP_abs),
                         declare $(static 'ecDictTSP),
                         declare $(static 'toClosurePath_abs),
                         declare $(static 'toClosureY_abs),
                         declare $(static 'ecDictY),
                         declare $(static 'bbDictC_abs)]

---------------------------------------------------------------------------
-- main (and aux stuff)

toFractional :: (Real a, Fractional b) => a -> b
toFractional = fromRational . toRational

-- time a monadic 'action' (in a monad above IO)
timeM :: (MonadIO m) => m a -> m (a, NominalDiffTime)
timeM action = do t0 <- liftIO getCurrentTime
                  x <- action
                  t1 <- liftIO getCurrentTime
                  let !t = diffUTCTime t1 t0
                  return (x, t)

-- variant of 'timeM' that is strict in the result of 'action'
timeM' :: (MonadIO m) => m a -> m (a, NominalDiffTime)
timeM' action = do t0 <- liftIO getCurrentTime
                   !x <- action
                   t1 <- liftIO getCurrentTime
                   let !t = diffUTCTime t1 t0
                   return (x, t)

usage :: (MonadIO m) => m a
usage = do
  liftIO $ putStrLn "Usage: tsp-skel [OPTIONS] FILE"
  liftIO $ putStrLn "  -sort: sort nodes according to increasing distance"
  liftIO $ putStrLn "  -vN:   Nth version of the algoritm (default N=0)"
  liftIO $ putStrLn "  -dN:   depth bound (for bounded iterator; default N=0)"
  liftIO $ exitFailure

parseVersion :: String -> Maybe Int
parseVersion s | isPrefixOf "-v" s = case reads $ drop 2 s of
                                       [(ver, "")]  -> Just ver
                                       _            -> Nothing
               | otherwise         = Nothing

parseDepth :: String -> Maybe Int
parseDepth s | isPrefixOf "-d" s = case reads $ drop 2 s of
                                     [(depth, "")]  -> Just depth
                                     _              -> Nothing
             | otherwise         = Nothing

parseBufSize :: String -> Maybe Int
parseBufSize s | isPrefixOf "-n" s = case reads $ drop 2 s of
                                       [(bufsize, "")]  -> Just bufsize
                                       _                -> Nothing
               | otherwise         = Nothing

parseNotifyFreq :: String -> Maybe Int
parseNotifyFreq s | isPrefixOf "-k" s = case reads $ drop 2 s of
                                          [(nfreq, "")]  -> Just nfreq
                                          _              -> Nothing
                  | otherwise         = Nothing

parseSortFlag :: String -> Maybe Bool
parseSortFlag "-sort" = Just True
parseSortFlag _       = Nothing

parseFilename :: String -> Maybe String
parseFilename ""      = Nothing
parseFilename ('-':_) = Nothing
parseFilename s       = Just s

parseArgs :: [String] -> (Bool, Int, Int, Int, Int, String)
parseArgs = foldl parse (False, 0, 0, 1, 1, "")
  where
    parse (sort, ver, depth, bufsize, nfreq, file) s =
      maybe id upd1 (parseSortFlag s) $
      maybe id upd2 (parseVersion s) $
      maybe id upd3 (parseDepth s) $
      maybe id upd4 (parseBufSize s) $
      maybe id upd5 (parseNotifyFreq s) $
      maybe id upd6 (parseFilename s) $ (sort, ver, depth, bufsize, nfreq, file)
    upd1 y ( _, x2, x3, x4, x5, x6) = ( y, x2, x3, x4, x5, x6)
    upd2 y (x1,  _, x3, x4, x5, x6) = (x1,  y, x3, x4, x5, x6)
    upd3 y (x1, x2,  _, x4, x5, x6) = (x1, x2,  y, x4, x5, x6)
    upd4 y (x1, x2, x3,  _, x5, x6) = (x1, x2, x3,  y, x5, x6)
    upd5 y (x1, x2, x3, x4,  _, x6) = (x1, x2, x3, x4,  y, x6)
    upd6 y (x1, x2, x3, x4, x5,  _) = (x1, x2, x3, x4, x5,  y)


-- parse HdpH runtime system config options; abort if there is an error
parseOpts :: [String] -> IO (RTSConf, [String])
parseOpts args = do
  either_conf <- updateConf args defaultRTSConf
  case either_conf of
    Left err_msg                 -> error $ "parseOpts: " ++ err_msg
    Right (conf, remaining_args) -> return (conf, remaining_args)


main :: IO ()
main = do
  hSetBuffering stdout LineBuffering
  hSetBuffering stderr LineBuffering
  -- HdpH magic (register static decls, parse cmd line opts and get going)
  register declareStatic
  opts_args <- getArgs
  (conf0, args) <- parseOpts opts_args
  let conf = conf0 { selSparkFIFO = True }  -- strict FIFO spark selection
  runParIO_ conf $ do
    -- parsing TSP cmd line arguments (no real error checking)
    let (sortCands, version, depth, buf_sz, ntf_freq, filename) = parseArgs args
    io $ putStrLn $
      "TSP" ++ " -v" ++ show version ++ " -d" ++ show depth ++
      " -n" ++ show buf_sz ++ " -k" ++ show ntf_freq ++
      (if sortCands then " -sort" else "") ++
      " " ++ filename
    -- reading TSP instance from file (or stdin)
    tsp <- io $ readTSP filename
    -- distributing TSP instance to all nodes
    (tspRef, t_distribute) <- timeM' $ do
      newECRefTSP tsp
    io $ putStrLn $ "t_distribute: " ++ show t_distribute
    -- dispatch on version
    ((tour, len, tasks), t_compute) <- timeM' $ case version of
      0 -> force <$> tspSeq tspRef sortCands depth                    -- seq
      1 -> force <$> tspSeqBuf tspRef sortCands depth buf_sz          -- seq
      2 -> force <$> tspPar tspRef sortCands depth                    -- par
      3 -> force <$> tspParBuf tspRef sortCands depth buf_sz ntf_freq -- par
      _ -> usage
    return ()
    -- deallocating TSP instance
    ((), t_deallocate) <- timeM' $ do
      freeECRef tspRef
    io $ putStrLn $ "t_deallocate: " ++ show t_deallocate
    -- print solution and statistics
    io $ putStrLn $ "tour: " ++ show tour
    io $ putStrLn $ "cities: " ++ show (length tour)
    io $ putStrLn $ "length: " ++ show len
    io $ putStrLn $ "tasks: " ++ show tasks
    io $ putStrLn $ "t_compute: " ++ show t_compute
  -- the end
  exitSuccess
