-- maxclique-skel.hs
--
-- Author: Patrick Maier <Patrick.Maier@glasgow.ac.uk>
-- Date: 27 Dec 2016
--
-- Max clique algorithm (described in [1]) implemented in parallel using
-- tree iterators; sets of vertices represented via Data.IntSet.
-- * Skeleton-based implementation in HdpH
-- 
-- References:
-- [1] Ciaran McCreesh. Algorithms for the Maximum Clique Problem,
--     and How to Parallelise Them. 6 June 2014.
--
---------------------------------------------------------------------------

{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleInstances #-}   -- for MonadIO instance
{-# LANGUAGE TemplateHaskell #-}

{-# OPTIONS_GHC -fno-warn-orphans #-}

module Main where

import Prelude
import Control.Category (Category, (<<<))
import qualified Control.Category as Cat (id, (.))
import Control.DeepSeq (NFData, ($!!), force)
import Control.Monad (unless)
import Control.Monad.IO.Class (MonadIO(liftIO))
import Data.Array (Array, bounds, (!), listArray)
import Data.IntSet (IntSet)
import qualified Data.IntSet as VertexSet
       (fromAscList, null, size, member,
        minView, empty, insert, delete, intersection, difference)
import Data.List (group, groupBy, sort, sortBy, isPrefixOf)
import qualified Data.IntMap.Strict as StrictMap (fromAscList, findWithDefault)
import Data.Ord (comparing, Down(Down))
import Data.Serialize (Serialize)
import Data.Time.Clock (NominalDiffTime, diffUTCTime, getCurrentTime)
import GHC.Generics (Generic)
import System.Environment (getArgs)
import System.Exit (exitFailure, exitSuccess)
import System.IO (stdout, stderr, hSetBuffering, BufferMode(..))

import Control.Parallel.HdpH
       (RTSConf, defaultRTSConf, updateConf,
        Par, runParIO_,
        myNode, allNodes, io, spawn, get,
        Thunk(Thunk), Closure, mkClosure, unClosure, unitC,
        StaticDecl, static, declare, register)
import qualified Control.Parallel.HdpH as HdpH (declareStatic)
import Control.Parallel.HdpH.Dist (one)
import Aux.ECRef
       (ECRef, ECRefDict(ECRefDict),
        newECRef, freeECRef, readECRef', writeECRef, gatherECRef')
import qualified Aux.ECRef as ECRef (declareStatic, toClosure, joinWith)
import Aux.Iter (IterM, nextIterM)
import Aux.TreeIter
       (Path, GeneratorM, TreeIterM, newTreeIterM, newPruneTreeIterM)


---------------------------------------------------------------------------
-- MonadIO instance for Par (orphan instance)

instance MonadIO Par where
  liftIO = io

{-# SPECIALIZE nextIterM :: IterM Par s a -> Par (Maybe a) #-}
{-# SPECIALIZE newTreeIterM
               :: Path a -> GeneratorM Par a -> Par (TreeIterM Par a) #-}
{-# SPECIALIZE newPruneTreeIterM
               :: (Path a -> Bool) -> Path a -> GeneratorM Par a
               -> Par (TreeIterM Par a) #-}


-----------------------------------------------------------------------------
-- HdpH branch-and-bound skeleton (based on tree iterators and ECRefs)

data MODE = SEQ | PAR deriving (Eq, Ord, Generic, Show)

instance NFData MODE
instance Serialize MODE


-- | Dictionary of operations for BnB skeleton.
-- Type variable 'x' is the path alphabet for tree the iterators,
-- type variable 'y' is the solution (including the bound) to be maximised.
data BBDict x y =
  BBDict {
    bbToClosure :: Path x -> Closure (Path x),     -- closure conv for Path
    bbGenerator :: GeneratorM Par x,               -- ordered tree generator
    bbObjective :: Path x -> y,                    -- objective function
    bbDoPrune   :: Path x -> ECRef y -> Par Bool,  -- pruning predicate
    bbIsTask    :: Path x -> Bool,                 -- task predicate
    bbMkTask    :: Maybe (Path x -> ECRef y -> Closure (Par (Closure ())))
      -- optional task constructor (applied to path picked by bbIsTask)
  }


-- | Sequential BnB skeleton.
-- Takes a closured BnB dictionary and a closured ECRef dictionary.
-- Runs a sequential BnB search and returns the maximal solution and
-- the number of tasks generated.
seqBB :: Closure (BBDict x y) -> Closure (ECRefDict y) -> Par (y, Int)
seqBB = bb SEQ


-- | Parallel BnB skeleton.
-- Takes a closured BnB dictionary and a closured ECRef dictionary.
-- Runs a parallel BnB search and returns the maximal solution and
-- the number of tasks generated.
parBB :: Closure (BBDict x y) -> Closure (ECRefDict y) -> Par (y, Int)
parBB = bb PAR



-- BnB search wrapper.
-- Takes a mode, a closured BnB dictionary, and a closured ECRef dictionary.
-- Runs a BnB search (as per mode) and returns the maximal solution and
-- the number of tasks generated.
bb :: MODE -> Closure (BBDict x y) -> Closure (ECRefDict y) -> Par (y, Int)
{-# INLINE bb #-}
bb mode bbDictC ecDictYC = do
  let !bbDict = unClosure bbDictC
  -- set up solution & bound propagating ECRef
  nodes <- case mode of { SEQ -> fmap (\ x -> [x]) myNode; PAR -> allNodes }
  let path0 = []
  let y0 = bbObjective bbDict path0
  yRef <- newECRef ecDictYC nodes y0
  -- run BnB search
  !tasks <- bbSearch mode bbDictC path0 yRef
  -- collect maximal solution (a readECRef' call should do that, too)
  y <- gatherECRef' yRef
  -- deallocate the ECRef and return result
  freeECRef yRef
  return (y, tasks)


-- Actual BnB search.
-- Takes a mode, a closured BnB dictionary, a starting path, and an ECRef
-- for propagating the solution.
-- Returns (when all tasks are completed) the number of tasks generated.
bbSearch :: MODE -> Closure (BBDict x y) -> Path x -> ECRef y -> Par Int
{-# INLINE bbSearch #-}
bbSearch mode bbDictC path0 yRef = do
  let !bbDict = unClosure bbDictC
  -- select task constructor
  let mkTask = case bbMkTask bbDict of
                 Just mk -> mk
                 Nothing -> defaultMkTask bbDictC
  -- set up outer tree iterator (which generates tasks)
  let generator []   = bbGenerator bbDict []
      generator path = do
        prune <- bbDoPrune bbDict path yRef
        if prune then return [] else bbGenerator bbDict path
  iter <- newPruneTreeIterM (bbIsTask bbDict) path0 generator
  -- loop stepping through outer iterator
  case mode of
    SEQ -> evalTasks 0
             where
               -- immediately evaluate tasks generated by iterator
               evalTasks !count = do
                 maybe_path <- nextIterM iter
                 case maybe_path of
                   Nothing   -> return count
                   Just path -> do
                     _ <- unClosure $ mkTask path yRef
                     evalTasks (count + 1)
    PAR -> spawnTasks 0 [] >>= \ (!count, ivars) ->
           -- block until all tasks are completed; wait on tasks in order of
           -- creation so thread may not block immediately (in case the oldest
           -- tasks have already completed); while thread is blocked, scheduler
           -- may evaluate further tasks (typically the youngest ones).
           mapM_ get (reverse ivars) >>
           return count
             where
               -- spawn tasks generated by iterator
               spawnTasks !count ivars = do
                 maybe_path <- nextIterM iter
                 case maybe_path of
                   Nothing   -> return (count, ivars)
                   Just path -> do
                     ivar <- spawn one $ mkTask path yRef
                     spawnTasks (count + 1) (ivar:ivars)


-- Default task generation (used when bbMkTask == Nothing).
defaultMkTask :: Closure (BBDict x y)
              -> Path x -> ECRef y -> Closure (Par (Closure ()))
defaultMkTask bbDictC path0 yRef =
  $(mkClosure [| defaultMkTask_abs (bbDictC, path0C, yRef) |])
    where
      !bbDict = unClosure bbDictC
      path0C = bbToClosure bbDict path0

defaultMkTask_abs :: (Closure (BBDict x y), Closure (Path x), ECRef y)
                  -> Thunk (Par (Closure ()))
defaultMkTask_abs (bbDictC, path0C, yRef) = Thunk $ do
  let !bbDict = unClosure bbDictC
  -- set up inner tree iterator (which enumerates path)
  let generator []   = bbGenerator bbDict []
      generator path = do
        prune <- bbDoPrune bbDict path yRef
        if prune then return [] else bbGenerator bbDict path
  iter <- newTreeIterM (unClosure path0C) generator
  -- loop stepping through inner iterator, maximising objective function
  let maxLoop = do
        maybe_path <- nextIterM iter
        case maybe_path of
          Nothing   -> return ()
          Just path -> do
            _maybe_y <- writeECRef yRef (bbObjective bbDict path)
            maxLoop
  maxLoop
  -- return unit closure to signal completion
  return unitC


---------------------------------------------------------------------------
-- permutations

-- A bijection from 'a' to 'b' is a pair of functions 'eff :: a -> b' and
-- 'gee :: b -> a' that are inverse to each other, i.e. 'gee . eff = id' and
-- 'eff . gee = id'.
data Bijection a b = Bijection { eff :: a -> b, gee :: b -> a }

-- Bijections form a category.
instance Category Bijection where
  id = Bijection { eff = \ x -> x,
                   gee = \ x -> x }
  beta . alpha = Bijection { eff = \ x -> eff beta $ eff alpha x,
                             gee = \ z -> gee alpha $ gee beta z }

-- Bijections can be inverted.
inv :: Bijection a b -> Bijection b a
inv alpha = Bijection { eff = gee alpha, gee = eff alpha }

-- Bijections can be applied.
app :: Bijection a b -> a -> b
app alpha x = eff alpha x

-- Permutations are endo-bijections.
type Perm a = Bijection a a


---------------------------------------------------------------------------
-- vertices

-- Vertices are non-negative machine integers.
type Vertex = Int

-- Hilbert's Hotel permutation on vertices: 'eff' increments, 'gee' decrements.
permHH :: Perm Vertex
permHH = Bijection { eff = \ v -> v + 1,
                     gee = \ v -> if v > 0 then v - 1 else error "permHH.gee" }


---------------------------------------------------------------------------
-- undirected graphs

-- An undirected graph is represented as a list of non-empty lists of vertices
-- such that each vertex occurs exactly once as the head of a list, and no list
-- contains duplicates. The list of heads is the list of vertices of the
-- undirected graph, and the tails are the respective vertices' adjacency lists.
-- The list of vertices and all adjacency lists are sorted in ascending order.
-- Additionally, an undirected graph is symmetric, ie. u is in the adjecency
-- list of v iff v is in the adjacency list of u.
type UGraph = [[Vertex]]

-- Convert a list of edges over 'n' vertices (numbered '1' to 'n') into an
-- undirected graph.
mkUG :: Int -> [(Vertex,Vertex)] -> UGraph
mkUG n edges =
  if n == length uG then uG else error "mkUG: vertices out of bounds"
    where
      sortUniq = map head . group . sort
      refl_edges = [(v,v) | v <- [1 .. n]]
      symm_edges = concat [[(u,v), (v,u)] | (u,v) <- edges]
      all_edges = sortUniq (refl_edges ++ symm_edges)
      grouped_edges = groupBy (\ (u,_) (v,_) -> u == v) all_edges
      uG = [u : [v | (_,v) <- grp, v /= u] | grp@((u,_):_) <- grouped_edges]

-- Apply a vertex permutation to an undirected graph.
appUG :: Perm Vertex -> UGraph -> UGraph
appUG alpha =
  sortBy (comparing head) .          -- sort list of vertices
  map (\ (u:vs) -> (u : sort vs)) .  -- sort all adjacency lists
  map (map $ app alpha)              -- apply alpha to all vertices

verticesUG :: UGraph -> [Vertex]
verticesUG = map head

degreesUG :: UGraph -> [(Vertex,Int)]
degreesUG = map (\ (u:vs) -> (u, length vs))

-- Check degrees are anti-monotonic wrt. vertex order, ie. whether
-- 'map snd . degreesUG' yields a non-increasing list.
isDegreesAntiMonotonicUG :: UGraph -> Bool
isDegreesAntiMonotonicUG = isAntiMonotonic . map snd . degreesUG
  where
    isAntiMonotonic (x:y:zs) = x >= y && isAntiMonotonic (y:zs)
    isAntiMonotonic [_]      = True
    isAntiMonotonic []       = True

-- Compute a permutation that transforms the given undirected graph into
-- an isomorphic one where the degrees are anti-monotonic wrt. vertex order.
-- This resulting vertex order is also known as /non-increasing degree order/.
antiMonotonizeDegreesPermUG :: UGraph -> Perm Vertex
antiMonotonizeDegreesPermUG uG =
  Bijection { eff = f, gee = g }
    where
      cmp = comparing $ \ (v, d) -> (Down d, v)
      g_assocs = zip (verticesUG uG) (map fst $ sortBy cmp $ degreesUG uG)
      f_assocs = sort [(x,y) | (y,x) <- g_assocs]
      !f_map = StrictMap.fromAscList f_assocs
      !g_map = StrictMap.fromAscList g_assocs
      f x = StrictMap.findWithDefault x x f_map
      g y = StrictMap.findWithDefault y y g_map


---------------------------------------------------------------------------
-- DIMACS2 parser

-- Parse DIMACS2 format and return number vertices 'n' and a list of edges;
-- vertices appearing in the list of edges are positive and bounded by 'n'.
parseDIMACS2 :: String -> (Int, [(Vertex,Vertex)])
parseDIMACS2 input =
  case stripDIMACS2Comments (map words $ lines input) of
    []   -> error "parseDIMACS2: no data"
    p:es -> (n, edges)
              where
                n = parseDIMACS2PLine p
                edges = map (parseDIMACS2ELine n) es

stripDIMACS2Comments :: [[String]] -> [[String]]
stripDIMACS2Comments = filter (\s -> not (null s || head (head s) == 'c'))

parseDIMACS2PLine :: [String] -> Int
parseDIMACS2PLine ("p":"edge":s:_) = n
  where
    n = case reads s of
          [(i,"")] -> if i > 0
                        then i
                        else error "parseDIMACS2: \"p\" line out of bounds (vertices)"
          _        -> error "parseDIMACS2: \"p\" line read error"
parseDIMACS2PLine _ = error "parseDIMACS2: \"p edge\" line missing or wrong"

parseDIMACS2ELine :: Int -> [String] -> (Vertex,Vertex)
parseDIMACS2ELine n  ["e", s1, s2] = (u,v)
  where
    u = case reads s1 of
          [(i,"")] -> if 1 <= i && i <= n
                        then i
                        else error "parseDIMACS2: \"e\" line out of bounds (source)"
          _        -> error "parseDIMACS2: \"e\" line read error (source)"
    v = case reads s2 of
          [(i,"")] -> if 1 <= i && i <= n
                        then i
                        else error "parseDIMACS2: \"e\" line out of bounds (sink)"
          _        -> error "parseDIMACS2: \"e\" line read error (sink)"
parseDIMACS2ELine _ _ = error "parseDIMCAS2: \"e\" line wrong"


---------------------------------------------------------------------------
-- vertex sets, represented as sets of Ints

type VertexSet = IntSet
  -- Supports operations fromAscLit, null, minView, delete,
  -- intersection, difference.


---------------------------------------------------------------------------
-- graphs, representation for max clique search

-- An undirected graph is represented as an array of adjacency sets,
-- which may be viewed as an adjacency matrix (of Booleans). This matrix
-- is assumed to be symmetric and irreflexive. Vertices are numbered from 0.
newtype Graph = G (Array Vertex VertexSet)
                deriving (Eq, Ord, Show, NFData, Serialize)

-- Converts an undirected graph (whose vertices must be numbered from 0)
-- into the array of adjacency sets representation.
mkG :: UGraph -> Graph
mkG [] = error "mkG: empty graph"
mkG uG = if head (head uG) == 0 
           then G $ listArray (0, n - 1) [VertexSet.fromAscList vs | _:vs <- uG]
           else error "mkG: vertices not numbered from 0"
             where
               n = length uG

verticesG :: Graph -> [Vertex]
verticesG (G g) = [l .. u] where (l,u) = bounds g

adjacentG :: Graph -> Vertex -> VertexSet
adjacentG (G g) v = g ! v

isAdjacentG :: Graph -> Vertex -> Vertex -> Bool
isAdjacentG bigG u v = VertexSet.member v $ adjacentG bigG u

degreeG :: Graph -> Vertex -> Int
degreeG bigG = VertexSet.size . adjacentG bigG

toClosureGraph :: Graph -> Closure Graph
toClosureGraph bigG = $(mkClosure [| toClosureGraph_abs bigG |])

toClosureGraph_abs :: Graph -> Thunk Graph
toClosureGraph_abs bigG = Thunk bigG


---------------------------------------------------------------------------
-- global immutable reference for graphs (replicated everywhere)

newECRefGraph :: Graph -> Par (ECRef Graph)
newECRefGraph bigG = do
  nodes <- allNodes
  newECRef ecDictGraphC nodes bigG

ecDictGraph :: ECRefDict Graph
ecDictGraph = ECRefDict { ECRef.toClosure = toClosureGraph,
                          ECRef.joinWith = \ _x _y -> Nothing }

ecDictGraphC :: Closure (ECRefDict Graph)
ecDictGraphC = $(mkClosure [| ecDictGraph |])


---------------------------------------------------------------------------
-- verification (of clique property, not of maximality)

-- True iff the given list of vertices form a clique.
isClique :: Graph -> [Vertex] -> Bool
isClique bigG vertices =
  and [isAdjacentG bigG u v | u <- vertices, v <- vertices, u /= v]


---------------------------------------------------------------------------
-- greedy colouring

-- Ordered vertex colouring, reprented as a list of vertex-colour pairs
-- (where colours are positive integers).
type ColourOrder = [(Vertex,Int)]

-- Greedy colouring of a set of vertices, as in [1]. Returns the list of
-- vertex-colour pairs in reverse order of colouring, i.e. the head of the
-- list is the vertex coloured last (with the highest colour).
colourOrder :: Graph -> VertexSet -> ColourOrder
colourOrder bigG bigP = colourFrom 1 bigP []
  where
    -- outer while loop; 'coloured' is a partial colouring
    colourFrom :: Int -> VertexSet -> ColourOrder -> ColourOrder
    colourFrom !colour uncoloured coloured =
      if VertexSet.null uncoloured
        then coloured
        else greedyWith colour uncoloured coloured uncoloured
    -- inner while loop; 'coloured' is a partial colouring
    greedyWith :: Int -> VertexSet -> ColourOrder -> VertexSet -> ColourOrder
    greedyWith !colour uncoloured coloured colourable =
      case VertexSet.minView colourable of
        Nothing ->
          -- 'colourable' is empty
          colourFrom (colour + 1) uncoloured coloured
        Just (v, colourable_minus_v) ->
          -- 'v' is minimal vertex in 'colourable'
          greedyWith colour uncoloured' coloured' colourable'
            where
              coloured'   = (v,colour):coloured
              uncoloured' = VertexSet.delete v uncoloured
              colourable' = VertexSet.difference colourable_minus_v $
                                                 adjacentG bigG v


---------------------------------------------------------------------------
-- path alphabet for maxclique tree iterator

data X = X [Vertex]   -- current clique
           Int        -- size of current clique
           VertexSet  -- candidate vertices: disj from curr clique but connected
           Int        -- number of colors to color candidate vertices
         deriving (Generic, Show)

instance NFData X
instance Serialize X

toClosurePath :: Path X -> Closure (Path X)
toClosurePath path = $(mkClosure [| toClosurePath_abs path |])

toClosurePath_abs :: Path X -> Thunk (Path X)
toClosurePath_abs path = Thunk path


-- path accessors
getClique :: Path X -> [Vertex]
getClique []                 = []
getClique (X bigC _ _ _ : _) = bigC

getCliquesize :: Path X -> Int
getCliquesize []                  = 0
getCliquesize (X _ sizeC _ _ : _) = sizeC

getCandidates :: Path X -> VertexSet
getCandidates []                 = VertexSet.empty  -- bogus value
getCandidates (X _ _ bigP _ : _) = bigP

getColours :: Path X -> Int
getColours []                     = -1        -- bogus value
getColours (X _ _ _ coloursP : _) = coloursP


-- path compression (possible because accessors use only last letter)
compressPath :: Path X -> Path X
compressPath []    = []
compressPath (x:_) = [x]

-- right (but not left) inverse of 'compressPath'
uncompressPath :: Path X -> Path X
uncompressPath []   = []
uncompressPath path = replicate (getCliquesize path) (head path)


---------------------------------------------------------------------------
-- solution type (including bound)

data Y = Y [Vertex]   -- current max clique
           Int        -- size of current max clique
         deriving (Eq, Ord, Generic, Show)

instance NFData Y
instance Serialize Y

toClosureY :: Y -> Closure Y
toClosureY y = $(mkClosure [| toClosureY_abs y |])

toClosureY_abs :: Y -> Thunk Y
toClosureY_abs y = Thunk y


---------------------------------------------------------------------------
-- global join-semilattice updateable solution

ecDictY :: ECRefDict Y
ecDictY =
  ECRefDict {
    ECRef.toClosure = toClosureY,
    ECRef.joinWith =
      \ (Y _ sizeC1) y2@(Y _ sizeC2) ->
        if sizeC2 <= sizeC1 then Nothing else Just y2
  }

ecDictYC :: Closure (ECRefDict Y)
ecDictYC = $(mkClosure [| ecDictY |])


-----------------------------------------------------------------------------
-- HdpH maxclique search using skeleton (with default task constructor)

-- BnB dictionary for depth-bounded task generation
bbDict :: ECRef Graph -> Int -> BBDict X Y
bbDict bigGRef task_depth =
  BBDict {
    bbToClosure = toClosurePath,
    bbGenerator =
      \ path -> do
        bigG <- readECRef' bigGRef
        let bigC  = getClique path
        let sizeC = getCliquesize path
        let bigP  | null path = VertexSet.fromAscList $ verticesG bigG
                  | otherwise = getCandidates path
        let vcs  = colourOrder bigG bigP
        let expand (v, colours) bigP =
              X (v:bigC)
                (sizeC + 1)
                (bigP `VertexSet.intersection` adjacentG bigG v)
                (colours - 1)
        return $
          zipWith expand vcs $
          tail $ scanr (VertexSet.insert . fst) VertexSet.empty vcs,
    bbObjective =
      \ path -> Y (getClique path) (getCliquesize path),
    bbDoPrune =
      \ path yRef -> do
        let sizeC    = getCliquesize path
        let coloursP = getColours path
        Y _ bound <- readECRef' yRef
        return $! not (null path) && sizeC + coloursP <= bound,
    bbIsTask =
      \ path -> length path == task_depth,
    bbMkTask = Nothing
  }

bbDictC :: ECRef Graph -> Int -> Closure (BBDict X Y)
bbDictC bigGRef task_depth =
  $(mkClosure [| bbDictC_abs (bigGRef, task_depth) |])

bbDictC_abs :: (ECRef Graph, Int) -> Thunk (BBDict X Y)
bbDictC_abs (bigGRef, task_depth) = Thunk $ bbDict bigGRef task_depth


-- sequential skeleton instantiation
maxClqSeq :: ECRef Graph -> Int -> Par ([Vertex], Int)
maxClqSeq bigGRef task_depth = do
  (Y clique _, !tasks) <- seqBB (bbDictC bigGRef task_depth) ecDictYC
  return (clique, tasks)


-- parallel skeleton instantiation
maxClqPar :: ECRef Graph -> Int -> Par ([Vertex], Int)
maxClqPar bigGRef task_depth = do
  (Y clique _, !tasks) <- parBB (bbDictC bigGRef task_depth) ecDictYC
  return (clique, tasks)


-----------------------------------------------------------------------------
-- HdpH maxclique search using skeleton (with optimised task constructor)

-- BnB dictionary for depth-bounded task generation
bbDictOpt :: ECRef Graph -> Int -> BBDict X Y
bbDictOpt bigGRef task_depth =
  (bbDict bigGRef task_depth) {
    bbMkTask = Just $ optMkTask bigGRef
  }

bbDictOptC :: ECRef Graph -> Int -> Closure (BBDict X Y)
bbDictOptC bigGRef task_depth =
  $(mkClosure [| bbDictOptC_abs (bigGRef, task_depth) |])

bbDictOptC_abs :: (ECRef Graph, Int) -> Thunk (BBDict X Y)
bbDictOptC_abs (bigGRef, task_depth) = Thunk $ bbDictOpt bigGRef task_depth


-- optimised sequential skeleton instantiation
maxClqSeqOpt :: ECRef Graph -> Int -> Par ([Vertex], Int)
maxClqSeqOpt bigGRef task_depth = do
  (Y clique _, !tasks) <- seqBB (bbDictOptC bigGRef task_depth) ecDictYC
  return (clique, tasks)


-- optimised parallel skeleton instantiation
maxClqParOpt :: ECRef Graph -> Int -> Par ([Vertex], Int)
maxClqParOpt bigGRef task_depth = do
  (Y clique _, !tasks) <- parBB (bbDictOptC bigGRef task_depth) ecDictYC
  return (clique, tasks)


-- Optimised task generation (std B&B search instead of inner tree iterator)
optMkTask :: ECRef Graph -> Path X -> ECRef Y -> Closure (Par (Closure ()))
optMkTask bigGRef path0 yRef =
  $(mkClosure [| optMkTask_abs (bigGRef, path0C, yRef) |])
    where
      path0C = toClosurePath $ compressPath path0

optMkTask_abs :: (ECRef Graph, Closure (Path X), ECRef Y)
              -> Thunk (Par (Closure ()))
optMkTask_abs (bigGRef, path0C, yRef) = Thunk $ do
  !bigG <- readECRef' bigGRef
  let path0 = unClosure path0C
  let y0 = Y (getClique path0) (getCliquesize path0)
  let bigPee | null path0 = VertexSet.fromAscList $ verticesG bigG
             | otherwise  = getCandidates path0
  unless (VertexSet.null bigPee) $
    search bigG yRef y0 bigPee
  return unitC


-- Branch-and-bound maxclique search, as in [1].
-- Takes the input graph, a referenct to the current bound and incumbent,
-- the currently explored solution, and a set of candidate vertices to be
-- added to the currently explored solution.
-- Updates the bound and incumbent when a better solution is found.
-- NOTE: Expects 'bigPee' to be non-empty.
search :: Graph       -- input graph
       -> ECRef Y     -- stores current best solution (incumbent & bound)
       -> Y           -- currently explored solution (clique & size)
       -> VertexSet   -- candidate vertices (to add to current clique)
       -> Par ()
search bigG yRef y bigPee =
  loop y bigPee $ colourOrder bigG bigPee
    where
      -- for loop
      loop :: Y            -- current clique with size
           -> VertexSet    -- candidate vertices
           -> ColourOrder  -- ordered colouring of candidate vertices
           -> Par ()
      loop _               _    []                = return ()
      loop (Y bigC !sizeC) bigP ((v,colour):more) = do
        Y _ bound <- readECRef' yRef
        if sizeC + colour <= bound
          then return ()
          else do
            -- accept v
            let y' = Y (v:bigC) $! (sizeC + 1)
            _ <- writeECRef yRef y'
            let bigP' = VertexSet.intersection bigP $ adjacentG bigG v
            -- recurse (unless bigP' empty)
            unless (VertexSet.null bigP') $
              search bigG yRef y' bigP'
            -- reject v
            let bigP'' = VertexSet.delete v bigP
            -- continue the loop
            loop y bigP'' more


-----------------------------------------------------------------------------
-- Static declaration (just before 'main')

-- Empty splice; TH hack to make all environment abstractions visible.
$(return [])

declareStatic :: StaticDecl
declareStatic = mconcat [HdpH.declareStatic,
                         ECRef.declareStatic,
                         declare $(static 'defaultMkTask_abs),
                         declare $(static 'toClosureGraph_abs),
                         declare $(static 'ecDictGraph),
                         declare $(static 'toClosurePath_abs),
                         declare $(static 'toClosureY_abs),
                         declare $(static 'ecDictY),
                         declare $(static 'bbDictC_abs),
                         declare $(static 'bbDictOptC_abs),
                         declare $(static 'optMkTask_abs)]


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
  liftIO $ putStrLn "Usage: maxclique [OPTIONS] FILE"
  liftIO $ putStrLn "  -perm, -noperm: Permute vertices into desc degree order"
  liftIO $ putStrLn "  -vN: Nth version of the algoritm (default N=0)"
  liftIO $ putStrLn "  -dN: depth bound (for bounded iterator; default N=0)"
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

parsePermutation :: String -> Maybe Bool
parsePermutation "-perm"   = Just True
parsePermutation "-noperm" = Just False
parsePermutation _         = Nothing

parseFilename :: String -> Maybe String
parseFilename ""      = Nothing
parseFilename ('-':_) = Nothing
parseFilename s       = Just s

parseArgs :: [String] -> (Bool, Int, Int, String)
parseArgs = foldl parse (True, 0, 0, "")
  where
    parse (perm, ver, depth, file) s =
      maybe id upd1 (parsePermutation s) $
      maybe id upd2 (parseVersion s) $
      maybe id upd3 (parseDepth s) $
      maybe id upd4 (parseFilename s) $ (perm, ver, depth, file)
    upd1 y ( _, x2, x3, x4) = (y, x2, x3, x4)
    upd2 y (x1,  _, x3, x4) = (x1, y, x3, x4)
    upd3 y (x1, x2,  _, x4) = (x1, x2, y, x4)
    upd4 y (x1, x2, x3,  _) = (x1, x2, x3, y)


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
  (conf, args) <- parseOpts opts_args
  runParIO_ conf $ do
    -- parsing maxclique cmd line arguments (no real error checking)
    let (permute, version, depth, filename) = parseArgs args
    io $ putStrLn $
      "MaxClique" ++ " -v" ++ show version ++
      (if depth >= 0 then " -d" ++ show depth else "") ++
      (if permute then " -perm" else " -noperm") ++
      " " ++ filename
    -- reading input graph
    (uG, t_read) <- timeM' $ do
      input <- io $ if null filename then getContents else readFile filename
      let (n, edges) = parseDIMACS2 input
      let uG' = mkUG n edges
      return $!! uG'
    io $ putStrLn $ "t_read: " ++ show t_read
    -- printing some statistics
    let degrees = map snd $ degreesUG uG
    let n = length degrees
    let min_deg = minimum degrees
    let max_deg = maximum degrees
    let avg_deg = sum (map toFractional degrees) / toFractional n :: Double
    io $ putStrLn $ "vertices: " ++ show n
    io $ putStrLn $ "max degree: " ++ show max_deg
    io $ putStrLn $ "min degree: " ++ show min_deg
    io $ putStrLn $ "avg degree: " ++ show avg_deg
    -- permuting and converting input graph
    ((alpha, bigG), t_permute) <- timeM' $ do
      let alpha' | permute   = antiMonotonizeDegreesPermUG uG
                 | otherwise = Cat.id
      let uG_alpha = appUG (inv permHH <<< alpha') uG
          -- uG_alpha in non-decreasing degree order, vertices numbered from 0.
      let !bigG' = mkG uG_alpha
      return (alpha', bigG')
--  BEGIN DEBUG
--  let new_vertices = verticesG bigG
--  let new_degrees  = map (degreeG bigG) new_vertices
--  let old_vertices = map (app (inv alpha <<< permHH)) new_vertices
--  io $ putStrLn $ unlines $ map show $
--         zip3 new_vertices old_vertices new_degrees
--  END DEBUG
    if permute
      then io $ putStrLn $ "t_permute: " ++ show t_permute
      else io $ putStrLn $ "t_construct: " ++ show t_permute
    -- distributing input graph to all nodes
    (bigGRef, t_distribute) <- timeM' $ do
      newECRefGraph bigG
    io $ putStrLn $ "t_distribute: " ++ show t_distribute
    -- dispatch on version
    ((bigCstar, info), t_compute) <- timeM' $ case version of
      0 -> force <$> maxClqSeq bigGRef depth     -- sequential; unoptimised
      1 -> force <$> maxClqSeqOpt bigGRef depth  -- sequential; optimised
      2 -> force <$> maxClqPar bigGRef depth     -- parallel; unoptimised
      3 -> force <$> maxClqParOpt bigGRef depth  -- parallel; optimised
      _ -> usage
    -- deallocating input graph
    ((), t_deallocate) <- timeM' $ do
      freeECRef bigGRef
    io $ putStrLn $ "t_deallocate: " ++ show t_deallocate
    -- print solution and statistics
    let bigCstar_alpha_inv = map (app (inv alpha <<< permHH)) bigCstar
    io $ putStrLn $ "     C*: " ++ show bigCstar_alpha_inv
    io $ putStrLn $ "sort C*: " ++ show (sort bigCstar_alpha_inv)
    io $ putStrLn $ "size: " ++ show (length bigCstar)
    io $ putStrLn $ "isClique: " ++ show (isClique bigG bigCstar)
    unless (info < 0) $
      io $ putStrLn $ "tasks: " ++ show info
    io $ putStrLn $ "t_compute: " ++ show t_compute
  -- the end
  exitSuccess
