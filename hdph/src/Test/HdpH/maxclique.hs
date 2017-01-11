-- maxclique.hs
--
-- Author: Patrick Maier <Patrick.Maier@glasgow.ac.uk>
-- Date: 27 Dec 2016
--
-- Max clique algorithm (described in [1]) implemented in parallel using
-- tree iterators; sets of vertices represented via Data.IntSet.
-- * Shared-memory parallel version using Haskell concurrency
-- * Distributed-memory parallel version using HdpH
-- 
-- References:
-- [1] Ciaran McCreesh. Algorithms for the Maximum Clique Problem,
--     and How to Parallelise Them. 6 June 2014.
--
---------------------------------------------------------------------------

{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE FlexibleInstances #-}   -- for MonadIO instance
{-# LANGUAGE TemplateHaskell #-}

{-# OPTIONS_GHC -fno-warn-orphans #-}

module Main where

import Prelude
import Control.Category (Category, (<<<))
import qualified Control.Category as Cat (id, (.))
import Control.Concurrent (forkIO)
import Control.Concurrent.Chan (Chan, newChan, writeChan, readChan)
import Control.Concurrent.QSem (newQSem, signalQSem, waitQSem)
import Control.DeepSeq (NFData(rnf), ($!!), force)
import Control.Monad (when, unless, forever, replicateM_)
import Control.Monad.IO.Class (MonadIO(liftIO))
import Data.Array (Array, bounds, (!), listArray)
import Data.IntSet (IntSet)
import qualified Data.IntSet as VertexSet
       (fromAscList, null, size, member,
        minView, empty, insert, delete, intersection, difference)
import Data.IORef (IORef, newIORef, readIORef, atomicModifyIORef')
import Data.List (group, groupBy, sort, sortBy, isPrefixOf)
import qualified Data.IntMap.Strict as StrictMap (fromAscList, findWithDefault)
import Data.Ord (comparing, Down(Down))
import Data.Serialize (Serialize)
import qualified Data.Serialize (put, get)
import Data.Time.Clock (NominalDiffTime, diffUTCTime, getCurrentTime)
import System.Environment (getArgs)
import System.Exit (exitFailure, exitSuccess)
import System.IO (stdout, stderr, hSetBuffering, BufferMode(..))

import Control.Parallel.HdpH
       (RTSConf, defaultRTSConf, updateConf,
        Par, runParIO_,
        GIVar, myNode, allNodes, io, spark, new, glob, rput, get,
        Thunk(Thunk), Closure, mkClosure, unitC,
        StaticDecl, static, declare, register)
import qualified Control.Parallel.HdpH as HdpH (declareStatic)
import Control.Parallel.HdpH.Dist (one)
import Aux.ECRef
       (ECRef, ECRefDict(ECRefDict),
        newECRef, freeECRef, readECRef', writeECRef, gatherECRef')
import qualified Aux.ECRef as ECRef (declareStatic, toClosure, joinWith)
import qualified Aux.Iter as Iter
import qualified Aux.TreeIter as TreeIter


---------------------------------------------------------------------------
-- monotonically updatable mutable variable in a join-semilattice

-- join-semilattices
class SL a where
  join     :: a -> a -> a     -- associative, commutative, idempotent
  leq      :: a -> a -> Bool  -- reflexive, transitive, antisymmetric
  joinWith :: a -> a -> Maybe a

  -- minimal complete definition: joinWith, or join and leq
  x `join` y = case joinWith x y of { Nothing -> x; Just z -> z }
  y `leq` x  = case joinWith x y of { Nothing -> True; _ -> False }
  joinWith x y | y `leq` x = Nothing
               | otherwise = Just (x `join` y)


-- mutable semilattice variables
newtype SLRef a = SLRef (IORef a)

newSLRef :: a -> IO (SLRef a)
newSLRef x = fmap SLRef $ newIORef x

readSLRef :: SLRef a -> IO a
readSLRef (SLRef ref) = readIORef ref

-- Updates the variable to hold the join of the old value and the given value;
-- returns Nothing if the variable didn't change; returns new value otherwise.
writeSLRef :: (SL a) => SLRef a -> a -> IO (Maybe a)
writeSLRef (SLRef ref) y =
  atomicModifyIORef' ref $ \ old_x ->
    case joinWith old_x y of
      Nothing    -> (old_x, Nothing)
      Just new_x -> (new_x, Just new_x)


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
-- maxclique search (simple recursive B&B)

-- Branch-and-bound maxclique search, as in [1].
-- Takes the input graph, the current bound and incumbent, the currently
-- explored clique 'bigCee', and a set of candidate vertices 'bigPee' (all
-- connected to all vertices in 'bigCee') to be added to extensions of 'bigCee'.
-- Returns the maximum clique extending 'bigCee' by vertices in 'bigPee' and
-- the clique size.
-- NOTE: Expects 'bigPee' to be non-empty.
expand :: Graph             -- input graph
       -> ([Vertex],Int)    -- current best solution (incumbent and bound)
       -> ([Vertex],Int)    -- currently explored solution (clique and size)
       -> VertexSet         -- candidate vertices (to add to current clique)
       -> ([Vertex],Int)    -- result: max clique + its size
expand bigG incumbent_bound bigCee_size bigPee =
  loop incumbent_bound bigCee_size bigPee $ colourOrder bigG bigPee
    where
      -- for loop
      loop :: ([Vertex], Int)      -- incumbent and bound
           -> ([Vertex], Int)      -- current clique with size
           -> VertexSet            -- candidate vertices
           -> ColourOrder          -- ordered colouring of candidate vertices
           -> ([Vertex], Int)      -- result: max clique and size
      loop (incumbent,!bound) _            _    []                =
        (incumbent,bound)
      loop (incumbent,!bound) (bigC,!size) bigP ((v,colour):more) =
        if size + colour <= bound
          then (incumbent,bound)
          else let
            -- accept v
            (bigC',!size')       = (v:bigC, size + 1)
            (incumbent',!bound') = if size' > bound
                                     then (bigC',    size')  -- new incumbent!
                                     else (incumbent,bound)
            bigP' = VertexSet.intersection bigP $ adjacentG bigG v
            -- recurse (unless bigP' empty)
            (incumbent'',!bound'') =
              if VertexSet.null bigP'
                then (incumbent',bound')
                else expand bigG (incumbent',bound') (bigC',size') bigP'
            -- reject v
            bigP'' = VertexSet.delete v bigP
            -- continue the loop
          in loop (incumbent'',bound'') (bigC,size) bigP'' more

-- Same as 'expand' but 'bigPee' may be empty.
expand' :: Graph -> ([Vertex],Int) -> ([Vertex],Int) -> VertexSet
        -> ([Vertex],Int)
expand' bigG (incumbent, bound) bigCee_size bigPee =
  if VertexSet.null bigPee then
    (incumbent, bound)
  else
    expand bigG (incumbent, bound) bigCee_size bigPee


maxClq :: Graph -> ([Vertex], Int)
maxClq bigG = (fst $ expand' bigG ([],0) ([],0) bigP, -1)
  where
    bigP = VertexSet.fromAscList $ verticesG bigG


---------------------------------------------------------------------------
-- tree iterator for maxclique search

-- space type
data X = X [Vertex]   -- current clique
           Int        -- size of current clique
           VertexSet  -- candidate vertices: disj from curr clique but connected
           Int        -- number of colors to color candidate vertices
         deriving (Show)

instance NFData X where
  rnf (X bigC sizeC bigP coloursP) =
    rnf bigC `seq` rnf sizeC `seq` rnf bigP `seq` rnf coloursP `seq` ()

instance Serialize X where
  put (X bigC sizeC bigP coloursP) = do Data.Serialize.put bigC
                                        Data.Serialize.put sizeC
                                        Data.Serialize.put bigP
                                        Data.Serialize.put coloursP
  get = do bigC     <- Data.Serialize.get
           sizeC    <- Data.Serialize.get
           bigP     <- Data.Serialize.get
           coloursP <- Data.Serialize.get
           return $ X bigC sizeC bigP coloursP

-- path accessors
getClique :: TreeIter.Path X -> [Vertex]
getClique []                 = []
getClique (X bigC _ _ _ : _) = bigC

getCliquesize :: TreeIter.Path X -> Int
getCliquesize []                  = 0
getCliquesize (X _ sizeC _ _ : _) = sizeC

getCandidates :: TreeIter.Path X -> VertexSet
getCandidates []                 = VertexSet.empty
getCandidates (X _ _ bigP _ : _) = bigP

getColours :: TreeIter.Path X -> Int
getColours []                     = -1
getColours (X _ _ _ coloursP : _) = coloursP

-- path compression (possible because tree gen and obj fun use only last X)
compressPath :: TreeIter.Path X -> TreeIter.Path X
compressPath []    = []
compressPath (x:_) = [x]

-- right inverse of 'compressPath'
uncompressPath :: TreeIter.Path X -> TreeIter.Path X
uncompressPath []   = []
uncompressPath path = replicate (getCliquesize path) (head path)


-- solution type
data Y = Y [Vertex]   -- current max clique
           Int        -- size of current max clique
         deriving (Eq, Ord, Show)

instance NFData Y where
  rnf (Y bigC sizeC) = rnf bigC `seq` rnf sizeC `seq` ()

instance Serialize Y where
  put (Y bigC sizeC) = do Data.Serialize.put bigC
                          Data.Serialize.put sizeC
  get = do bigC  <- Data.Serialize.get
           sizeC <- Data.Serialize.get
           return $ Y bigC sizeC

instance SL Y where
  joinWith (Y _ sizeC1) y2@(Y _ sizeC2) | sizeC2 <= sizeC1  = Nothing
                                        | otherwise         = Just y2

-- objective function
f :: TreeIter.Path X -> Y
f []                     = Y [] 0
f (X bigC sizeC _ _ : _) = Y bigC sizeC

-- ordered tree generator, pruning
g :: Graph -> SLRef Y -> TreeIter.Path X -> IO [X]
g bigG _ [] = do
  let bigP = VertexSet.fromAscList $ verticesG bigG
  let vcs = colourOrder bigG bigP
  return $
    zipWith (g_expand bigG [] 0) vcs $
    tail $ scanr (VertexSet.insert . fst) VertexSet.empty vcs
g bigG boundRef (X bigC sizeC bigP coloursP : _) = do
  Y _ bound <- readSLRef boundRef
  if sizeC + coloursP <= bound
    then return []    -- prune subtree
    else if VertexSet.null bigP
      then return []  -- close branch
      else do
        let vcs = colourOrder bigG bigP
        return $
           zipWith (g_expand bigG bigC sizeC) vcs $
           tail $ scanr (VertexSet.insert . fst) VertexSet.empty vcs

g_expand :: Graph -> [Vertex] -> Int -> (Vertex, Int) -> VertexSet -> X
g_expand bigG bigC sizeC (v, colours) bigP =
  X (v:bigC) (sizeC + 1) bigP' (colours - 1)
    where
      bigP' = bigP `VertexSet.intersection` adjacentG bigG v

{-# SPECIALIZE TreeIter.newTreeIterM :: TreeIter.Path a
                                     -> TreeIter.GeneratorM IO a
                                     -> IO (TreeIter.TreeIterM IO a) #-}
{-# SPECIALIZE Iter.nextIterM :: Iter.IterM IO s a -> IO (Maybe a) #-}


---------------------------------------------------------------------------
-- maxclique search by running tree iterator

maxClqIter :: Graph -> Bool -> IO ([Vertex], Int)
maxClqIter bigG debug = do
  boundRef <- newSLRef (Y [] 0)
  maxClqSearchAt bigG boundRef [] debug
  Y bigC _ <- readSLRef boundRef
  return (bigC, -1)

maxClqSearchAt :: Graph -> SLRef Y -> TreeIter.Path X -> Bool -> IO ()
maxClqSearchAt bigG boundRef path0 debug = do
  when debug $ do
    Y _ bound <- readSLRef boundRef
    let n = getCliquesize path0
    let c = getColours path0
    let op = if n + c <= bound then " <= " else " > "
    putStrLn $ "BEG " ++ show (getClique path0) ++
               " {" ++ show n ++ "+" ++ show c ++ op ++ show bound ++ "}"
  TreeIter.newTreeIterM path0 (g bigG boundRef) >>= loop
  when debug $ do
    Y _ bound <- readSLRef boundRef
    putStrLn $ "END " ++ show (getClique path0) ++ " {" ++ show bound ++ "}"
    where
      loop iter = do
        maybe_path <- Iter.nextIterM iter
        case maybe_path of
          Nothing   -> return ()
          Just path -> do
            maybe_Y <- writeSLRef boundRef (f path)
            when debug $
              case maybe_Y of
                Nothing              -> return ()
                Just (Y _ new_bound) -> do
                  putStrLn $ "UPD " ++ show (getClique path0) ++
                             " {" ++ show new_bound ++ "}"
            loop iter


---------------------------------------------------------------------------
-- maxclique search by running depth-bounded tree iterator

-- NOTE: 2nd component of result is number of "tasks" at `depth`.
maxClqBndIter :: Graph -> Int -> Bool -> IO ([Vertex], Int)
maxClqBndIter bigG depth debug = do
  let isDepth path = length path == depth
  boundRef <- newSLRef (Y [] 0)
  iter <- TreeIter.newPruneTreeIterM isDepth [] (g bigG boundRef)
  tasks <- loop 0 boundRef iter
  Y bigC _ <- readSLRef boundRef
  return (bigC, tasks)
    where
      loop !tasks boundRef iter = do
        maybe_path0 <- Iter.nextIterM iter
        case maybe_path0 of
          Nothing    -> return tasks
          Just path0 -> do
            maxClqSearchAt bigG boundRef (compressPath path0) debug
            loop (tasks + 1) boundRef iter


---------------------------------------------------------------------------
-- parallel maxclique search by running depth-bounded tree iterator

-- NOTE: 2nd component of result is number of tasks at `depth`.
maxClqBndIterPar :: Graph -> Int -> Int -> Bool -> IO ([Vertex], Int)
maxClqBndIterPar bigG depth workers debug = do
  taskCh <- newChan
  replicateM_ workers $ forkIO $ forever $ do { task <- readChan taskCh; task }
  let isDepth path = length path == depth
  boundRef <- newSLRef (Y [] 0)
  doneCh <- newChan
  iter <- TreeIter.newPruneTreeIterM isDepth [] (g bigG boundRef)
  tasks <- forkLoop 0 taskCh boundRef doneCh iter
  joinLoop tasks doneCh
  Y bigC _ <- readSLRef boundRef
  return (bigC, tasks)
    where
      forkLoop !forked taskCh boundRef doneCh iter = do
        maybe_path0 <- Iter.nextIterM iter
        case maybe_path0 of
          Nothing    -> return forked
          Just path0 -> do
            writeChan taskCh $ do
              maxClqSearchAt bigG boundRef (compressPath path0) debug
              writeChan doneCh ()
            forkLoop (forked + 1) taskCh boundRef doneCh iter

joinLoop :: Int -> Chan () -> IO ()
joinLoop await doneCh
  | await == 0 = return ()
  | otherwise  = do { readChan doneCh; joinLoop (await - 1) doneCh }


---------------------------------------------------------------------------
-- parallel maxclique search by running depth-bounded tree iterator

-- NOTE: 2nd component of result is number of tasks at `depth`.
maxClqBufBndIterPar :: Graph -> Int -> Int -> Bool -> IO ([Vertex], Int)
maxClqBufBndIterPar bigG depth workers debug = do
  taskCh <- newChan
  replicateM_ workers $ forkIO $ forever $ do { task <- readChan taskCh; task }
  let isDepth path = length path == depth
  boundRef <- newSLRef (Y [] 0)
  doneCh <- newChan
  bufSem <- newQSem (10 * workers)
  iter <- TreeIter.newPruneTreeIterM isDepth [] (g bigG boundRef)
  tasks <- forkLoop 0 taskCh boundRef doneCh bufSem iter
  joinLoop tasks doneCh
  Y bigC _ <- readSLRef boundRef
  return (bigC, tasks)
    where
      forkLoop !forked taskCh boundRef doneCh bufSem iter = do
        maybe_path0 <- Iter.nextIterM iter
        case maybe_path0 of
          Nothing    -> return forked
          Just path0 -> do
            waitQSem bufSem
            writeChan taskCh $ do
              signalQSem bufSem
              maxClqSearchAt bigG boundRef (compressPath path0) debug
              writeChan doneCh ()
            forkLoop (forked + 1) taskCh boundRef doneCh bufSem iter


---------------------------------------------------------------------------
-- verification (of clique property, not of maximality)

-- True iff the given list of vertices form a clique.
isClique :: Graph -> [Vertex] -> Bool
isClique bigG vertices =
  and [isAdjacentG bigG u v | u <- vertices, v <- vertices, u /= v]


---------------------------------------------------------------------------
-- distributed graphs (immutable)
--
-- Could use IMRefs from ECRef module;
-- won't do that here to demonstrate how to create immutable ECRefs.

newECRefGraph :: Graph -> Par (ECRef Graph)
newECRefGraph bigG = do
  nodes <- allNodes
  newECRef dictGraphC nodes bigG

toClosureGraph :: Graph -> Closure Graph
toClosureGraph bigG = $(mkClosure [| toClosureGraph_abs bigG |])

toClosureGraph_abs :: Graph -> Thunk Graph
toClosureGraph_abs bigG = Thunk bigG

dictGraph :: ECRefDict Graph
dictGraph = ECRefDict { ECRef.toClosure = toClosureGraph,
                        ECRef.joinWith = \ _x _y -> Nothing }

dictGraphC :: Closure (ECRefDict Graph)
dictGraphC = $(mkClosure [| dictGraph |])


---------------------------------------------------------------------------
-- distributed maxclique solution (join-semilattice updateable)

newECRefY :: Y -> Par (ECRef Y)
newECRefY y = do
  nodes <- allNodes
  newECRef dictYC nodes y

toClosureY :: Y -> Closure Y
toClosureY y = $(mkClosure [| toClosureY_abs y |])

toClosureY_abs :: Y -> Thunk Y
toClosureY_abs y = Thunk y

dictY :: ECRefDict Y
dictY = ECRefDict { ECRef.toClosure = toClosureY,
                    ECRef.joinWith = joinWith }

dictYC :: Closure (ECRefDict Y)
dictYC = $(mkClosure [| dictY |])


---------------------------------------------------------------------------
-- MonadIO instance for Par (orphan instance)

instance MonadIO Par where
  liftIO = io


---------------------------------------------------------------------------
-- HdpH maxclique search (depth-bounded tree iterator)

-- NOTE: 2nd component of result is number of tasks at `depth`.
maxClqBndIterHdpH :: ECRef Graph -> Int -> Bool -> Par ([Vertex], Int)
maxClqBndIterHdpH bigGRef depth debug = do
  bigG <- readECRef' bigGRef
  boundRef <- newECRefY (Y [] 0)
  let isDepth path = length path == depth
  iter <- TreeIter.newPruneTreeIterM isDepth [] (gPar bigG boundRef)
  ivars <- sparkLoop [] boundRef iter
  let !tasks = length ivars
  mapM_ get ivars  -- block until all sparked tasks are completed
  Y bigC _ <- gatherECRef' boundRef  -- readECRef' should do here, too
  freeECRef boundRef
  return (bigC, tasks)
    where
      sparkLoop ivars boundRef iter = do
        maybe_path0 <- Iter.nextIterM iter
        case maybe_path0 of
          Nothing    -> return $ reverse ivars
          Just path0 -> do
            let pth0 = compressPath path0  -- compress to save on serialisation
            ivar <- new
            gv <- glob ivar
            spark one $(mkClosure [| maxClqBndIterHdpH_abs
                                     (bigGRef, boundRef, pth0, debug, gv) |])
            sparkLoop (ivar:ivars) boundRef iter

maxClqBndIterHdpH_abs :: (ECRef Graph,
                          ECRef Y,
                          TreeIter.Path X,
                          Bool,
                          GIVar (Closure ()))
                      -> Thunk (Par ())
maxClqBndIterHdpH_abs (bigGRef, boundRef, pth0, debug, gv) =
  Thunk $ maxClqSearchAtPar bigGRef boundRef pth0 debug >> rput gv unitC


-- ordered tree generator in Par monad, pruning;
-- variant of 'g' with differs mainly in type of 'boundRef'
gPar :: Graph -> ECRef Y -> TreeIter.Path X -> Par [X]
gPar bigG _ [] = do
  let bigP = VertexSet.fromAscList $ verticesG bigG
  let vcs = colourOrder bigG bigP
  return $
    zipWith (g_expand bigG [] 0) vcs $
    tail $ scanr (VertexSet.insert . fst) VertexSet.empty vcs
gPar bigG boundRef (X bigC sizeC bigP coloursP : _) = do
  Y _ bound <- readECRef' boundRef
  if sizeC + coloursP <= bound
    then return []    -- prune subtree
    else if VertexSet.null bigP
      then return []  -- close branch
      else do
        let vcs = colourOrder bigG bigP
        return $
           zipWith (g_expand bigG bigC sizeC) vcs $
           tail $ scanr (VertexSet.insert . fst) VertexSet.empty vcs


-- variant of 'maxClqSearchAt' for Par monad (and ECRefs)
maxClqSearchAtPar :: ECRef Graph -> ECRef Y -> TreeIter.Path X -> Bool -> Par ()
maxClqSearchAtPar bigGRef boundRef path0 debug = do
  bigG <- readECRef' bigGRef
  when debug $ do
    me <- myNode
    Y _ bound <- readECRef' boundRef
    let n = getCliquesize path0
    let c = getColours path0
    let op = if n + c <= bound then " <= " else " > "
    io $ putStrLn $ show me ++ " BEG " ++ show (getClique path0) ++
                    " {" ++ show n ++ "+" ++ show c ++ op ++ show bound ++ "}"
  TreeIter.newTreeIterM path0 (gPar bigG boundRef) >>= loop
  when debug $ do
    me <- myNode
    Y _ bound <- readECRef' boundRef
    io $ putStrLn $ show me ++ " END " ++ show (getClique path0) ++
                    " {" ++ show bound ++ "}"
    where
      loop iter = do
        maybe_path <- Iter.nextIterM iter
        case maybe_path of
          Nothing   -> return ()
          Just path -> do
            maybe_Y <- writeECRef boundRef (f path)
            when debug $ do
              me <- myNode
              case maybe_Y of
                Nothing              -> return ()
                Just (Y _ new_bound) -> do
                  io $ putStrLn $ show me ++ " UPD " ++
                                  show (getClique path0) ++
                                  " {" ++ show new_bound ++ "}"
            loop iter

{-# SPECIALIZE TreeIter.newTreeIterM :: TreeIter.Path a
                                     -> TreeIter.GeneratorM Par a
                                     -> Par (TreeIter.TreeIterM Par a) #-}
{-# SPECIALIZE Iter.nextIterM :: Iter.IterM Par s a -> Par (Maybe a) #-}


---------------------------------------------------------------------------
-- HdpH maxclique search (depth-bounded tree iter + standard alg for seq tasks)

-- NOTE: 2nd component of result is number of tasks at `depth`.
maxClqBndIterHdpHv2 :: ECRef Graph -> Int -> Bool -> Par ([Vertex], Int)
maxClqBndIterHdpHv2 bigGRef depth debug = do
  bigG <- readECRef' bigGRef
  boundRef <- newECRefY (Y [] 0)
  let isDepth path = length path == depth
  iter <- TreeIter.newPruneTreeIterM isDepth [] (gPar bigG boundRef)
  ivars <- sparkLoop [] boundRef iter
  let !tasks = length ivars
  mapM_ get ivars  -- block until all sparked tasks are completed
  Y bigC _ <- gatherECRef' boundRef  -- readECRef' should do here, too
  freeECRef boundRef
  return (bigC, tasks)
    where
      sparkLoop ivars boundRef iter = do
        maybe_path0 <- Iter.nextIterM iter
        case maybe_path0 of
          Nothing    -> return $ reverse ivars
          Just path0 -> do
            let pth0 = compressPath path0  -- compress to save on serialisation
            ivar <- new
            gv <- glob ivar
            spark one $(mkClosure [| maxClqBndIterHdpHv2_abs
                                     (bigGRef, boundRef, pth0, debug, gv) |])
            sparkLoop (ivar:ivars) boundRef iter

maxClqBndIterHdpHv2_abs :: (ECRef Graph,
                            ECRef Y,
                            TreeIter.Path X,
                            Bool,
                            GIVar (Closure ()))
                        -> Thunk (Par ())
maxClqBndIterHdpHv2_abs (bigGRef, boundRef, pth0, _debug, gv) =
  Thunk $ maxClqSearchAtPar' bigGRef boundRef pth0 >> rput gv unitC


-- variant of 'maxClqSearchAtPar' based on standard algo (rather than tree iter)
maxClqSearchAtPar' :: ECRef Graph -> ECRef Y -> TreeIter.Path X -> Par ()
maxClqSearchAtPar' bigGRef boundRef path0 = do
  bigG <- readECRef' bigGRef
  let y = Y (getClique path0) (getCliquesize path0)
  let bigPee | null path0 = VertexSet.fromAscList $ verticesG bigG
             | otherwise  = getCandidates path0
  unless (VertexSet.null bigPee) $
    expandPar bigG boundRef y bigPee

-- variant of 'expand' for the Par monad (+ sharing incumbent via ECRef).
-- NOTE: Expects 'bigPee' to be non-empty.
expandPar :: Graph       -- input graph
          -> ECRef Y     -- stores current best solution (incumbent & bound)
          -> Y           -- currently explored solution (clique & size)
          -> VertexSet   -- candidate vertices (to add to current clique)
          -> Par ()
expandPar bigG boundRef y bigPee =
  loop y bigPee $ colourOrder bigG bigPee
    where
      -- for loop
      loop :: Y            -- current clique with size
           -> VertexSet    -- candidate vertices
           -> ColourOrder  -- ordered colouring of candidate vertices
           -> Par ()
      loop _               _    []                = return ()
      loop (Y bigC !sizeC) bigP ((v,colour):more) = do
        Y _ bound <- readECRef' boundRef
        if sizeC + colour <= bound
          then return ()
          else do
            -- accept v
            let y' = Y (v:bigC) $! (sizeC + 1)
            _ <- writeECRef boundRef y'
            let bigP' = VertexSet.intersection bigP $ adjacentG bigG v
            -- recurse (unless bigP' empty)
            unless (VertexSet.null bigP') $
              expandPar bigG boundRef y' bigP'
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
                         declare $(static 'toClosureGraph_abs),
                         declare $(static 'toClosureY_abs),
                         declare $(static 'dictGraph),
                         declare $(static 'dictY),
                         declare $(static 'maxClqBndIterHdpH_abs),
                         declare $(static 'maxClqBndIterHdpHv2_abs)]


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
  liftIO $ putStrLn "  -nN: workers (for parallel search; default N=0)"
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

parseWorkers :: String -> Maybe Int
parseWorkers s | isPrefixOf "-n" s = case reads $ drop 2 s of
                                       [(n, "")]  -> Just n
                                       _          -> Nothing
               | otherwise         = Nothing

parsePermutation :: String -> Maybe Bool
parsePermutation "-perm"   = Just True
parsePermutation "-noperm" = Just False
parsePermutation _         = Nothing

parseDebug :: String -> Maybe Bool
parseDebug "-debug" = Just True
parseDebug _        = Nothing

parseFilename :: String -> Maybe String
parseFilename ""      = Nothing
parseFilename ('-':_) = Nothing
parseFilename s       = Just s

parseArgs :: [String] -> (Bool, Int, Int, Int, Bool, String)
parseArgs = foldl parse (True, 0, 0, 0, False, "")
  where
    parse (perm, ver, depth, wrks, dbg, file) s =
      maybe id upd1 (parsePermutation s) $
      maybe id upd2 (parseVersion s) $
      maybe id upd3 (parseDepth s) $
      maybe id upd4 (parseWorkers s) $
      maybe id upd5 (parseDebug s) $
      maybe id upd6 (parseFilename s) $ (perm, ver, depth, wrks, dbg, file)
    upd1 y ( _, x2, x3, x4, x5, x6) = (y, x2, x3, x4, x5, x6)
    upd2 y (x1,  _, x3, x4, x5, x6) = (x1, y, x3, x4, x5, x6)
    upd3 y (x1, x2,  _, x4, x5, x6) = (x1, x2, y, x4, x5, x6)
    upd4 y (x1, x2, x3,  _, x5, x6) = (x1, x2, x3, y, x5, x6)
    upd5 y (x1, x2, x3, x4,  _, x6) = (x1, x2, x3, x4, y, x6)
    upd6 y (x1, x2, x3, x4, x5,  _) = (x1, x2, x3, x4, x5, y)


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
    let (permute, version, depth, workers, debug, filename) = parseArgs args
    io $ putStrLn $
      "MaxClique" ++ " -v" ++ show version ++
      (if depth >= 0 then " -d" ++ show depth else "") ++
      (if workers > 0 then " -n" ++ show workers else "") ++
      (if permute then " -perm" else " -noperm") ++
      (if debug then " -debug" else "") ++
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
      0 -> -- standard sequential Branch&Bound algorithm
           return $!! maxClq bigG
      1 -> -- sequential maxclique search based on tree iterator
           io $ force <$> maxClqIter bigG debug
      2 -> -- sequential maxclique search based on nested tree iterator;
           -- depth-bounded outer iterator generates tasks (for seq execution)
           io $ force <$> maxClqBndIter bigG depth debug
      3 -> -- parallel maxclique search based on nested tree iterator;
           -- depth-bounded outer iterator generates tasks
           io $ force <$> maxClqBndIterPar bigG depth workers debug
      4 -> -- same as '3' but generates no more than '10 * workers' tasks
           -- in advance
           io $ force <$> maxClqBufBndIterPar bigG depth workers debug
      5 -> -- HdpH maxclique search based on nested tree iterator;
           -- depth-bounded outer iterator generates HdpH tasks
           force <$> maxClqBndIterHdpH bigGRef depth debug
      6 -> -- HdpH maxclique search; depth-bounded tree iterator generates
           -- HdpH tasks which execute variant of standard seq algorithm
           force <$> maxClqBndIterHdpHv2 bigGRef depth debug
      _ -> usage
    -- deallocating input graph
    freeECRef bigGRef
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
