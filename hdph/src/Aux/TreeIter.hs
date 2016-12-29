-- Iterators over trees
--
-- 12-15 Feb 2016
--
-- Patrick Maier <C.Patrick.Maier@gmail.com>
--
-- NOTE: Test code commented out (requires 'random' package)
--

{-# LANGUAGE DeriveFunctor, DeriveFoldable, DeriveTraversable #-}

module Aux.TreeIter
  ( -- * trees
    Tree(Tree)
  , root
  , size
  , depth
  , width
  , enumNodes

    -- * paths from the root
  , Path
  , paths

    -- * tree generators (functional and in IO)
  , Generator
  , GeneratorM
  , generate
  , generateM
  , degenerate

    -- * functional tree iterators
  , TreeIter
  , newTreeIter
  , newPruneTreeIter

    -- * monadic tree iterators (in a monad above IO)
  , TreeIterM
  , newTreeIterM
  , newPruneTreeIterM
  ) where

import Prelude
import Control.Monad ((<=<))
import Control.Monad.IO.Class (MonadIO)
import Control.Monad.ST (runST)
import Data.List (find, nub)
import Data.STRef (newSTRef, modifySTRef', readSTRef)
-- TESTING ONLY
-- import System.Random (RandomGen, random, getStdRandom)
-- import System.IO.Unsafe (unsafePerformIO)

import Aux.Iter
       (Iter, newIter, runIter, buffer, skipUntil,
        IterM, newIterM, runIterM, bufferM, skipUntilM)

{-
-- TESTING (requires module be declared as Main)
main = do
  putStrLn $ "sizes " ++ show (map size rts)
  rtst3 <- tst3
  putStrLn $ "tst1,2,3  " ++ show (tst1 && tst2 && rtst3)
  putStrLn $ "tst1a,b,c " ++ show (tst1a && tst1b && tst1c)
  putStrLn $ "tst2a,b,c " ++ show (tst2a && tst2b && tst2c)
  rtst3a <- tst3a
  rtst3b <- tst3b
  rtst3c <- tst3c
  putStrLn $ "tst3a,b,c " ++ show (rtst3a && rtst3b && rtst3c)
  rtst4a <- tst4a
  rtst4b <- tst4b
  rtst4c <- tst4c
  putStrLn $ "tst4a,b,c " ++ show (rtst4a && rtst4b && rtst4c)
-}

-------------------------------------------------------------------------------
-- Trees

-- Non-empty, arbitrarily branching trees
data Tree a = Tree a [Tree a]
              deriving (Eq, Ord, Show, Functor, Foldable, Traversable)

root :: Tree a -> a
root (Tree x _) = x

depth :: Tree a -> Int
depth (Tree _ ts) = maximum (0 : map ((+ 1) . depth) ts)

width :: Tree a -> Int
width = maximum . transform
  where
    transform (Tree _ ts) = Tree (length ts) (map transform ts)

size :: Tree a -> Int
size (Tree _ ts) = 1 + sum (map size ts)

takeWidth :: Int -> Tree a -> Tree a
takeWidth w (Tree x ts) = Tree x $ take w $ map (takeWidth w) ts

takeDepth :: Int -> Tree a -> Tree a
takeDepth d (Tree x ts) | d <= 0    = Tree x []
                        | otherwise = Tree x $ map (takeDepth (d - 1)) ts

-- Returns trees of same shape as list `ts` yet whose nodes are numbered
-- consecutively from 1, following a depth-first, left-to-right traversal.
enumNodes :: [Tree a] -> [Tree Int]
enumNodes ts = runST $ do
  ctr <- newSTRef (0 :: Int)
  let ctrPlusPlus _ = do { modifySTRef' ctr (+ 1); readSTRef ctr }
  mapM (mapM ctrPlusPlus) ts

{-
-- TESTING
-- test data
t1 = Tree 6 []
t2 = Tree 7 [t1]
t3 = Tree (-42) [t2, t1, t2]
t4 = Tree 10 [t1, t2, t3]
-}


-------------------------------------------------------------------------------
-- Paths

-- A path is the list of nodes encountered on the way to the root.
type Path a = [a]

-- Lists all paths in a list of trees in depth-frist, left-to-right order.
-- Note that the output is prefix-closed, i.e. any prefix of a path is
-- a path (and proper prefixes appear earlier).
paths :: [Tree a] -> [Path a]
paths = ([]:) . map reverse . concat . (map go)
  where 
    go (Tree x ts) = [x] : concat (map (map (x:) . go) ts)

-- True iff the given list of paths is a set (ie. has no duplicates).
isPathSet :: (Eq a) => [Path a] -> Bool
isPathSet paths = paths == nub paths


-------------------------------------------------------------------------------
-- Generating functions

-- Generating functions (or actions) map each path to a list of extensions
-- (ordered left to right).
type Generator    a = Path a ->    [a]
type GeneratorIO  a = Path a -> IO [a]
type GeneratorM m a = Path a -> m [a]

-- Generate a unique list of trees from the generating function `g`.
generate :: Generator a -> [Tree a]
generate g = expand []
  where
    expand path = map (\x -> Tree x (expand (x:path))) $ g path

generateIO :: GeneratorIO a -> IO [Tree a]
generateIO g = expand []
  where
    expand path = mapM (\x -> Tree x <$> expand (x:path)) =<< g path

generateM :: (Monad m) => GeneratorM m a -> m [Tree a]
generateM g = expand []
  where
    expand path = mapM (\x -> Tree x <$> expand (x:path)) =<< g path

-- Right inverse of `generate` (provided `paths ts` has no duplicates).
degenerate :: (Eq a) => [Tree a] -> Generator a
degenerate ts = degenerate' ts . reverse
  where
    degenerate' ts = \ path ->
      case path of
        []        -> map root ts
        (x:path') -> case find ((x ==) . root) ts of
                       Nothing           -> []
                       Just (Tree _ ts') -> degenerate' ts' path'

{-
-- TESTING
-- test: `degenerate` is the right inverse of `generate`.
tst1 = ts == (generate . degenerate) ts
         where ts = enumNodes [rt1,rt2,rt3,rt4]

-- test: `degenerate` fails as right inverse when there are duplicate paths.
tst2 = ts /= (generate . degenerate) ts && (not . isPathSet . paths) ts
         where ts = [rt1,rt2,rt3,rt4]

-- test: `generate` and `generateIO` agree.
tst3 = (generate g ==) <$> generateIO (return . g)
         where g = degenerate $ enumNodes [rt1,rt2,rt3,rt4]
-}


-------------------------------------------------------------------------------
-- Iterator over paths in a tree (depth-first, left-to-right)

-- Stack of choice points (stack of lists of tree nodes)
-- Invariant: Stack may be empty but all lists on stack are non-empty.
type Stack a = [[a]]

-- Creates a stack from the given path.
pathToStack :: Path a -> Stack a
pathToStack = map (\ x -> [x])

-- Recreates a path by traversing the stack.
stackToPath :: Stack a -> Path a
stackToPath = map head

-- Advances stack (pick next choice point, or backtrack)
nextStack :: Stack a -> Maybe (Stack a)
nextStack []              = Nothing                      -- done
nextStack ([_]:stack)     = nextStack stack              -- backtrack
nextStack (choices:stack) = Just (tail choices : stack)  -- next choice point
                            -- Note: By Stack invariant, choices /= [].


-- Iterator iterating over paths (of trees).
type TreeIter a = Iter (Maybe (Stack a)) (Path a)

-- Iterator to iterate over the paths of trees generated by `g` and
-- extending `path0`.
newTreeIter :: Path a -> Generator a -> TreeIter a
newTreeIter path0 g = newIter next (Just $ pathToStack path0)
  where
 -- next :: Maybe (Stack a) -> Maybe (Maybe (Stack a), Path a)
    next Nothing      = Nothing
    next (Just stack) =
      if null choices
        then Just (nextStack stack,      path)
        else Just (Just $ choices:stack, path)
          where
            path    = stackToPath stack
            choices = g path

{-
-- TESTING
-- test function: compare `paths . generate` to `runIter . newTreeIter []`
f1 g = (paths . generate) g == (runIter . newTreeIter []) g
tst1a = and [f1 $ degenerate [t] | t <- rts]
tst1b = f1 $ degenerate rts
tst1c = f1 $ degenerate $ enumNodes rts
rts = [rt1, rt2, rt3, rt4]
-}

-------------------------------------------------------------------------------
-- Pruning iterator over paths in a tree (depth-first, left-to-right)

-- Iterator to iterate over the paths of trees generated by `g`,
-- extending `path0` and minimally satisfying `p` (ie. paths with no
-- proper prefix satisfying `p`).
newPruneTreeIter :: (Path a -> Bool) -> Path a -> Generator a -> TreeIter a
newPruneTreeIter p path0 g = newTreeIter path0 g' `skipUntil` p
  where
    g' path = if p path then [] else g path

{-
-- TESTING
-- test function: compare
-- * `filter p . paths . generate` and
-- * `runIter . (`buffer` n) . newPruneTreeIter p []`
-- where `p = (== d) . length`
f2 n d g = lhs g == rhs g
  where
    lhs = filter p . paths . generate
    rhs = runIter . (`buffer` n) . newPruneTreeIter p []
    p = (== d) . length
tst2a = and [f2 n d $ degenerate [t] | t <- rts, d <- [0..11], n <- [1..20]]
tst2b = and [f2 n d $ degenerate rts | d <- [0 .. 11], n <- [1..20]]
tst2c = and [f2 n d $ degenerate $ enumNodes rts | d <- [0..11], n <- [1..20]]
-}


-------------------------------------------------------------------------------
-- Generic monadic iterator over paths in a tree (depth-first, left-to-right)

-- Generic monadic iterator over paths (of trees).
type TreeIterM m a = IterM m (Maybe (Stack a)) (Path a)

-- Iterator to iterate over the paths of trees generated by `g` and
-- extending `path0`.
newTreeIterM :: (MonadIO m) => Path a -> GeneratorM m a -> m (TreeIterM m a)
{-# INLINABLE newTreeIterM #-}
newTreeIterM path0 g = newIterM next (Just $ pathToStack path0)
  where
 -- next :: Maybe (Stack a) -> m (Maybe (Maybe (Stack a), Path a))
    next Nothing      = return Nothing
    next (Just stack) = do
      let path = stackToPath stack
      choices <- g path
      if null choices
        then return $ Just (nextStack stack,      path)
        else return $ Just (Just $ choices:stack, path)

{-
-- TESTING
-- test function: compare `paths . generate` to `runIterM <=< newTreeIterM []`
f3 :: (Eq a) => Generator a -> IO Bool
f3 g =
  ((paths . generate) g ==) <$> (runIterM <=< newTreeIterM []) (return . g)
tst3a = and <$> sequence [f3 $ degenerate [t] | t <- rts]
tst3b = f3 $ degenerate rts
tst3c = f3 $ degenerate $ enumNodes rts
-}


-------------------------------------------------------------------------------
-- Generic monadic pruning iterator over tree paths (depth-first, left-to-right)

-- Generic monadic iterator to iterate over the paths of trees generated by `g`,
-- extending `path0` and minimally satisfying `p` (ie. paths with no proper
-- prefix satisfying `p`).
-- Note that the action `p` maybe called many times; its side effects
-- should therefore be idempotent.
newPruneTreeIterM :: (MonadIO m)
                  => (Path a -> m Bool) -> Path a -> GeneratorM m a
                  -> m (TreeIterM m a)
{-# INLINABLE newPruneTreeIterM #-}
newPruneTreeIterM p path0 g = newTreeIterM path0 g' >>= skipUntilM p
  where
    g' path = do { prune <- p path; if prune then return [] else g path }

{-
-- TESTING
-- test function: compare
-- * `filter p . paths . generate` and
-- * `runIterM <=< bufferM n <=< newPruneTreeIterM (return . p) []`
-- where `p = (== d) . length`
f4 :: (Eq a) => Int -> Int -> Generator a -> IO Bool
f4 n d g = (lhs g ==) <$> rhs (return . g)
  where
    lhs = filter p . paths . generate
    rhs = runIterM <=< bufferM n <=< newPruneTreeIterM (return . p) []
    p = (== d) . length
tst4a = and <$> sequence [f4 n d $ degenerate [t] | t <- rts, d <- [0..11], n <- [1..20]]
tst4b = and <$> sequence [f4 n d $ degenerate rts | d <- [0 .. 11], n <- [1..20]]
tst4c = and <$> sequence [f4 n d $ degenerate $ enumNodes rts | d <- [0..11], n <- [1..20]]
-}


-------------------------------------------------------------------------------
-- Random trees (for testing)

{-
-- TESTING ONLY
-- Geometric distribution (support [0..]) with probability 1/2
geometric :: (RandomGen g) => g -> (Integer, g)
geometric g = toss g 0
  where
    toss g k | success   = (k, g')
             | otherwise = toss g' (k + 1) where (success, g') = random g

-- Random tree of unit type; shape selection governed by depth and width bounds
-- and by repeatedly drawing from geometric distribution;
-- depth < 0 means no depth bound; width < 0 means no width bound;
-- switching off both bounds may lead result in out-of-memory crash.
randomTree :: (RandomGen g) => Int -> Int -> g -> (Tree (), g)
randomTree 0     _     g = (Tree () [], g)
randomTree depth width g = (Tree () ts, g'')
  where
    (k, g')   = geometric g
    (ts, g'') = go (if width < 0 then k else min (fromIntegral width) k) [] g'
    go 0 accu g = (accu, g)
    go k accu g = go (k - 1) (t:accu) g'
                    where (t, g') = randomTree (depth - 1) width g

-- Random tree of unit type satisfying predicate p; shape seletion as above;
-- will diverge if there are few trees satisfying p.
randomTreeUntil :: (RandomGen g)
                => (Tree () -> Bool) -> Int -> Int -> g -> (Tree (), g)
randomTreeUntil p depth width g = let
    (t, g') = randomTree depth width g in
  if p t then (t, g') else randomTreeUntil p depth width g'

randomTreeUntilIO :: (Tree () -> Bool) -> Int -> Int -> IO (Tree ())
randomTreeUntilIO p depth width = getStdRandom $ randomTreeUntil p depth width

-- random test data
rt1 = unsafePerformIO $
        head . enumNodes . (:[]) <$> randomTreeUntilIO ((> 19) . size) 6 2
rt2 = unsafePerformIO $
        head . enumNodes . (:[]) <$> randomTreeUntilIO ((> 99) . size) 6 6
rt3 = unsafePerformIO $
        head . enumNodes . (:[]) <$> randomTreeUntilIO ((> 9) . depth) 10 10
rt4 = unsafePerformIO $
        head . enumNodes . (:[]) <$> randomTreeUntilIO ((> 9) . width) 10 10
-}
