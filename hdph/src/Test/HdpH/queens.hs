-- N queens problem in HdpH
--
-- Author: Patrick Maier
-----------------------------------------------------------------------------

{-# LANGUAGE FlexibleInstances #-}     -- req'd for ToClosure insts
{-# LANGUAGE TypeSynonymInstances #-}  -- req'd for ToClosure insts
{-# LANGUAGE TemplateHaskell #-}       -- req'd for mkClosure, etc

module Main where

import Prelude
import Control.DeepSeq (NFData)
import Control.Monad (when, (<=<))
import Data.Functor ((<$>))
import Data.List (elemIndex, stripPrefix, transpose)
import Data.Maybe (fromJust)
import Data.Monoid (mconcat)
import System.Environment (getArgs)
import System.IO (stdout, stderr, hSetBuffering, BufferMode(..))
import System.Random (mkStdGen, setStdGen)

import Control.Parallel.HdpH
       (RTSConf(..), defaultRTSConf, updateConf,
        Par, runParIO,
        force, fork, new, get, put,
        IVar,
        Thunk(Thunk), Closure, mkClosure,
        toClosure, ToClosure(locToClosure),
        static, StaticToClosure, staticToClosure,
        StaticDecl, declare, register, here)
import qualified Control.Parallel.HdpH as HdpH (declareStatic)
import Control.Parallel.HdpH.Strategies 
       (Strategy, ForceCC(locForceCC),
        parMapNF, parMapChunkedNF,
        StaticForceCC, staticForceCC)
import qualified Control.Parallel.HdpH.Strategies as Strategies (declareStatic)


-----------------------------------------------------------------------------
-- Static declaration

instance ToClosure Board where locToClosure = $(here)
instance ToClosure [Board] where locToClosure = $(here)
instance ForceCC [Board] where locForceCC = $(here)

declareStatic :: StaticDecl
declareStatic =
  mconcat
    [HdpH.declareStatic,
     Strategies.declareStatic,
     declare (staticToClosure :: StaticToClosure Board),
     declare (staticToClosure :: StaticToClosure [Board]),
     declare (staticForceCC :: StaticForceCC [Board]),
     declare $(static 'clo_extendBoard_abs)]


-----------------------------------------------------------------------------

-- See [http://en.literateprograms.org/Eight_Queens_puzzle_(Haskell)]
-- for an explanation about the representation of solutions.
type Board = [Row]
type Row = Int


-----------------------------------------------------------------------------
-- sequential N queens solver

queens :: Int -> [Board]
queens n | n <= 0    = []
         | otherwise = genBoards n n


genBoards :: Int -> Int -> [Board]
genBoards _ 0 = [[]]
genBoards n i = concat $ map (extendBoard n) $ genBoards n (i - 1)


extendBoard :: Int -> Board -> [Board]
extendBoard n board = [ row:board | row <- [1 .. n], safeAddition board row ]


safeAddition :: Board -> Row -> Bool
safeAddition board row0 = go board 1
  where
    go []          _ = True
    go (row:board) i = row0 /= row && abs (row0 - row) /= i && go board (i + 1)


-----------------------------------------------------------------------------
-- parallel N queens solver; shared memory (w/ skeletons for Par monad)

-- k is chunk size
par_queens :: Int -> Int -> Par [Board]
par_queens k n | n <= 0    = return []
               | otherwise = gen n
  where
    gen :: Int -> Par [Board]
    gen 0 = return [[]]
    gen i = concat <$> (par_map_chunk k (extendBoard n) =<< gen (i - 1))


-- parallel map; shared memory
par_map :: (NFData b) => (a -> b) -> [a] -> Par [b]
par_map f = par_forceList . map f

par_map_chunk :: (NFData b) => Int -> (a -> b) -> [a] -> Par [b]
par_map_chunk n f = par_forceList_clustered (chunk n) unchunk . map f

par_map_slice :: (NFData b) => Int -> (a -> b) -> [a] -> Par [b]
par_map_slice n f = par_forceList_clustered (slice n) unslice . map f


-- strategy; shared memory
par_forceList :: (NFData a) => [a] -> Par [a]
par_forceList = mapM get <=< mapM spawn
  where
    spawn :: (NFData a) => a -> Par (IVar a)
    spawn y = do v <- new
                 fork (force y >>= put v)
                 return v

-- clustering strategy (parametrised by NFData); shared memory
par_forceList_clustered :: (NFData b)
                        => ([a] -> [b]) -> ([b] -> [a]) -> [a] -> Par [a]
par_forceList_clustered cluster uncluster xs =
  uncluster <$> (par_forceList $ cluster xs)


-- clustering functions: chunking and slicing
chunk :: Int -> [a] -> [[a]]
chunk n | n <= 0    = chunk 1
        | otherwise = go
            where
              go [] = []
              go xs = ys : go zs where (ys,zs) = splitAt n xs

unchunk :: [[a]] -> [a]
unchunk = concat

slice :: Int -> [a] -> [[a]]
slice n = transpose . chunk n

unslice :: [[a]] -> [a]
unslice = concat . transpose


-----------------------------------------------------------------------------
-- parallel N queens solver; distributed memory (using task farm + threshold)

-- t is threshold
threshfarm_queens :: Int -> Int -> Par [Board]
threshfarm_queens t n | n <= 0    = return []
                      | otherwise = gen n
  where
    gen :: Int -> Par [Board]
    gen i | i <= t    = force $ genBoards n i
          | otherwise = concat <$>
                          (parMapNF $(mkClosure [| clo_extendBoard_abs n |]) =<<
                           gen (i - 1))

clo_extendBoard_abs :: Int -> Thunk (Board -> [Board])
clo_extendBoard_abs n = Thunk $ extendBoard n


-----------------------------------------------------------------------------
-- parallel N queens solver; distributed memory (using chunking task farm)

-- k is chunk size
chunkfarm_queens :: Int -> Int -> Par [Board]
chunkfarm_queens k n | n <= 0    = return []
                     | otherwise = gen n
  where
    gen :: Int -> Par [Board]
    gen 0 = return [[]]
    gen i = concat <$>
              (parMapChunkedNF k $(mkClosure [| clo_extendBoard_abs n |]) =<<
               gen (i - 1))


-----------------------------------------------------------------------------
-- parallel N queens solver; distributed memory (using threshold + chunking)

-- t is threshold, k is chunk size
threshchunkfarm_queens :: Int -> Int -> Int -> Par [Board]
threshchunkfarm_queens t k n | n <= 0    = return []
                             | otherwise = gen n
  where
    gen :: Int -> Par [Board]
    gen i | i <= t    = force $ genBoards n i
          | otherwise =
              concat <$>
                (parMapChunkedNF k $(mkClosure [| clo_extendBoard_abs n |]) =<<
                 gen (i - 1))


-----------------------------------------------------------------------------
-- initialisation and argument processing and 'main'

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


-- parse (optional) arguments in this order: 
-- * version to run
-- * problem size (ie. number of queens and (sqrt of) board size)
-- * threshold or chunk size (determines granularity of sequential tasks)
-- * chunk size
parseArgs :: [String] -> (Int, Int, Int, Int)
parseArgs []     = (defVers, defN, defGran1, defGran2)
parseArgs (s:ss) =
  let go :: Int -> [String] -> (Int, Int, Int, Int)
      go v []           = (v, defN,    defGran1, defGran2)
      go v [s1]         = (v, read s1, defGran1, defGran2)
      go v [s1,s2]      = (v, read s1, read s2,  defGran2)
      go v (s1:s2:s3:_) = (v, read s1, read s2,  read s3)
  in case stripPrefix "v" s of
       Just s' -> go (read s') ss
       Nothing -> go defVers (s:ss)

defVers  =   4 :: Int
defN     =  12 :: Int
defGran1 =  10 :: Int
defGran2 = 100 :: Int


main :: IO ()
main = do
  hSetBuffering stdout LineBuffering
  hSetBuffering stderr LineBuffering
  register declareStatic
  opts_args <- getArgs
  (conf, seed, args) <- parseOpts opts_args
  let (version, n, gran1, gran2) = parseArgs args
  initrand seed
  case version of
      0 -> do x <- return $ queens n
              putStrLn $
                "{v0} length $ queens " ++ show n ++
                " = " ++ show (length x)
      1 -> do output <- runParIO conf $ par_queens gran1 n
              case output of
                Just x  -> putStrLn $
                             "{v1, " ++
                             "chunkSize=" ++ show gran1 ++ "} " ++
                             "length $ queens " ++ show n ++ " = " ++
                             show (length x)
                Nothing -> return ()
      2 -> do output <- runParIO conf $ threshfarm_queens gran1 n
              case output of
                Just x  -> putStrLn $
                             "{v2, " ++
                             "threshold=" ++ show gran1 ++ "} " ++
                             "length $ queens " ++ show n ++ " = " ++
                             show (length x)
                Nothing -> return ()
      3 -> do output <- runParIO conf $ chunkfarm_queens gran1 n
              case output of
                Just x  -> putStrLn $
                             "{v3, " ++
                             "chunkSize=" ++ show gran1 ++ "} " ++
                             "length $ queens " ++ show n ++ " = " ++
                             show (length x)
                Nothing -> return ()
      4 -> do output <- runParIO conf $ threshchunkfarm_queens gran1 gran2 n
              case output of
                Just x  -> putStrLn $
                             "{v4, " ++
                             "threshold=" ++ show gran1 ++ ", " ++
                             "chunkSize=" ++ show gran2 ++ "} " ++
                             "length $ queens " ++ show n ++ " = " ++
                             show (length x)
                Nothing -> return ()
      _ -> return ()

-- NOTE: May run out-of-memory in distributed modes (v2-v4) if n >= 13.
--       Need to investigate why. It may be a problem of too much laziness
--       when processing intermediate results.
--       Also, does not show good speedup (at most 2 on 7 cores).
