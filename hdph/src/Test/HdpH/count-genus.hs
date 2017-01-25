-- Counting numerical semigroups of genus N.
-- The algorithm (published in [1]) traverses a tree of numerical semigroups,
-- where the semigroups at level N are exactly those of genus N. Hence
-- the task is to count the vertices of the tree at level N.
--
-- Author: Patrick Maier
--
-- References:
-- [1] Maria Bras-Amoros, Julio Fernandez-Gonzalez.
--     Computation of numerical semigroups by means of seeds.
--     arXiv: 1607.01545v1 [math.CO] 6 July 2016.
--
-----------------------------------------------------------------------------

{-# LANGUAGE TemplateHaskell #-}  -- req'd for mkClosure, etc
{-# LANGUAGE DeriveGeneric #-}

module Main where

import Prelude
import Control.DeepSeq (NFData(rnf))
import Control.Exception (evaluate)
import Control.Monad (forM, when, zipWithM_)
import Control.Monad.ST (runST)
import Data.List (stripPrefix)
import Data.Time.Clock (NominalDiffTime, diffUTCTime, getCurrentTime)
import Data.Serialize (Serialize)
import GHC.Generics (Generic)
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

import Test.HdpH.STBitVector (BitVector)
import qualified Test.HdpH.STBitVector as BV
       (fromList, new, test, shiftL, shiftR, unionWith, union, intersect,
        unsafeInsert, unsafeFreeze)

-----------------------------------------------------------------------------
-- sequential algorithm

-- Representation of semigroup with seeds in semigroup tree.
data SGP =
  SGP
   !BitVector  -- bit vector  of gaps;                           G in [1]
   !BitVector  -- bit vector of seeds;                           S in [1]
   !Int        -- conductor of semigroup; support of BitVectors; c in [1]
   !Int        -- lambda_1 of semigroup, ie. smallest elt != 0;  m in [1]
   !Int        -- genus of semigroup, i.e. number of 1s in G;    g in [1]
   deriving (Generic)

instance NFData SGP where
  rnf x = x `seq` ()

instance Serialize SGP

instance Show SGP where
  show (SGP bigG bigS _c _m _g) = show bigG ++ ":" ++ show bigS

-- Root of semigroup tree (trivial semigroup).
root :: SGP
root = SGP (BV.fromList (1,[])) (BV.fromList (1,[0])) 1 1 0

-- Returns the children of the given semigroup in the semigroup tree.
-- The computation runs in the ST monad to mutate bitvectors in place;
-- the function guarantees to never modify its argument.
children :: SGP -> [SGP]
children (SGP bigG_frzn bigS_frzn c m g) = runST $ do
  -- create bigS as a mutable copy of bigS_frzn
  bigS <- BV.new c
  bigS `BV.unionWith` bigS_frzn
  -- create rake as a mutable copy of bigG_frzn
  rake <- BV.new c
  rake `BV.unionWith` bigG_frzn
  -- for loop, iterating s_tilde from 0 to m - 1
  fmap concat $ forM [0 .. m - 1] $ \ s_tilde -> do
    -- check whether s_tilde is a seed
    isSeed <- BV.test bigS s_tilde
    node_or_nil <-
      if isSeed
        then do
          -- s_tilde is a seed: construct child
          let g_tilde = g + 1
          let c_tilde = c + s_tilde + 1
          let m_tilde | s_tilde == 0 && m == c = c_tilde
                      | otherwise              = m
          -- construct representation of semigroup by copying bigG_frzn
          bigG_tilde <- BV.new c_tilde
          bigG_tilde `BV.unionWith` bigG_frzn
          BV.unsafeInsert (c_tilde - 2) bigG_tilde
          bigG_tilde_frzn <- BV.unsafeFreeze bigG_tilde
          -- construct representation of seeds by copying bigS
          bigS_tilde <- BV.new c_tilde
          bigS_tilde `BV.union` bigS
          bigS_tilde `BV.shiftL` (s_tilde + 1)
          BV.unsafeInsert (c_tilde - 3) bigS_tilde
          BV.unsafeInsert (c_tilde - 2) bigS_tilde
          BV.unsafeInsert (c_tilde - 1) bigS_tilde
          bigS_tilde_frzn <- BV.unsafeFreeze bigS_tilde
          -- return child as singleton list
          return [SGP bigG_tilde_frzn bigS_tilde_frzn c_tilde m_tilde g_tilde]
        else
          -- s_tilde is not a seed; return an empty list
           return []
    -- rake bigS
    rake `BV.shiftR` 1
    bigS `BV.intersect` rake
    -- return result (empty list or singleton)
    return node_or_nil

-- Number of semigroups of genus 'g' that are descendents of 'sgp'.
countGenusBelow :: Int -> SGP -> Integer
countGenusBelow g sgp@(SGP _ _ _ _ g_sgp)
  | g_sgp  < g = sum $ map (countGenusBelow g) $ children sgp
  | g_sgp == g = 1
  | otherwise  = 0


-----------------------------------------------------------------------------
-- parallel algorithm, direct implementation

-- Number of semigroups of genus 'g' that are descendents of 'sgp';
-- 'seq_levels' determines how much of the semigroup tree is traversed
-- sequentially, the actual number of levels depends on lambda_1 but is
-- guaranteed to be between 'seq_levels' and 1/2 * 'seq_levels';
-- a 'seq_levels' value between 1/3 * g and 5/12 * g appears to work well.
countGenusBelowPar :: Int -> Int -> SGP -> Par Integer
countGenusBelowPar seq_levels g sgp@(SGP _ _ _ m g_sgp) = do
  -- sequential threshold; taking m (ie. lambda_1) into account helps balance
  let threshold =
        min ((m * seq_levels) `div` (2 * (g - seq_levels)) + g - seq_levels)
            (g - seq_levels `div` 2)
  if g_sgp >= threshold
    then return $! countGenusBelow g sgp
    else do
      vs <- mapM spawn $ reverse $ children sgp   -- Reversing children
      n_g <- sum . map unClosure <$> mapM get vs  -- improves performance here
      return $! n_g
        where
          spawn sgp_tilde = do
            v <- new
            gv <- glob v
            let task = $(mkClosure [| countGenusBelowPar_abs
                                      (seq_levels, g, sgp_tilde, gv) |])
            spark one task
            return v

countGenusBelowPar_abs :: (Int, Int, SGP, GIVar (Closure Integer))
                       -> Thunk (Par ())
countGenusBelowPar_abs (seq_levels, g, sgp_tilde, gv) = Thunk $ do
  n_g <- countGenusBelowPar seq_levels g sgp_tilde
  rput gv $ toClosureInteger $! n_g


toClosureInteger :: Integer -> Closure Integer
toClosureInteger i = $(mkClosure [| toClosureInteger_abs i |])

toClosureInteger_abs :: Integer -> Thunk Integer
toClosureInteger_abs i = Thunk i


-----------------------------------------------------------------------------
-- Static declaration (just before 'main')

-- Empty splice; TH hack to make all environment abstractions visible.
$(return [])

declareStatic :: StaticDecl
declareStatic =
  mconcat
    [HdpH.declareStatic,
     declare $(static 'countGenusBelowPar_abs),
     declare $(static 'toClosureInteger_abs)]


-----------------------------------------------------------------------------
-- initialisation, argument processing and 'main'

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


-- parse (optional) arguments in this order: 
-- * version to run
-- * genus to count
-- * bottom levels of tree to traverse sequentially
parseArgs :: [String] -> (Int, Int, Int)
parseArgs []     = (def_ver, def_g, def_seq_levels)
parseArgs (s:ss) =
  let go :: Int -> [String] -> (Int, Int, Int)
      go v []        = (v, def_g,   def_seq_levels)
      go v [s1]      = (v, read s1, def_seq_levels)
      go v (s1:s2:_) = (v, read s1, read s2)
  in case stripPrefix "v" s of
       Just s' -> go (read s') ss
       Nothing -> go def_ver (s:ss)

-- defaults for optional arguments
def_ver, def_g, def_seq_levels :: Int
def_ver        =  1  -- version
def_g          = 30  -- genus
def_seq_levels = 21  -- bottom levels of tree to traverse sequentially


printResults :: (String, String, String, String, String) -> IO ()
printResults (version, levels, input, output, runtime) =
  zipWithM_ printTagged tags [version, levels, input, output, runtime]
    where printTagged tag val = putStrLn (tag ++ val)
          tags = ["VERSION: ", "SEQUENTIAL_LEVELS: ",
                  "INPUT: ", "OUTPUT: ", "RUNTIME: "]


main :: IO ()
main = do
  hSetBuffering stdout LineBuffering
  hSetBuffering stderr LineBuffering
  register declareStatic
  opts_args <- getArgs
  (conf, seed, args) <- parseOpts opts_args
  let (version, g, seq_levels) = parseArgs args
  initrand seed
  case version of
      0 -> do (x, t) <- timeIO $ evaluate
                          (countGenusBelow g root)
              printResults ("v0", "-1", show g, show x, show t)
      1 -> do (output, t) <- timeIO $ evaluate =<< runParIO conf
                               (countGenusBelowPar seq_levels g root)
              case output of
                Nothing -> return ()
                Just x  ->
                  printResults ("v1", show seq_levels, show g, show x, show t)
      _ -> return ()
