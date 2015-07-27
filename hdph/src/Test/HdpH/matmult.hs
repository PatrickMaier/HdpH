-- Simple matrix multiplication in HdpH
--
-- Author: Patrick Maier
-----------------------------------------------------------------------------

{-# LANGUAGE TemplateHaskell #-}  -- req'd for mkClosure, etc
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE DeriveFunctor, DeriveFoldable, DeriveTraversable #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE BangPatterns #-}

{-# OPTIONS_GHC -fno-warn-orphans #-}

module Main where

import Prelude hiding (mapM, sum)
import Control.DeepSeq (NFData, rnf, ($!!))
import Control.Monad (when, replicateM, zipWithM_, (<=<))
import Data.Foldable (Foldable)
import Data.Functor ((<$>))
import Data.IORef (IORef, newIORef, atomicWriteIORef, readIORef)
import Data.List (stripPrefix, transpose, foldl1')
import Data.Monoid (mconcat)
import Data.Serialize (Serialize)
import qualified Data.Serialize (put, get, putFloat64le, getFloat64le)
import Data.Time.Clock (NominalDiffTime, diffUTCTime, getCurrentTime)
import Data.Traversable (Traversable, mapM)
import System.Environment (getArgs)
import System.IO (stdout, stderr, hSetBuffering, BufferMode(..))
import System.IO.Unsafe (unsafePerformIO)
import System.Random (mkStdGen, setStdGen, Random, randomIO)

import Control.Parallel.HdpH
       (RTSConf(..), defaultRTSConf, updateConf,
        Par, runParIO,
        allNodes, io, force, fork, spark, spawnAt, new, get, put, glob, rput,
        IVar, GIVar, Node,
        Thunk(Thunk), Closure, unClosure, mkClosure,
        toClosure, ToClosure(locToClosure),
        Static, static, StaticToClosure, staticToClosure,
        StaticDecl, declare, register, here)
import qualified Control.Parallel.HdpH as HdpH (declareStatic)
import Control.Parallel.HdpH.Dist (one)


-----------------------------------------------------------------------------
-- toplevel mutable state (monomorphic)

-- IORef holding pair of input matrices `a` and `tr_b` (ie. `b` transposed);
-- initialised to invalid empty matrices.
matrices :: IORef (Matrix Double', Matrix Double')
matrices = unsafePerformIO $ newIORef (Matrix [[]], Matrix [[]])
{-# NOINLINE matrices #-}


-----------------------------------------------------------------------------
-- utils, etc.

-- Time an `IO` action.
timeIO :: IO a -> IO (a, NominalDiffTime)
timeIO action = do t0 <- getCurrentTime
                   x <- action
                   t1 <- getCurrentTime
                   return (x, diffUTCTime t1 t0)

timeIO_ :: IO a -> IO NominalDiffTime
timeIO_ action = snd <$> timeIO action

-- Time a `Par` action.
timePar :: Par a -> Par (a, NominalDiffTime)
timePar action = do t0 <- io $ getCurrentTime
                    x <- action
                    t1 <- io $ getCurrentTime
                    return (x, diffUTCTime t1 t0)


-- Perform same task on all nodes in parallel, waiting for all results.
pushEverywhere :: Closure (Par (Closure a)) -> Par [Closure a]
pushEverywhere task = do
  nodes <- allNodes
  futures <- mapM (\ node -> spawnAt node task) nodes
  mapM get futures


-----------------------------------------------------------------------------
-- double precision floating point numbers with more efficient serialisation

newtype Double' = Double' { unDouble' :: Double }
                  deriving (Eq, Ord, NFData, Random)

instance Show Double' where
  show = show . unDouble'

instance Serialize Double' where
  put = Data.Serialize.putFloat64le . unDouble'
  get = Double' <$> Data.Serialize.getFloat64le


-----------------------------------------------------------------------------
-- rings as algebraic structures with addition and multiplication

class Ring a where
  -- ring addition; associative and commutative
  radd :: a -> a -> a
  -- ring multiplication: associative and distributing over addition
  rmul :: a -> a -> a

instance Ring Int where { radd = (+); rmul = (*) }
instance Ring Integer where { radd = (+); rmul = (*) }
instance Ring Rational where { radd = (+); rmul = (*) }
instance Ring Float where { radd = (+); rmul = (*) }
instance Ring Double where { radd = (+); rmul = (*) }
instance Ring Double' where
  radd (Double' x) (Double' y) = Double' (x + y)
  rmul (Double' x) (Double' y) = Double' (x * y)

-- explicit `Ring` dictionary
data RingD a = RingD { raddD :: a -> a -> a, rmulD :: a -> a -> a }

mkRingD :: (Ring a) => RingD a
mkRingD = RingD { raddD = radd, rmulD = rmul }


-----------------------------------------------------------------------------
-- vectors as lists of ring elements

type Vector a = [a]

-- `True` iff argument is actually a vector.
-- All subsequent functions implicitly assume that their `Vector` arguments
-- satisfy `isVector`.
isVector :: Vector a -> Bool
isVector = not . null

len :: Vector a -> Int
len = length

sumVector :: (Ring a) => Vector a -> a
sumVector = foldl1' radd

sumVectorD :: (RingD a) -> Vector a -> a
sumVectorD rd = foldl1' (raddD rd)

-- `innerProd x y` assumes `len x == len y`.
innerProd :: (Ring a) => Vector a -> Vector a -> a
innerProd x y = sumVector $ zipWith rmul x y

-- `innerProdD rd x y` assumes `len x == len y`.
innerProdD :: RingD a -> Vector a -> Vector a -> a
innerProdD rd x y = sumVectorD rd $ zipWith (rmulD rd) x y

-- `chunkBy k x` assumes `k > 0`.
chunkBy :: Int -> Vector a -> [Vector a]
chunkBy k | k <= 0    = error "chunkBy"
          | otherwise = go
              where
                go [] = []
                go xs = ys : go zs where (ys,zs) = splitAt k xs
{-# INLINE chunkBy #-}

-- Inverse of `chunkBy k`.
unChunk :: [Vector a] -> Vector a
unChunk = concat
{-# INLINE unChunk #-}

-- `randomVectorIO k` generates a random vector of length `k`.
randomVectorIO :: (Random a) => Int -> IO (Vector a)
randomVectorIO k | k <= 0    = error "randomVectorIO"
                 | otherwise = replicateM k randomIO


-----------------------------------------------------------------------------
-- matrices as lists of vectors of ring elements (row-major format)

newtype Matrix a = Matrix { unMatrix :: [Vector a] }
                   deriving (Eq, Ord, Show, Functor, Foldable, Traversable)

instance (NFData a) => NFData (Matrix a) where
  rnf = rnf . unMatrix

instance (Serialize a) => Serialize (Matrix a) where
  put = Data.Serialize.put . unMatrix
  get = Matrix <$> Data.Serialize.get

-- Evaluates shape of given matrix, without evaluating the elements.
evalShape :: Matrix a -> Matrix a
evalShape (Matrix a) = go a `seq` Matrix a
  where
    go []              = ()
    go ([]:rows)       = go rows
    go ((_:cols):rows) = go (cols:rows)

-- `True` iff argument is actually a matrix.
-- All subsequent functions implicitly assume that their `Matrix` arguments
-- satisfy `isMatrix`, whence they guarantee that their `Matrix` outputs will.
isMatrix :: Matrix a -> Bool
isMatrix (Matrix a) =
  not (null a) && all (\ row -> isVector row && len row == len (head a)) a

dim :: Matrix a -> (Int, Int)
dim (Matrix a) = (m, n) where { !m = length a; !n = len (head a) }

enumRows :: Matrix a -> [Int]
enumRows (Matrix a) = zipWith const [0 ..] a

enumCols :: Matrix a -> [Int]
enumCols (Matrix a) = zipWith const [0 ..] (head a)

tr :: Matrix a -> Matrix a
tr = Matrix . transpose . unMatrix
{-# INLINE tr #-}

sum :: (Ring a) => Matrix a -> a
sum = foldl1' radd . map sumVector . unMatrix

-- `add a b` assumes `dim a == dim b`.
add :: (Ring a) => Matrix a -> Matrix a -> Matrix a
add (Matrix a) (Matrix b) = Matrix $ zipWith (zipWith radd) a b

-- `mul a b` assumes `snd (dim a) == fst (dim b)`.
mul :: (Ring a) => Matrix a -> Matrix a -> Matrix a
mul a b = mulT a (tr b)

-- `mulT a (tr b) == mul a b` and assumes `snd (dim a) == snd (dim b)`.
mulT :: (Ring a) => Matrix a -> Matrix a -> Matrix a
mulT (Matrix a) (Matrix tr_b) =
  Matrix [[innerProd row_i col_j | col_j <- tr_b] | row_i <- a]

instance (Ring a) => Ring (Matrix a) where { radd = add; rmul = mul }

-- `stripeAt i k a` assumes `k > 0` and `0 <= i < m` where `dim a (m,n)`.
-- The call returns a `k` row stripe of `a` starting at row `i`;
-- if `i + k > m` the stripe is only `m - i` rows.
stripeAt :: Int -> Int -> Matrix a -> Matrix a
stripeAt i k = Matrix . take k . drop i . unMatrix
{-# INLINE stripeAt #-}

-- `stripeBy k a` assumes `k > 0` and returns a list of /stripes/, i.e.
-- matrices of dimension `(k,n)`, or possibly `(mod m k, n)` in case of
-- the very last matrix, where `(m,n) = dim a`.
stripeBy :: Int -> Matrix a -> [Matrix a]
stripeBy k = map Matrix . chunkBy k . unMatrix
{-# INLINE stripeBy #-}

-- Inverse of `stripeBy k`.
unStripe :: [Matrix a] -> Matrix a
unStripe = Matrix . unChunk . map unMatrix
{-# INLINE unStripe #-}

-- `blockBy (k,l) a` assumes `k > 0 && l > 0` and returns a matrix of /blocks/,
-- i.e. matrices of dimension `(k,l)` in general; the dimensions of a block
-- at the end of a column may be `mod m k` instead of `k`, and at the end of
-- a row may be `mod n l` instead of `l`, where `(m,n) = dim a`.
blockBy :: (Int, Int) -> Matrix a -> Matrix (Matrix a)
blockBy (k,l) = Matrix . map (map tr . stripeBy l . tr) . stripeBy k
{-# INLINE blockBy #-}

-- Inverse of `blockBy (k,l)`.
unBlock :: Matrix (Matrix a) -> Matrix a
unBlock = unStripe . map (tr . unStripe . map tr) . unMatrix
{-# INLINE unBlock #-}

-- `blockTemplate (m,n) (k,l)` assumes that `(m,n) = dim a` and `(k,l) = dim b`
-- for some matrices `a` and `b`. It returns a matrix of indices `(i,j)`
-- specifying a `(k,l)`-dimensional block structure on `a`; that is, every
-- `(i,j)` identifies the upper left corner of a block of dimension `(k,l)`
-- (or smaller if it is the last block in the column or row).
blockTemplate :: (Int,Int) -> (Int,Int) -> Matrix (Int,Int)
blockTemplate (m,n) (k,l) =
  Matrix [[(i,j) | j <- [0, l .. n - 1]] | i <- [0, k .. m - 1]]

-- `randomMatrixIO (m,n)` generates a random matrix of dimension `(m,n)`.
randomMatrixIO :: (Random a) => (Int, Int) -> IO (Matrix a)
randomMatrixIO (m,n) = Matrix <$> replicateM m (randomVectorIO n)


-----------------------------------------------------------------------------
-- sequential matrix multiplication using Gentleman's algorithm;

seqMulT :: (Ring a, NFData a)
        => (Int,Int) -> Matrix a -> Matrix a -> Matrix a
seqMulT (k,l) a tr_b =
  evalShape $! result
    where
      result = unBlock $! fmap computeBlock args
      args   = blockTemplate (fst $ dim a, fst $ dim tr_b) (k,l)
      computeBlock (i,j) =
        id $!! mulT (stripeAt i k a) (stripeAt j l tr_b)


-----------------------------------------------------------------------------
-- parallel matrix multiplication using Gentleman's algorithm;
-- shared memory

parMulT :: (Ring a, NFData a)
        => (Int,Int) -> Matrix a -> Matrix a -> Par (Matrix a)
parMulT (k,l) a tr_b = do
  let addIVar x =
        new >>= \ v -> return (x, v)
  let mkTask ((i,j), v) =
        (force (mulT (stripeAt i k a) (stripeAt j l tr_b)) >>= put v, v)
  args <- mapM addIVar $ blockTemplate (fst $ dim a, fst $ dim tr_b) (k,l)
  let tasks = fmap mkTask args
  futures <- mapM (\ (act, v) -> fork act >> return v) tasks
  result <- unBlock <$> mapM get futures
  return $! evalShape result


-----------------------------------------------------------------------------
-- parallel matrix multiplication using Gentleman's algorithm;
-- distributed memory (monomorphic), pure work stealing

distStealMulT :: (Int,Int) -> Matrix Double' -> Matrix Double'
              -> Par (Matrix Double')
distStealMulT (k,l) a tr_b = do
  let addGIVar x =
        new >>= \ v -> glob v >>= \ gv -> return (x, gv, v)
  let mkTask ((stripe_i, stripe_j), gv, v) =
        ($(mkClosure [| distStealMulT_abs (stripe_i, stripe_j, gv) |]), v)
  args <- mapM addGIVar $
            fmap (\ (i,j) -> (stripeAt i k a, stripeAt j l tr_b)) $
              blockTemplate (fst $ dim a, fst $ dim tr_b) (k,l)
  let tasks = fmap mkTask args
  futures <- mapM ( \ (clo, v) -> spark one clo >> return v) tasks
  result <- unBlock <$> mapM (return . unClosure <=< get) futures
  return $! evalShape result

distStealMulT_abs :: (Matrix Double',
                      Matrix Double',
                      GIVar (Closure (Matrix Double')))
                  -> Thunk (Par ())
distStealMulT_abs (stripe_i, stripe_j, gv) =
  Thunk $ force (mulT stripe_i stripe_j) >>= rput gv . toClosure


-----------------------------------------------------------------------------
-- parallel matrix multiplication using Gentleman's algorithm;
-- distributed memory (monomorphic), predistribution of inputs + work stealing;
-- predistribution relies on `matrices` holding a pair of valid matrices.

pdistStealMulT :: (Int,Int) -> Matrix Double' -> Matrix Double'
               -> Par (Matrix Double')
pdistStealMulT (k,l) a tr_b = do
  let addGIVar x =
        new >>= \ v -> glob v >>= \ gv -> return (x, gv, v)
  let mkTask ((i,j), gv, v) =
        ($(mkClosure [| pdistStealMulT_abs (i, j, k, l, gv) |]), v)
  args <- mapM addGIVar $ blockTemplate (fst $ dim a, fst $ dim tr_b) (k,l)
  let tasks = fmap mkTask args
  futures <- mapM ( \ (clo, v) -> spark one clo >> return v) tasks
  result <- unBlock <$> mapM (return . unClosure <=< get) futures
  return $! evalShape result

pdistStealMulT_abs :: (Int, Int, Int, Int, GIVar (Closure (Matrix Double')))
                   -> Thunk (Par ())
pdistStealMulT_abs (i, j, k, l, gv) = Thunk $ do
  (a, tr_b) <- io $ readIORef matrices
  force (mulT (stripeAt i k a) (stripeAt j l tr_b)) >>= rput gv . toClosure


-----------------------------------------------------------------------------
-- Static declaration (just before 'main')

-- Empty splice; TH hack to make all environment abstractions visible.
$(return [])

-- orphan ToClosure instances (unavoidably so)
instance ToClosure (Matrix Double') where locToClosure = $(here)

declareStatic :: StaticDecl
declareStatic =
  mconcat
    [HdpH.declareStatic,         -- declare Static deserialisers
     declare (staticToClosure :: StaticToClosure (Matrix Double')),
     declare $(static 'distStealMulT_abs),
     declare $(static 'pdistStealMulT_abs)]


-----------------------------------------------------------------------------
-- initialisation, argument processing and 'main'

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
-- * dimension of square input/output matrices
-- * dimension of blocks (rows)
-- * dimension of blocks (columns)
parseArgs :: [String] -> (Int, Int, Int, Int)
parseArgs []     = (defVers, defN, defK, defK)
parseArgs (s:ss) =
  let go :: Int -> [String] -> (Int, Int, Int, Int)
      go v []           = (v, defN,    defK,    defK)
      go v [s1]         = (v, read s1, defK,    defK)
      go v [s1,s2]      = (v, read s1, read s2, read s2)
      go v (s1:s2:s3:_) = (v, read s1, read s2, read s3)
  in case stripPrefix "v" s of
       Just s' -> go (read s') ss
       Nothing -> go defVers (s:ss)

-- defaults for optional arguments
defVers, defN, defK :: Int
defVers = 1    -- version
defN    = 500  -- dimension of square input and output matrices
defK    = 125  -- block dimension


outputResults :: Int -> (Int,Int) -> Int
              -> (Maybe (Matrix Double', NominalDiffTime), NominalDiffTime)
              -> IO ()
outputResults ver blkdim dim (res, total_t) =
  case res of
    Nothing           -> return ()
    Just (out, run_t) ->
      zipWithM_ printTagged tags [ver', blkdim', dim', out', run_t', total_t']
        where
          printTagged tag val = putStrLn (tag ++ ": " ++ val)
          tags = ["VERSION","BLOCKDIM","MATRIXDIM","OUT","RUNTIME","TOTALTIME"]
          ver' = "v" ++ show ver
          blkdim'  = show (fst blkdim) ++ "x" ++ show (snd blkdim)
          dim'     = show dim ++ "x" ++ show dim
          sum_out  = unDouble' (sum out)
          out'     = show (4 * sum_out / fromIntegral (dim ^ 3)) -- expect ~ 1.0
          run_t'   = show run_t
          total_t' = show total_t


main :: IO ()
main = do
  hSetBuffering stdout LineBuffering
  hSetBuffering stderr LineBuffering
  register declareStatic
  opts_args <- getArgs
  (conf, seed, args) <- parseOpts opts_args
  let (version, n, k, l) = parseArgs args
  initrand seed
  -- create random input matrices (in normal form)
  t0 <- timeIO_ $ do
          a <- randomMatrixIO (n,n)
          b <- randomMatrixIO (n,n)
          atomicWriteIORef matrices $!! (a, tr b)
          -- NOTE: Random matrices are same everywhere if random seed /= 0.
  case version of
      0 -> outputResults version (k,l) n =<< do
             putStrLn $ show t0 ++ " [generating inputs]"
             (a, tr_b) <- readIORef matrices
             (x, t) <- timeIO $ return $! seqMulT (k,l) a tr_b
             return (Just (x, t), t)
      1 -> outputResults version (k,l) n =<< do
             timeIO $ runParIO conf $ do
               io $ putStrLn $ show t0 ++ " [generating inputs]"
               (a, tr_b) <- io $ readIORef matrices
               timePar $ parMulT (k,l) a tr_b
      2 -> outputResults version (k,l) n =<< do
             timeIO $ runParIO conf $ do
               io $ putStrLn $ show t0 ++ " [generating inputs]"
               (a, tr_b) <- io $ readIORef matrices
               timePar $ distStealMulT (k,l) a tr_b
      3 -> outputResults version (k,l) n =<< do
             timeIO $ runParIO conf $ do
               io $ putStrLn $ show t0 ++ " [generating inputs]"
               (a, tr_b) <- io $ readIORef matrices
               timePar $ pdistStealMulT (k,l) a tr_b
      _ -> return ()
