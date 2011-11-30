-- Sum of totients in HdpH_IO
--
-- Visibility: HdpH test suite
-- Author: Patrick Maier <P.Maier@hw.ac.uk>
-- Created: 17 Jul 2011
--
-----------------------------------------------------------------------------

module Main where

import Prelude
import Control.Concurrent (forkIO)
import Control.DeepSeq (NFData, deepseq)
import Data.List (stripPrefix)
import Data.Functor ((<$>))
import Data.Monoid (mconcat)
import System.Environment (getArgs)
import System.IO (stdout, stderr, hSetBuffering, BufferMode(..))

import MP.MPI (defaultWithMPI)
import HdpH_IO (withHdpH,
                NodeId, allNodes,
                pushTo,
                IVar, new, get, put,
                glob, rput,
                Env, encodeEnv, decodeEnv,
                Closure, unClosure, toClosure, unsafeMkClosure,
                DecodeStatic, decodeStatic,
                Static, staticAs, declare, register)


-----------------------------------------------------------------------------
-- 'Static' declaration and registration

instance DecodeStatic Integer

registerStatic :: IO ()
registerStatic =
  register $ mconcat
    [declare totient_Static,
     declare sum_totient_Static,
     declare dist_sum_totient_Static,
     declare (decodeStatic :: Static (Env -> Integer))]


-----------------------------------------------------------------------------
-- Euler's totient function (for positive integers)

totient :: Int -> Integer
totient n = toInteger $ length $ filter (\ k -> gcd n k == 1) [1 .. n]

totient_Static :: Static (Env -> Int -> Integer)
totient_Static = staticAs
  (const totient)
  "Main.totient"


-----------------------------------------------------------------------------
-- sequential sum of totients

sum_totient :: [Int] -> Integer
sum_totient = sum . map totient

sum_totient_Static :: Static (Env -> [Int] -> Integer)
sum_totient_Static = staticAs
  (const sum_totient)
  "Main.sum_totient"


-----------------------------------------------------------------------------
-- parallel sum of totients; shared memory IO threads

par_sum_totient :: Int -> Int -> Int -> IO Integer
par_sum_totient lower upper chunksize =
  sum <$> (mapM join =<< (mapM fork_sum_euler $ chunked_list))
    where
      chunked_list = chunk chunksize [upper, upper - 1 .. lower] :: [[Int]]

      fork_sum_euler :: [Int] -> IO (IVar Integer)
      fork_sum_euler xs = do v <- new
                             let job = put v $ force $ sum_totient xs
                             forkIO job
                             return v

      join :: IVar Integer -> IO Integer
      join = get


-----------------------------------------------------------------------------
-- distributed sum of totients; explicit round-robin placement

dist_sum_totient :: Int -> Int -> Int -> IO Integer
dist_sum_totient lower upper chunksize = do
  nodes <- allNodes
  let chunks_round_robin = zip chunked_list (cycle nodes)
  sum <$> (mapM join =<< (mapM push_sum_euler $ chunks_round_robin))
    where
      chunked_list = chunk chunksize [upper, upper - 1 .. lower] :: [[Int]]

      push_sum_euler :: ([Int], NodeId) -> IO (IVar (Closure Integer))
      push_sum_euler (xs,node) = do
        v <- new
        gv <- glob v
        let job = rput gv $ toClosure $ force $ sum_totient xs
        let env = encodeEnv (xs, gv)
        let fun = dist_sum_totient_Static
        pushTo (unsafeMkClosure job fun env) node
        return v

      join :: IVar (Closure Integer) -> IO Integer
      join v = unClosure <$> get v

dist_sum_totient_Static :: Static (Env -> IO ())
dist_sum_totient_Static = staticAs
  (\ env -> let (xs, gv) = decodeEnv env
              in rput gv $ toClosure $ force $ sum_totient xs)
  "Main.dist_sum_totient"


-----------------------------------------------------------------------------
-- chunking up lists; inverse of 'chunk n' is 'concat'

chunk :: Int -> [a] -> [[a]]
chunk n [] = []
chunk n xs = ys : chunk n zs where (ys,zs) = splitAt n xs


-----------------------------------------------------------------------------
-- argument processing and 'main'

-- parse (optional) arguments in this order: 
-- * version to run
-- * lower bound for Euler's totient function
-- * upper bound for Euler's totient function
-- * size of chunks (evaluated sequentially)
parseArgs :: [String] -> (Int, Int, Int, Int)
parseArgs []     = (defVers, defLower, defUpper, defChunk)
parseArgs (s:ss) =
  let go :: Int -> [String] -> (Int, Int, Int, Int)
      go v []           = (v, defLower, defUpper, defChunk)
      go v [s1]         = (v, defLower, read s1,  defChunk)
      go v [s1,s2]      = (v, read s1,  read s2,  defChunk)
      go v (s1:s2:s3:_) = (v, read s1,  read s2,  read s3)
  in case stripPrefix "v" s of
       Just s' -> go (read s') ss
       Nothing -> go defVers (s:ss)


defVers  =     2 :: Int
defLower =     1 :: Int
defUpper = 20000 :: Int
defChunk =   100 :: Int


main :: IO ()
main = do
  hSetBuffering stdout LineBuffering
  hSetBuffering stderr LineBuffering
  registerStatic
  defaultWithMPI $ do
    args <- getArgs
    let (version, lower, upper, chunksize) = parseArgs args
    case version of
      0 -> do x <- return $ sum_totient [upper, upper - 1 .. lower]
              putStrLn $
                "{v0} sum $ map totient [" ++ show lower ++ ".." ++
                show upper ++ "] = " ++ show x
      1 -> do x <- par_sum_totient lower upper chunksize
              putStrLn $
                "{v1, chunksize=" ++ show chunksize ++ "} " ++
                "sum $ map totient [" ++ show lower ++ ".." ++
                show upper ++ "] = " ++ show x
      2 -> do output <- withHdpH $
                          dist_sum_totient lower upper chunksize
              case output of
                Just x  -> putStrLn $
                             "{v2, chunksize=" ++ show chunksize ++ "} " ++
                             "sum $ map totient [" ++ show lower ++ ".." ++
                             show upper ++ "] = " ++ show x
                Nothing -> return ()
      _ -> return ()


-----------------------------------------------------------------------------
-- auxiliaries

-- force to normal form; taken from deepseq-1.2.0.1
force :: (NFData a) => a -> a
force x = x `deepseq` x
