-- Test the overhead of explicit closure elimination,
-- depending on whether closures are in WHNF, in NF, or serialised.
--
-- Author: Patrick Maier
-----------------------------------------------------------------------------

{-# OPTIONS_GHC -O0 #-}  -- get optimiser out of the way for these tests
{-# LANGUAGE TemplateHaskell #-}

module Main where

import Prelude
import Control.DeepSeq (NFData, rnf)
import Control.Exception (evaluate)
import Data.Functor ((<$>))
import Data.Int (Int32)
import Data.Monoid (mconcat)
import Data.List (stripPrefix)
import Data.Serialize (encode, decode)
import Data.Time.Clock (NominalDiffTime, diffUTCTime, getCurrentTime)
import System.Environment (getArgs)

import Control.Parallel.HdpH.Closure
       (Closure, mkClosure, unClosure, Thunk(Thunk),
        StaticDecl, static, declare, register)

-----------------------------------------------------------------------------
-- Static declaration

declareStatic :: StaticDecl
declareStatic =
  mconcat
    [declare $(static 'reduce_abs)]

-----------------------------------------------------------------------------
-- time an IO action
timeIO :: IO a -> IO (a, NominalDiffTime)
timeIO action = do t0 <- getCurrentTime
                   x <- action
                   t1 <- getCurrentTime
                   return (x, diffUTCTime t1 t0)

-----------------------------------------------------------------------------
-- cut an input list into chunks of given size

chunk :: Int -> [a] -> [[a]]
chunk _ [] = []
chunk n xs = ys : chunk n zs where (ys,zs) = splitAt n xs

-----------------------------------------------------------------------------
-- force all elements in a list to WHNF

seqList :: [a] -> ()
seqList []     = ()
seqList (x:xs) = x `seq` seqList xs

-----------------------------------------------------------------------------
-- actual computations (on lists of 32-bit ints)

-- generate an infinite list of size 'c' chunks of 32-bit ints from given seed
generate :: Int -> Int32 -> [[Int32]]
generate c = chunk c . iterate p

-- reduce a lists of 32-bit ints
reduce :: [Int32] -> Int32
reduce = sum . map p

-- some polynomial on Int32
p :: Int32 -> Int32
p v = 1 * v^12 + 5 * v^10 + 13 * v^8 + 25 * v^6 + 40 * v^4 + 55 *v^2 + 67 -
      75 * v - 78 * v^3 - 75 * v^5 - 67 * v^7 - 55 * v^9 - 40 * v^11 -
      25 * v^13 - 13* v^15 - 5 * v^17 - 1 * v^19


-----------------------------------------------------------------------------
-- reductions (assuming input list of lists is in normal form)

-- no closures
redNative :: [[Int32]] -> IO (Int32, NominalDiffTime)
redNative xss = timeIO $ evaluate (sum $ map reduce xss)


-- list of closures
redClosure :: [[Int32]] -> IO (Int32, NominalDiffTime)
redClosure xss = do
  let clos = map (\ xs -> $(mkClosure [| reduce_abs xs |])) xss
  evaluate (seqList clos)
  timeIO $ evaluate (sum $ map unClosure clos)

reduce_abs :: [Int32] -> Thunk Int32
reduce_abs xs = Thunk $ reduce xs


-- list of closures in normal form
redClosureNF :: [[Int32]] -> IO (Int32, NominalDiffTime)
redClosureNF xss = do
  let clos = map (\ xs -> $(mkClosure [| reduce_abs xs |])) xss
  evaluate (rnf clos)
  timeIO $ evaluate (sum $ map unClosure clos)


-- list of closures serialised to ByteStrings
redClosureBS :: [[Int32]] -> IO (Int32, NominalDiffTime)
redClosureBS xss = do
  let bs = map (\ xs -> encode $(mkClosure [| reduce_abs xs |])) xss
  evaluate (seqList bs)
  timeIO $ evaluate (sum $ map (unClosure . either error id . decode) bs)


-----------------------------------------------------------------------------
-- argument parsing and main

-- parse (optional) arguments in this order: 
-- * version to run
-- * chunk size
-- * number of chunks
-- * seed value
parseArgs :: [String] -> (Int, Int, Int, Int32)
parseArgs []     = (defVers, defChunks, defChunkSize, defSeed)
parseArgs (s:ss) =
  let go :: Int -> [String] -> (Int, Int, Int, Int32)
      go v []           = (v, defChunks, defChunkSize, defSeed)
      go v [s1]         = (v, defChunks, read s1,      defSeed)
      go v [s1,s2]      = (v, read s2,   read s1,      defSeed)
      go v (s1:s2:s3:_) = (v, read s2,   read s1,      read s3)
  in case stripPrefix "v" s of
       Just s' -> go (read s') ss
       Nothing -> go defVers (s:ss)

defVers :: Int
defVers = 0

defChunks, defChunkSize :: Int
defChunks    = 50000
defChunkSize =    10

defSeed :: Int32
defSeed = 1


main :: IO ()
main = do
  register declareStatic
  (version, chunks, chunksize, seed) <- parseArgs <$> getArgs
  -- generate list of lists (in normal form)
  let xss = take chunks $ generate chunksize seed
  evaluate (rnf xss)
  -- dispatch on version
  (z, t) <- case version of
              0 -> redNative xss
              1 -> redClosure xss
              2 -> redClosureNF xss
              3 -> redClosureBS xss
              _ -> error $ "version v" ++ show version ++ " not supported"
  putStrLn $ "{v" ++ show version ++ 
             ", chunks=" ++ show chunks ++
             ", chunksize=" ++ show chunksize ++
             ", seed=" ++ show seed ++ "}  reduce=" ++ show z ++
             " {t=" ++ show t ++ "}"
