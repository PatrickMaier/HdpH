-- Test the overhead of nested explicit closures,
-- depending on whether closures are in WHNF, in NF, or serialised.
--
-- Author: Patrick Maier
--
-- compile: ghc --make -O2 nested_opt.hs
--
-----------------------------------------------------------------------------

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
    [declare $(static 'reduce_abs),
     declare $(static 'mkFlat_abs),
     declare $(static 'mkNested_abs)]

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
-- closure construction (assuming input list of lists is in normal form)

mkFlat :: [[Int32]] -> Closure Int32
mkFlat xss = $(mkClosure [| mkFlat_abs xss |])

mkFlat_abs :: [[Int32]] -> Thunk Int32
mkFlat_abs xss = Thunk $ sum $ map reduce xss

mkNested :: [[Int32]] -> Closure Int32
mkNested xss = $(mkClosure [| mkNested_abs clos |])
  where
    clos = map (\ xs -> $(mkClosure [| reduce_abs xs |])) xss

mkNested_abs :: [Closure Int32] -> Thunk Int32
mkNested_abs clos = Thunk $ sum $ map unClosure clos

reduce_abs :: [Int32] -> Thunk Int32
reduce_abs xs = Thunk $ reduce xs


-----------------------------------------------------------------------------
-- reductions (assuming input list of lists is in normal form)

-- no closures
redNative :: [[Int32]] -> IO (Int32, NominalDiffTime)
redNative xss = timeIO $ evaluate (sum $ map reduce xss)

-- flat closure
redFlat :: [[Int32]] -> IO (Int32, NominalDiffTime)
redFlat xss = timeIO $ do
                let clo = mkFlat xss
                evaluate (unClosure clo)

-- flat closure in normal form
redFlatNF :: [[Int32]] -> IO (Int32, NominalDiffTime)
redFlatNF xss = timeIO $ do
                  let clo = mkFlat xss
                  evaluate (rnf clo)
                  evaluate (unClosure clo)

-- flat closure serialised to ByteString
redFlatBS :: [[Int32]] -> IO (Int32, NominalDiffTime)
redFlatBS xss = timeIO $ do
                  let bs = encode $ mkFlat xss
                  evaluate (bs `seq` ())
                  evaluate (unClosure $ either error id $ decode bs)

-- nested closure
redNested :: [[Int32]] -> IO (Int32, NominalDiffTime)
redNested xss = timeIO $ do
                  let clo = mkNested xss
                  evaluate (unClosure clo)

-- nested closure in normal form
redNestedNF :: [[Int32]] -> IO (Int32, NominalDiffTime)
redNestedNF xss = timeIO $ do
                    let clo = mkNested xss
                    evaluate (rnf clo)
                    evaluate (unClosure clo)

-- nested closure serialised to ByteString
redNestedBS :: [[Int32]] -> IO (Int32, NominalDiffTime)
redNestedBS xss = timeIO $ do
                    let bs = encode $ mkNested xss
                    evaluate (bs `seq` ())
                    evaluate (unClosure $ either error id $ decode bs)


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
  (xss, t0) <- timeIO $ do
    let xss = take chunks $ generate chunksize seed
    evaluate (rnf xss)
    return xss
  putStrLn $ "Generating input: t=" ++ show t0
  -- dispatch on version
  (z, t) <- case version of
              0  -> redNative xss
              11 -> redFlat xss
              12 -> redFlatNF xss
              13 -> redFlatBS xss
              21 -> redNested xss
              22 -> redNestedNF xss
              23 -> redNestedBS xss
              _  -> error $ "version v" ++ show version ++ " not supported"
  putStrLn $ "{v" ++ show version ++ 
             ", chunks=" ++ show chunks ++
             ", chunksize=" ++ show chunksize ++
             ", seed=" ++ show seed ++ "}  reduce=" ++ show z ++
             " {t=" ++ show t ++ "}"
