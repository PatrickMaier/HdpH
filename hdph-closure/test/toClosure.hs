-- Test the space overhead of toClosure.
--
-- Will be compiled with -O0 to avoid optimizer affecting tests.
--
-- Author: Patrick Maier
-----------------------------------------------------------------------------

{-# OPTIONS_GHC -fno-warn-orphans #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE BangPatterns #-}

module Main where

import Prelude
import Control.DeepSeq (deepseq)
import Control.Exception (evaluate)
import qualified Data.ByteString as BS (length)
import Data.Functor ((<$>))
import Data.Monoid (mconcat)
import Data.List (stripPrefix)
import Data.Serialize (encode, decode)
import Data.Time.Clock (NominalDiffTime, diffUTCTime, getCurrentTime)
import System.Environment (getArgs)

import Control.Parallel.HdpH.Closure
       (Closure, unClosure,
        toClosure, ToClosure(locToClosure),
        StaticToClosure, staticToClosure,
        StaticDecl, declare, register, here)
import qualified Control.Parallel.HdpH.Closure (declareStatic)

-----------------------------------------------------------------------------
-- split a list into a list of fixed size chunks

chunk :: Int -> [a] -> [[a]]
chunk k xs | k <= 0 = error "chunk: chunk size non-positive"
           | otherwise = go xs
              where           
                go [] = []
                go zs = pref : go suff where (pref,suff) = splitAt k zs


-----------------------------------------------------------------------------
-- time an IO action
timeIO :: IO a -> IO (a, NominalDiffTime)
timeIO action = do t0 <- getCurrentTime
                   x <- action
                   t1 <- getCurrentTime
                   return (x, diffUTCTime t1 t0)


-----------------------------------------------------------------------------
-- measure serialisation of a list of lists of Ints

measureListListInt :: [[Int]] -> IO ((Int, Int), NominalDiffTime)
measureListListInt xss =
  timeIO $ do
    let bs = encode xss
    evaluate (bs `deepseq` ())
    let !sz = BS.length bs
    let !z  = sum $ concat $ either error id $ decode bs
    evaluate ((z, sz))


-----------------------------------------------------------------------------
-- measure serialisation of a closure containing a list of lists of Ints

measureCloListListInt :: [[Int]] -> IO ((Int, Int), NominalDiffTime)
measureCloListListInt xss =
  timeIO $ do
    let clo = toClosure xss :: Closure [[Int]]
    let bs = encode clo
    evaluate (bs `deepseq` ())
    let !sz = BS.length bs
    let !z  = sum $ concat $ unClosure $ either error id $ decode bs
    evaluate ((z, sz))


-----------------------------------------------------------------------------
-- measure serialisation of a list of closures, each containing a list of Ints

measureListCloListInt :: [[Int]] -> IO ((Int, Int), NominalDiffTime)
measureListCloListInt xss =
  timeIO $ do
    let clos = map toClosure xss :: [Closure [Int]]
    let bs = encode clos
    evaluate (bs `deepseq` ())
    let !sz = BS.length bs
    let !z  = sum $ concat $ map unClosure $ either error id $ decode bs
    evaluate ((z, sz))


-----------------------------------------------------------------------------
-- measure serialisation of a closure containing a list of closures,
-- each containing a list of Ints

measureCloListCloListInt :: [[Int]] -> IO ((Int, Int), NominalDiffTime)
measureCloListCloListInt xss =
  timeIO $ do
    let clos = map toClosure xss :: [Closure [Int]]
    let clo = toClosure clos :: Closure [Closure [Int]]
    let bs = encode clo
    evaluate (bs `deepseq` ())
    let !sz = BS.length bs
    let !z  = sum $ concat $ map unClosure $ unClosure $
                either error id $ decode bs
    evaluate ((z, sz))


-----------------------------------------------------------------------------
-- Static declaration (just before 'main')

-- Empty splice; TH hack to make all environment abstractions visible.
$(return [])

-- orphan ToClosure instances (unavoidably so)
instance ToClosure [Int] where locToClosure = $(here)
instance ToClosure [[Int]] where locToClosure = $(here)

declareStatic :: StaticDecl
declareStatic =
  mconcat
    [Control.Parallel.HdpH.Closure.declareStatic,
     declare (staticToClosure :: StaticToClosure [Int]),
     declare (staticToClosure :: StaticToClosure [[Int]])]


-----------------------------------------------------------------------------
-- argument parsing and main

-- parse (optional) arguments in this order:
-- * version to run
-- * length of list
-- * start value
-- * chunk size
parseArgs :: [String] -> (Int, Int, Int, Int)
parseArgs []     = (defVers, defLength, defStart, defChunk)
parseArgs (s:ss) =
  let go :: Int -> [String] -> (Int, Int, Int, Int)
      go v []           = (v, defLength, defStart, defChunk)
      go v [s1]         = (v, read s1,   defStart, defChunk)
      go v [s1,s2]      = (v, read s1,   read s2,  defChunk)
      go v (s1:s2:s3:_) = (v, read s1,   read s2,  read s3)
  in case stripPrefix "v" s of
       Just s' -> go (read s') ss
       Nothing -> go defVers (s:ss)

defVers :: Int
defVers = 0

defLength, defStart, defChunk :: Int
defLength = 100000
defStart  =      1
defChunk  =    100


main :: IO ()
main = do
  register declareStatic
  (version, n, x, k) <- parseArgs <$> getArgs
  let xss = chunk k $ take n [x ..]
  -- dispatch on version
  ((z, sz), t) <- case version of
                    0 -> measureListListInt xss
                    1 -> measureCloListListInt xss
                    2 -> measureListCloListInt xss
                    3 -> measureCloListCloListInt xss
                    _  -> error $ "version v" ++ show version ++ " unsupported"
  putStrLn $ "{v" ++ show version ++ ", chunk " ++ show k ++
             " $ take " ++ show n ++ " [" ++ show x ++ " ..]} sum=" ++ show z ++
             " {t=" ++ show t ++ ", size=" ++ show sz ++ "bytes}"
