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


-----------------------------------------------------------------------------
-- time an IO action
timeIO :: IO a -> IO (a, NominalDiffTime)
timeIO action = do t0 <- getCurrentTime
                   x <- action
                   t1 <- getCurrentTime
                   return (x, diffUTCTime t1 t0)


-----------------------------------------------------------------------------
-- serialize a list of Ints

measureListInt :: Int -> Int -> IO ((Int, Int), NominalDiffTime)
measureListInt n x =
  timeIO $ do
    let bs = encode $ take n [x ..]
    evaluate (bs `deepseq` ())
    let !sz = BS.length bs
    let !z  = sum $ either error id $ decode bs
    evaluate ((z, sz))


-----------------------------------------------------------------------------
-- serialize a closure containing a list of Ints

measureCloListInt :: Int -> Int -> IO ((Int, Int), NominalDiffTime)
measureCloListInt n x =
  timeIO $ do
    let clo = toClosure $ take n [x ..] :: Closure [Int]
    let bs = encode clo
    evaluate (bs `deepseq` ())
    let !sz = BS.length bs
    let !z  = sum $ unClosure $ either error id $ decode bs
    evaluate ((z, sz))


-----------------------------------------------------------------------------
-- serialize a list of Int closures

measureListCloInt :: Int -> Int -> IO ((Int, Int), NominalDiffTime)
measureListCloInt n x =
  timeIO $ do
    let clos = map toClosure $ take n [x ..] :: [Closure Int]
    let bs = encode clos
    evaluate (bs `deepseq` ())
    let !sz = BS.length bs
    let !z  = sum $ map unClosure $ either error id $ decode bs
    evaluate ((z, sz))


-----------------------------------------------------------------------------
-- serialize a list of closures, each containing a list of Ints

measureListCloListInt :: Int -> Int -> Int -> IO ((Int, Int), NominalDiffTime)
measureListCloListInt n x k =
  timeIO $ do
    let clos = map toClosure $ chunk k $ take n [x ..] :: [Closure [Int]]
    let bs = encode clos
    evaluate (bs `deepseq` ())
    let !sz = BS.length bs
    let !z  = sum $ concat $ map unClosure $ either error id $ decode bs
    evaluate ((z, sz))

chunk :: Int -> [a] -> [[a]]
chunk _ [] = []
chunk k xs = ys : chunk k zs where (ys,zs) = splitAt k xs


-----------------------------------------------------------------------------
-- Static declaration (just before 'main')

-- Empty splice; TH hack to make all environment abstractions visible.
$(return [])

-- orphan ToClosure instances (unavoidably so)
instance ToClosure Int where locToClosure = $(here)
instance ToClosure [Int] where locToClosure = $(here)

declareStatic :: StaticDecl
declareStatic =
  mconcat
    [declare (staticToClosure :: StaticToClosure Int),
     declare (staticToClosure :: StaticToClosure [Int])]


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
  -- dispatch on version
  ((z, sz), t) <- case version of
                    0 -> measureListInt n x
                    1 -> measureCloListInt n x
                    2 -> measureListCloInt n x
                    3 -> measureListCloListInt n x k
                    _  -> error $ "version v" ++ show version ++ " unsupported"
  putStrLn $ "{v" ++ show version ++
             ", n=" ++ show n ++ ", x=" ++ show x ++
             ", k=" ++ show k ++ "} sum=" ++ show z ++
             " {t=" ++ show t ++ ", size=" ++ show sz ++ "bytes}"
