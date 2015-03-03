{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveGeneric #-}

-- HdpH implementaion of a Mandelbrot set creation
--
-- Based largely on code by Rob Stewart <R.Stewart@hw.ac.uk> who adapted it
-- from the monad-par implementation.
--
-- Author: Blair Archbald
-- Date: 2/3/2015
-----------------------------------------------------------------------------


module Main where

import Control.DeepSeq hiding (force)
import Control.Exception (evaluate)
import Control.Monad (when)
import Control.Parallel.HdpH
import Control.Parallel.HdpH.Strategies
import qualified Control.Monad.Par as MonadPar
import qualified Control.Monad.Par.Combinator as MonadPar.C
import qualified Control.Parallel.HdpH as HdpH (declareStatic)
import qualified Control.Parallel.HdpH.Strategies as Strategies (declareStatic)

import Data.Binary (Binary)
import Data.Complex
import Data.Functor ((<$>))
import Data.List (stripPrefix)
import Data.List.Split (splitOn)
import Data.Maybe (isJust,fromJust)
import Data.Monoid (mconcat)
import Data.Serialize
import Data.Typeable (Typeable)
import Data.Vector.Cereal () -- To get the generic deriving for cereal and vectors.
import qualified Data.Vector.Unboxed as V

import GHC.Generics (Generic)

import System.Clock
import System.Environment (getArgs)
import System.IO (stdout, stderr, hSetBuffering, BufferMode(..))
import System.Random (mkStdGen, setStdGen)

import Prelude

type MSTime = Double

data VecTree = Leaf (V.Vector Int)
             | MkNode VecTree VecTree
             deriving (Eq,Show,Generic,Typeable)
instance Serialize VecTree
instance NFData VecTree
instance ToClosure VecTree
  where locToClosure = $(here)

instance ToClosure (Int, Int)
  where locToClosure = $(here)

----------------------------------------------------------------------------
-- sequential mandel function

mandel :: Int -> Complex Double -> Int
mandel max_depth c = loop 0 0
  where
   loop i !z
    | i == max_depth       = i
    | magnitude z >= 2.0   = i
    | otherwise            = loop (i+1) (z*z + c)

checkSum :: VecTree -> Int
checkSum (Leaf vec) = V.foldl (+) 0 vec
checkSum (MkNode v1 v2) = checkSum v1 + checkSum v2

-----------------------------------------------
-- Mandel using monad-par
--  From: https://github.com/simonmar/monad-par/blob/master/examples/src/mandel.hs

monadparRunMandel :: Int -> Int -> Int -> Int -> MonadPar.Par VecTree
monadparRunMandel = monadparRunMandel' (-2) (-2) 2 2

--This is where the skeleton comes from.
monadparRunMandel' :: Double -> Double -- (minX, MinY)
                    -> Double -> Double -- (maxX, maxY)
                    -> Int -> Int -- (winX, winY)
                    -> Int -- Depth
                    -> Int -- Threshold
                    -> MonadPar.Par VecTree
monadparRunMandel' minX minY maxX maxY winX winY max_depth threshold =
  MonadPar.C.parMapReduceRangeThresh threshold (MonadPar.C.InclusiveRange 0 (winY-1))
     (\y ->
       do
          let vec = V.generate winX (\x -> mandelStep y x)
          seq (vec V.! 0) $ return (Leaf vec))
     (\ a b -> return$ MkNode a b)
     (Leaf V.empty)
  where
    mandelStep i j = mandel max_depth z
        where z = ((fromIntegral j * r_scale) / fromIntegral winY + minY) :+
                  ((fromIntegral i * c_scale) / fromIntegral winX + minX)
    r_scale  =  maxY - minY  :: Double
    c_scale =   maxX - minX  :: Double

-----------------------------------------------
-- Mandel using the generic HpdH D&C skeleton

hdphDandCMandel :: Int -> Int -> Int -> Int -> Par VecTree
hdphDandCMandel = hdphDandCMandel' (-2) (-2) 2 2

hdphDandCMandel' :: Double -> Double -- (minX, MinY)
                 -> Double -> Double -- (maxX, maxY)
                 -> Int -> Int -- (winX, winY)
                 -> Int -- Depth
                 -> Int -- Threshold
                 -> Par VecTree
hdphDandCMandel' minX minY maxX maxY winX winY maxDepth threshold = do
  res <- parDivideAndConquer
          $(mkClosure [|dc_trivial threshold|])
          $(mkClosure [|dc_decompose|])
          $(mkClosure [|dc_combine|])
          $(mkClosure [|dc_algorithm (minX, minY, maxX, maxY, winX, winY, maxDepth)|])
          (toClosure (0,winY-1))
  return $ unClosure res

dc_trivial :: Int -> Thunk (Closure (Int, Int) -> Bool)
dc_trivial threshold = Thunk $ \bnds -> let (min,max) = unClosure bnds
                                        in   max - min <= threshold

dc_decompose :: Closure (Int, Int) -> [Closure (Int, Int)]
dc_decompose bnds = let (min, max) = unClosure bnds
                        mid = min + (max - min) `quot` 2
                     in [toClosure (min, mid), toClosure (mid+1, max)]

-- This could prove to be a bottleneck and reason to use Rob's Skeleton
-- (TODO: compare performance between approaches)
dc_combine :: Closure a -> [Closure VecTree] -> Closure VecTree
dc_combine _ ts = toClosure $ foldl1 MkNode $ map unClosure ts

dc_algorithm :: (Double, Double, -- (minX, minY)
                 Double, Double, -- (maxX, maxY)
                 Int, Int, -- (winX, winY)
                 Int) -- maxDepth
              -> Thunk (Closure (Int,Int) -> Par (Closure VecTree))
dc_algorithm (minX, minY, maxX, maxY, winX, winY, maxDepth) = Thunk $ \bnds ->
  let (min,max) = unClosure bnds
      v = foldl go V.empty [min..max]
  in  do vec <- force v
         return $ toClosure (Leaf vec)
  where
    go a y = a V.++ V.generate winX (\x -> mandelStep y x)
    mandelStep i j = mandel maxDepth (calcZ i j)
    calcZ i j = ((fromIntegral j * r_scale) / fromIntegral winY + minY) :+
                ((fromIntegral i * c_scale) / fromIntegral winX + minX)
    r_scale =  maxY - minY  :: Double
    c_scale =  maxX - minX  :: Double

-----------------------------------------------------------------------------
-- initialisation, argument processing and 'main'

timeDiffMSecs :: TimeSpec -> TimeSpec -> MSTime
timeDiffMSecs (TimeSpec s1 n1) (TimeSpec s2 n2) = fromIntegral (t2 - t1)
                                                          /
                                                  fromIntegral (10 ^ 6)
  where t1 = (fromIntegral s1 * 10 ^ 9) + fromIntegral n1
        t2 = (fromIntegral s2 * 10 ^ 9) + fromIntegral n2

timeIOMs :: IO a -> IO (a, MSTime)
timeIOMs action = do s  <- getTime Monotonic
                     x  <- action
                     e  <- getTime Monotonic
                     return (x, timeDiffMSecs s e)

-- initialize random number generator
initrand :: Int -> IO ()
initrand seed = when (seed /= 0) $ setStdGen (mkStdGen seed)

-----------------------------------------------------
-- Static Closures

$(return []) -- Bring the types into scope so that reify works.
declareStatic :: StaticDecl
declareStatic =
  mconcat
    [HdpH.declareStatic,         -- declare Static deserialisers
     Strategies.declareStatic,   -- from imported modules
     declare (staticToClosure :: StaticToClosure VecTree),
     declare (staticToClosure :: StaticToClosure (Int,Int)),

     declare $(static 'dc_trivial),
     declare $(static 'dc_decompose),
     declare $(static 'dc_combine),
     declare $(static 'dc_algorithm),

     declare $(static 'mandel)]
---------------------------------------------------------------------------

-- parse (optional) arguments in this order:
-- * version to run
-- * X value for Mandel
-- * Y value for Mandel
-- * Depth value for Mandel
-- * Threshold for Mandel
parseArgs :: [String] -> (Int, Int, Int, Int, Int, Int)
parseArgs []     = (defVers, defX, defY, defDepth,defThreshold,defExpected)
parseArgs (s:ss) =
  let go :: Int -> [String] -> (Int, Int, Int, Int,Int, Int)
      go v []              = (v, defX, defY, defDepth,defThreshold,defExpected)
      go v [s1]            = (v, defX, read s1,  defDepth,defThreshold,defExpected)
      go v [s1,s2]         = (v, read s1,  read s2,  defDepth,defThreshold,defExpected)
      go v [s1,s2,s3]      = (v, read s1,  read s2,  read s3, defThreshold,defExpected)
      go v [s1,s2,s3,s4] = (v, read s1,  read s2,  read s3, read s4,defExpected)
      go v (s1:s2:s3:s4:s5:_) = (v, read s1,  read s2,  read s3, read s4, read s5)
  in case stripPrefix "v" s of
       Just s' -> go (read s') ss
       Nothing -> go defVers (s:ss)

-- defaults from Simon Marlow from monad-par example
defVers, defX, defY, defDepth, defThreshold, defExpected :: Int
defVers      = 0
defX         = 1024
defY         = 1024
defDepth     = 256
defThreshold = 1
defExpected  = 0

main :: IO ()
main = do
  hSetBuffering stdout LineBuffering
  hSetBuffering stderr LineBuffering
  register Main.declareStatic

  hdphCfg    <- flip updateConf defaultRTSConf =<< getArgs
  (conf, as) <- case hdphCfg of
    Left err -> error $ "Could not initialise HdpH config: " ++ show err
    Right a  -> return a

  let (version, valX, valY, valDepth,valThreshold,expected) = parseArgs as
  --initrand seed --Figure out if we need this later. Get this from the args?
  case version of
      1 -> do (p, t) <- timeIOMs $ evaluate =<< return (MonadPar.runPar
                (monadparRunMandel valX valY valDepth valThreshold))
              printOutput (Just p,t)
      2 -> do res <- timeIOMs $ evaluate =<< runParIO conf
                                (hdphDandCMandel valX valY valDepth valThreshold)
              printOutput res
      _ -> return ()

printOutput :: (Maybe VecTree, MSTime) -> IO ()
printOutput (p,t) = case p of
                     Just pixels -> do
                       putStrLn $ "CHECKSUM: " ++ show (checkSum pixels)
                       putStrLn $ "RUNTIME: "  ++ show t ++ "ms"
                     Nothing -> return ()
