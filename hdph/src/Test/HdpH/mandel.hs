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
import Control.Monad (when, foldM)
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
import Data.Time.Clock (NominalDiffTime, diffUTCTime, getCurrentTime)
import Data.Typeable (Typeable)
import Data.Vector.Cereal () -- To get the generic deriving for cereal and vectors.
import qualified Data.Vector.Unboxed as V

import GHC.Generics (Generic)

import System.Environment (getArgs)
import System.IO (stdout, stderr, hSetBuffering, BufferMode(..))
import System.Random (mkStdGen, setStdGen)


import Prelude

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

mandelHdpHDandC :: Double -> Double -- (minX, MinY)
                -> Double -> Double -- (maxX, maxY)
                -> Int -> Int -- (winX, winY)
                -> Int -- Depth
                -> Int -- Threshold
                -> Par VecTree
mandelHdpHDandC minX minY maxX maxY winX winY maxDepth threshold = do
  res <- parDivideAndConquer
          $(mkClosure [|dc_trivial threshold|])
          $(mkClosure [|dc_decompose|])
          $(mkClosure [|dc_combine|])
          $(mkClosure [|dc_algorithm (minX, minY, maxX, maxY, winX, winY, maxDepth)|])
          (toClosure (0,winY-1))
  return $ unClosure res

{- parDivideAndConquer :: Closure (Closure a -> Bool)
                    -> Closure (Closure a -> [Closure a])
                    -> Closure (Closure a -> [Closure b] -> Closure b)
                    -> Closure (Closure a -> Par (Closure b))
                    -> Closure a
                    -> Par (Closure b)
-}

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
      --y = unClosure y_clo
      --vec = V.generate winX (\x -> mandelStep y x)
  in  force $ toClosure (Leaf $ foldl go V.empty [min..max])
  -- seq (vec V.! 0) $ return (toClosure (Leaf vec))
  where
    go a y = a V.++ V.generate winX (\x -> mandelStep y x)
    mandelStep i j = mandel maxDepth (calcZ i j)
    calcZ i j = ((fromIntegral j * r_scale) / fromIntegral winY + minY) :+
                ((fromIntegral i * c_scale) / fromIntegral winX + minX)
    r_scale =  maxY - minY  :: Double
    c_scale =  maxX - minX  :: Double

------------------------------------------------
-- Map and Reduce function closure implementations

map_f :: (Double,Double,Double,Double,Int,Int,Int)
      -> Closure Int
      -> Par (Closure VecTree)
map_f (minX,minY,maxX,maxY,winX,winY,maxDepth) = \y_clo -> do
    let y = unClosure y_clo
    let vec = V.generate winX (\x -> mandelStep y x)
    seq (vec V.! 0) $ return (toClosure (Leaf vec))
  where
    mandelStep i j = mandel maxDepth (calcZ i j)
    calcZ i j = ((fromIntegral j * r_scale) / fromIntegral winY + minY) :+
           ((fromIntegral i * c_scale) / fromIntegral winX + minX)
    r_scale =  maxY - minY  :: Double
    c_scale =   maxX - minX  :: Double

reduce_f :: Closure VecTree -> Closure VecTree -> Par (Closure VecTree)
reduce_f = \a_clo b_clo -> return $ toClosure (MkNode (unClosure a_clo) (unClosure b_clo))

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
     declare $(static 'map_f),
     declare $(static 'reduce_f),
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
      -- 0 -> do (pixels, t) <- timeIO $ evaluate =<< runParIO conf
      --           (dist_skel_par_mandel valX valY valDepth valThreshold)
      --         if isJust pixels
      --          then
      --            putStrLn $
      --           "{v0} mandel-par " ++
      --           "X=" ++ show valX ++ " Y=" ++ show valY ++
      --           " depth=" ++ show valDepth ++ " threshold=" ++ show valThreshold ++
      --           " checksum=" ++ show (checkSum (fromJust pixels)) ++
      --           " {runtime=" ++ show t ++ "}"
      --          else return ()
      -- 1 -> do (pixels, t) <- timeIO $ evaluate =<< runParIO conf
      --           (dist_skel_push_mandel valX valY valDepth valThreshold)
      --         if isJust pixels
      --          then
      --            putStrLn $
      --           "{v1} mandel-push " ++
      --           "X=" ++ show valX ++ " Y=" ++ show valY ++
      --           " depth=" ++ show valDepth ++ " threshold=" ++ show valThreshold ++
      --           " checksum=" ++ show (checkSum (fromJust pixels)) ++
      --           " {runtime=" ++ show t ++ "}"
      --          else return ()
      4 -> do (pixels, t) <- timeIO $ evaluate =<< return (MonadPar.runPar
                (monadparRunMandel valX valY valDepth valThreshold))
              putStrLn $
                "{v4} monadpar-mandel-par " ++
                "X=" ++ show valX ++ " Y=" ++ show valY ++
                " depth=" ++ show valDepth ++ " threshold=" ++ show valThreshold ++
                " checksum=" ++ show (checkSum pixels) ++
                " {runtime=" ++ show t ++ "}"
      5 -> do (pixels, t) <- timeIO $ evaluate =<< runParIO conf
                                (mandelHdpHDandC (-2) (-2) 2 2 valX valY valDepth valThreshold)
              when (isJust pixels) $
               putStrLn $
                "{v4} mandelHdpH " ++
                "X=" ++ show valX ++ " Y=" ++ show valY ++
                " depth=" ++ show valDepth ++ " threshold=" ++ show valThreshold ++
                " checksum=" ++ show (checkSum (fromJust pixels)) ++
                " {runtime=" ++ show t ++ "}"
      _ -> return ()

