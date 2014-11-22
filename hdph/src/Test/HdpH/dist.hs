-- Testing distance metric and the like in HdpH
--
-- Author: Patrick Maier
-----------------------------------------------------------------------------

{-# LANGUAGE FlexibleInstances #-}  -- for some ToClosure instances
{-# LANGUAGE TemplateHaskell #-}    -- for mkClosure, etc

{-# OPTIONS_GHC -fno-warn-orphans #-}

module Main where

import Prelude
import Data.Functor ((<$>))
import Data.List (sort)
import Data.Monoid (mconcat)
import System.Environment (getArgs)
import System.IO (stdout, stderr, hSetBuffering, BufferMode(..))

import Control.Parallel.HdpH
       (RTSConf(..), defaultRTSConf, updateConf,
        Par, runParIO_,
        allNodes, dist, equiDist, force, io,
        Node, Dist, one, div2,
        Closure, mkClosure,
        toClosure, ToClosure(locToClosure),
        static, StaticToClosure, staticToClosure,
        StaticDecl, declare, register, here)
import qualified Control.Parallel.HdpH as HdpH (declareStatic)
import Control.Parallel.HdpH.Strategies (pushMapM)
import qualified Control.Parallel.HdpH.Strategies as Strategies (declareStatic)


-----------------------------------------------------------------------------
-- computing and comparing the graph of distance metric on all nodes

type DistGraph = [(Node, Node, Dist)]

-- Compute Closure wrapping sorted graph of distance metric
myDistGraph :: Par (Closure DistGraph)
myDistGraph = do
  nodes <- allNodes
  graph <- force $ sort [(p, q, dist p q) | p <- nodes, q <- nodes]
  return $ toClosure graph

const_myDistGraph :: a -> Par (Closure DistGraph)
const_myDistGraph = const myDistGraph

allDistGraphs :: Par [DistGraph]
allDistGraphs = do
  nodes <- allNodes
  pushMapM nodes $(mkClosure [|const_myDistGraph|]) nodes

checkDistGraphs :: Par ()
checkDistGraphs = do
  graph1:graphs <- allDistGraphs
  let n = length graphs + 1
  let k = length graph1
  io $ putStrLn $ "Graph of distance metric (" ++ show k ++ " lines)"
  mapM_ (\e -> io $ putStrLn $ show e) graph1
  let match = all (graph1 ==) graphs
  io $ putStrLn $ "all " ++ show n ++ " graphs match: " ++ show match


-----------------------------------------------------------------------------
-- retrieving all nodes by gathering equidistant bases

-- Gather nodes within radius 'r' around the current node
gatherNodes :: Dist -> Par (Closure [Node])
gatherNodes r = do
  basis <- equiDist r
  case basis of
    [(p0, 1)] -> return $ toClosure [p0]
    _ -> do let qs = map fst basis
            let rs = [div2 r | _ <- qs]
            toClosure <$> concat <$> pushMapM qs $(mkClosure [|gatherNodes|]) rs

allGatherNodes :: Par [[Node]]
allGatherNodes = do
  nodes <- allNodes
  pushMapM nodes $(mkClosure [|gatherNodes|]) [one | _ <- nodes]

checkGatheredNodes :: Par ()
checkGatheredNodes = do
  nodes <- sort <$> allNodes
  nodess <- map sort <$> allGatherNodes
  let n = length nodess
  let match = all (nodes ==) nodess
  io $ putStrLn $ "all " ++ show n ++ " node lists match: " ++ show match


-----------------------------------------------------------------------------
-- Static declaration (just before 'main')

-- Empty splice; TH hack to make all environment abstractions visible.
$(return [])

-- orphan ToClosure instances (unavoidably so)
instance ToClosure Node where locToClosure = $(here)
instance ToClosure Dist where locToClosure = $(here)
instance ToClosure DistGraph where locToClosure = $(here)

declareStatic :: StaticDecl
declareStatic = mconcat
  [HdpH.declareStatic,
   Strategies.declareStatic,
   declare (staticToClosure :: StaticToClosure Node),
   declare (staticToClosure :: StaticToClosure Dist),
   declare (staticToClosure :: StaticToClosure DistGraph),
   declare $(static 'const_myDistGraph),
   declare $(static 'gatherNodes)]


-----------------------------------------------------------------------------
-- initialisation, argument processing and 'main'

-- parse runtime system config options; abort if there is an error
parseOpts :: [String] -> IO (RTSConf, [String])
parseOpts args = do
  either_conf <- updateConf args defaultRTSConf
  case either_conf of
    Left err_msg                 -> error $ "parseOpts: " ++ err_msg
    Right (conf, remaining_args) -> return (conf, remaining_args)


main :: IO ()
main = do
  hSetBuffering stdout LineBuffering
  hSetBuffering stderr LineBuffering
  register declareStatic
  opts_args <- getArgs
  (conf, _args) <- parseOpts opts_args
  runParIO_ conf $ checkDistGraphs >> checkGatheredNodes
