-- Hello World in HdpH
--
-- Author: Patrick Maier
-----------------------------------------------------------------------------

{-# LANGUAGE TemplateHaskell #-}

module Main where

import Prelude
import Data.List (stripPrefix)
import Data.Monoid (mconcat)
import System.Environment (getArgs)
import System.IO (stdout, stderr, hSetBuffering, BufferMode(..))
import System.Mem (performGC)

import Control.Parallel.HdpH
       (RTSConf(..), defaultRTSConf,
        Par, runParIO_,
        myNode, allNodes, io, pushTo, new, get, glob, rput,
        NodeId, IVar, GIVar,
        Closure, mkClosure,
        toClosure, ToClosure(locToClosure),
        static, StaticToClosure, staticToClosure,
        StaticDecl, declare, register, here)
import qualified Control.Parallel.HdpH as HdpH (declareStatic)


-----------------------------------------------------------------------------
-- Static declaration

instance ToClosure () where locToClosure = $(here)

declareStatic :: StaticDecl
declareStatic = mconcat [HdpH.declareStatic,
                         declare (staticToClosure :: StaticToClosure ()),
                         declare $(static 'hello_abs)]


-----------------------------------------------------------------------------
-- Hello World code

hello_world :: Par ()
hello_world = do
  master <- myNode
  io $ putStrLn $ "Master " ++ show master ++ " wants to know: Who is here?"
  world <- allNodes
  vs <- mapM push_hello world
  mapM_ get vs
    where
      push_hello :: NodeId -> Par (IVar (Closure ()))
      push_hello node = do
        v <- new
        done <- glob v
        pushTo $(mkClosure [| hello_abs done |]) node
        return v

hello_abs :: GIVar (Closure ()) -> Par ()
hello_abs done = do
  here <- myNode
  io $ putStrLn $ "Hello from " ++ show here
  rput done $ toClosure ()


-----------------------------------------------------------------------------
-- initialisation, argument processing and 'main'

-- parse runtime system config options
parseOpts :: [String] -> (RTSConf, [String])
parseOpts args = go (defaultRTSConf, args) where
  go :: (RTSConf, [String]) -> (RTSConf, [String])
  go (conf, [])   = (conf, [])
  go (conf, s:ss) =
   case stripPrefix "-d" s of
   Just s  -> go (conf { debugLvl = read s }, ss)
   Nothing ->
    case stripPrefix "-scheds=" s of
    Just s  -> go (conf { scheds = read s }, ss)
    Nothing ->
     case stripPrefix "-wakeup=" s of
     Just s  -> go (conf { wakeupDly = read s }, ss)
     Nothing ->
      case stripPrefix "-hops=" s of
      Just s  -> go (conf { maxHops = read s }, ss)
      Nothing ->
       case stripPrefix "-maxFish=" s of
       Just s  -> go (conf { maxFish = read s }, ss)
       Nothing ->
        case stripPrefix "-minSched=" s of
        Just s  -> go (conf { minSched = read s }, ss)
        Nothing ->
         case stripPrefix "-minNoWork=" s of
         Just s  -> go (conf { minFishDly = read s }, ss)
         Nothing ->
          case stripPrefix "-maxNoWork=" s of
          Just s  -> go (conf { maxFishDly = read s }, ss)
          Nothing ->
           case stripPrefix "-numProcs=" s of
           Just s  -> go (conf { numProcs = read s }, ss)
           Nothing ->
            (conf, s:ss)


main :: IO ()
main = do
  hSetBuffering stdout LineBuffering
  hSetBuffering stderr LineBuffering
  register declareStatic
  opts_args <- getArgs
  let (conf, _args) = parseOpts opts_args
  runParIO_ conf hello_world
