-- Hello World in HdpH
--
-- Author: Patrick Maier
-----------------------------------------------------------------------------

{-# LANGUAGE TemplateHaskell #-}

{-# OPTIONS_GHC -fno-warn-orphans #-}

module Main where

import Prelude
import Control.Monad (replicateM, unless)
import Data.Monoid (mconcat)
import System.Environment (getArgs)
import System.IO (stdout, stderr, hSetBuffering, BufferMode(..))

import Control.Parallel.HdpH
       (RTSConf(..), defaultRTSConf, updateConf,
        Par, runParIO_,
        myNode, allNodes, io, spark, pushTo, new, get, glob, rput,
        Node, IVar, GIVar,
        Thunk(Thunk), Closure, mkClosure,
        toClosure, ToClosure(locToClosure),
        static, StaticToClosure, staticToClosure,
        StaticDecl, declare, register, here, initialiseHdpH)
import qualified Control.Parallel.HdpH as HdpH (declareStatic)
import Control.Parallel.HdpH.Dist (one)


-----------------------------------------------------------------------------
-- Hello World code

-- Send greetings from every node.
hello_world :: Par ()
hello_world = do
  master <- myNode
  io $ putStrLn $ "Master " ++ show master ++ " wants to know: Who is here?"
  world <- allNodes
  vs <- mapM push_hello_world world
  mapM_ get vs
    where
      push_hello_world :: Node -> Par (IVar (Closure ()))
      push_hello_world node = do
        v <- new
        done <- glob v
        pushTo $(mkClosure [| hello_world_abs done |]) node
        return v

hello_world_abs :: GIVar (Closure ()) -> Thunk (Par ())
hello_world_abs done = Thunk $ do
  me <- myNode
  io $ putStrLn $ "Hello from " ++ show me
  rput done $ toClosure ()


-- Every stolen task sends send greetings its executing node.
-- Serves simply to demonstrate randomness of work stealing.
hello_thief :: Int -> Par ()
hello_thief n_tasks = do
  master <- myNode
  io $ putStrLn $ "Master " ++ show master ++ " wants to know: Who steals?"
  let spark_hello_thief :: Par (IVar (Closure ()))
      spark_hello_thief = do
        v <- new
        done <- glob v
        spark one $(mkClosure [| hello_thief_abs (master, done) |])
        return v
  vs <- replicateM n_tasks spark_hello_thief
  mapM_ get vs

hello_thief_abs :: (Node, GIVar (Closure ())) -> Thunk (Par ())
hello_thief_abs (master, done) = Thunk $ do
  me <- myNode
  unless (me == master) $
    io $ putStrLn $ "Hello from " ++ show me
  rput done $ toClosure ()


-----------------------------------------------------------------------------
-- Static declaration (just before 'main')

-- Empty splice; TH hack to make all environment abstractions visible.
$(return [])

-- orphan ToClosure instance (unavoidably so)
instance ToClosure () where locToClosure = $(here)

declareStatic :: StaticDecl
declareStatic = mconcat [HdpH.declareStatic,
                         declare (staticToClosure :: StaticToClosure ()),
                         declare $(static 'hello_world_abs),
                         declare $(static 'hello_thief_abs)]


-----------------------------------------------------------------------------
-- initialisation, argument processing and 'main'

-- parse runtime system config options; abort if there is an error
parseOpts :: [String] -> IO (RTSConf, [String])
parseOpts args = do
  either_conf <- updateConf args defaultRTSConf
  case either_conf of
    Left err_msg                 -> error $ "parseOpts: " ++ err_msg
    Right (conf, remaining_args) -> return (conf, remaining_args)

-- parse arguments; either nothing or number of tasks to be stolen.
parseArgs :: [String] -> Int
parseArgs []     = defTasks
parseArgs (s1:_) = read s1

defTasks :: Int
defTasks = 0

main :: IO ()
main = do
  hSetBuffering stdout LineBuffering
  hSetBuffering stderr LineBuffering
  register declareStatic
  opts_args <- getArgs
  (conf, args) <- parseOpts opts_args
  initialiseHdpH conf
  let n_tasks = parseArgs args
  if n_tasks <= 0
    then runParIO_ conf $ hello_world
    else runParIO_ conf $ hello_thief n_tasks
