-- Hello World in HdpH
--
-- Author: Patrick Maier
-----------------------------------------------------------------------------

{-# LANGUAGE TemplateHaskell #-}

{-# OPTIONS_GHC -fno-warn-orphans #-}

module Main where

import Prelude
import Data.Monoid (mconcat)
import System.Environment (getArgs)
import System.IO (stdout, stderr, hSetBuffering, BufferMode(..))

import Control.Parallel.HdpH
       (RTSConf(..), defaultRTSConf, updateConf,
        Par, runParIO_,
        myNode, allNodes, io, pushTo, new, get, glob, rput,
        Node, IVar, GIVar,
        Thunk(Thunk), Closure, mkClosure,
        toClosure, ToClosure(locToClosure),
        static, StaticToClosure, staticToClosure,
        StaticDecl, declare, register, here)
import qualified Control.Parallel.HdpH as HdpH (declareStatic)


-----------------------------------------------------------------------------
-- Static declaration

-- orphan ToClosure instance (unavoidably so)
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
      push_hello :: Node -> Par (IVar (Closure ()))
      push_hello node = do
        v <- new
        done <- glob v
        pushTo $(mkClosure [| hello_abs done |]) node
        return v

hello_abs :: GIVar (Closure ()) -> Thunk (Par ())
hello_abs done = Thunk $ do
  me <- myNode
  io $ putStrLn $ "Hello from " ++ show me
  rput done $ toClosure ()


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
  runParIO_ conf hello_world
