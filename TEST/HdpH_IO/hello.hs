-- Hello World in HdpH_IO
--
-- Visibility: HdpH test suite
-- Author: Patrick Maier <P.Maier@hw.ac.uk>
-- Created: 07 Sep 2011
--
-----------------------------------------------------------------------------

{-# LANGUAGE TemplateHaskell #-}

module Main where

import Prelude
import Data.Monoid (mconcat)
import System.IO (stdout, stderr, hSetBuffering, BufferMode(..))

import MP.MPI (defaultWithMPI)
import HdpH_IO (withHdpH_,
                NodeId, myNode, allNodes,
                pushTo,
                IVar, new, get,
                GIVar, glob, rput,
                Closure, mkClosure,
                toClosure, ToClosure(locToClosure),
                static, StaticToClosure, staticToClosure,
                StaticDecl, declare, register, here)
import qualified HdpH_IO (declareStatic)


-----------------------------------------------------------------------------
-- Static declaration

instance ToClosure () where locToClosure = $(here)

declareStatic :: StaticDecl
declareStatic = mconcat [HdpH_IO.declareStatic,
                         declare (staticToClosure :: StaticToClosure ()),
                         declare $(static 'hello_abs)]


-----------------------------------------------------------------------------
-- Hello World code

hello_world :: IO ()
hello_world = do
  world <- allNodes
  vs <- mapM push_hello world
  mapM_ get vs
    where
      push_hello :: NodeId -> IO (IVar (Closure ()))
      push_hello node = do
        v <- new
        done <- glob v
        pushTo $(mkClosure [| hello_abs done |]) node
        return v

hello_abs :: GIVar (Closure ()) -> IO ()
hello_abs done = do
  me <- myNode
  putStrLn $ "Hello from " ++ show me
  rput done $ toClosure ()


-----------------------------------------------------------------------------
-- minimal 'main'

main :: IO ()
main = do
  hSetBuffering stdout LineBuffering  -- output buffering
  hSetBuffering stderr LineBuffering
  register declareStatic              -- register 'Static' terms
  defaultWithMPI $ do                 -- run MPI
    withHdpH_ hello_world             -- and within that run HdpH
