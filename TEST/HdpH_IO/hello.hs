-- Hello World in HdpH_IO
--
-- Visibility: HdpH test suite
-- Author: Patrick Maier <P.Maier@hw.ac.uk>
-- Created: 07 Sep 2011
--
-----------------------------------------------------------------------------

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
                Env, encodeEnv, decodeEnv,
                Closure, toClosure, unsafeMkClosure,
                DecodeStatic, decodeStatic,
                Static, staticAs, declare, register)


-----------------------------------------------------------------------------
-- 'Static' declaration and registration

instance DecodeStatic ()

registerStatic :: IO ()
registerStatic =
  register $ mconcat
    [declare hello_Static,
     declare (decodeStatic :: Static (Env -> ()))]


-----------------------------------------------------------------------------
-- Hello World code

unitClosure :: Closure ()
unitClosure = toClosure ()


hello :: GIVar (Closure ()) -> IO ()
hello done = do me <- myNode
                putStrLn $ "Hello from " ++ show me
                rput done unitClosure

hello_Static :: Static (Env -> IO ())
hello_Static = staticAs
  (\ env -> let done = decodeEnv env
              in hello done)
  "Main.hello"


hello_world :: IO ()
hello_world = do
  world <- allNodes
  vs <- mapM spark_hello world
  mapM_ get vs
    where
      spark_hello :: NodeId -> IO (IVar (Closure ()))
      spark_hello node = do v <- new
                            done <- glob v
                            let val = hello done
                            let env = encodeEnv done
                            let fun = hello_Static
                            pushTo (unsafeMkClosure val fun env) node
                            return v


-----------------------------------------------------------------------------
-- minimal 'main'

main :: IO ()
main = do
  hSetBuffering stdout LineBuffering  -- output buffering
  hSetBuffering stderr LineBuffering
  registerStatic                      -- register 'Static' terms
  defaultWithMPI $ do                 -- run MPI
    withHdpH_ hello_world             -- and within that run HdpH
