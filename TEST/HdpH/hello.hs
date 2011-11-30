-- Hello World in HdpH
--
-- Visibility: HdpH test suite
-- Author: Patrick Maier <P.Maier@hw.ac.uk>
-- Created: 07 Sep 2011
--
-----------------------------------------------------------------------------

module Main where

import Prelude
import Data.List (stripPrefix)
import Data.Monoid (mconcat)
import System.Environment (getArgs)
import System.IO (stdout, stderr, hSetBuffering, BufferMode(..))
import System.Mem (performGC)

import qualified MP.MPI_ByteString as MPI
import HdpH (RTSConf(..), defaultRTSConf,
             Par, runParIO_,
             myNode, allNodes, io, pushTo, new, get, glob, rput,
             NodeId,
             IVar, GIVar,
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


hello :: GIVar (Closure ()) -> Par ()
hello done = do here <- myNode
                io $ do putStrLn $ "Hello from " ++ show here
                rput done unitClosure

hello_Static :: Static (Env -> Par ())
hello_Static = staticAs
  (\ env -> let done = decodeEnv env
              in hello done)
  "Main.hello"


hello_world :: Par ()
hello_world = do
  world <- allNodes
  vs <- mapM spark_hello world
  io performGC  -- just testing whether global IVars survive a GC
  mapM_ get vs
    where                   
      spark_hello :: NodeId -> Par (IVar (Closure ()))
      spark_hello node = do v <- new
                            done <- glob v
                            let val = hello done
                            let env = encodeEnv done
                            let fun = hello_Static
                            pushTo (unsafeMkClosure val fun env) node
                            return v


-----------------------------------------------------------------------------
-- initialisation, argument processing and 'main'

-- parse runtime system config options (+ unused seed for rand num gen)
parseOpts :: [String] -> (RTSConf, Int, [String])
parseOpts args = go (defaultRTSConf, 0, args) where
  go :: (RTSConf, Int, [String]) -> (RTSConf, Int, [String])
  go (conf, seed, [])   = (conf, seed, [])
  go (conf, seed, s:ss) =
   case stripPrefix "-rand=" s of
   Just s  -> go (conf, read s, ss)
   Nothing ->
    case stripPrefix "-d" s of
    Just s  -> go (conf { debugLvl = read s }, seed, ss)
    Nothing ->
     case stripPrefix "-scheds=" s of
     Just s  -> go (conf { scheds = read s }, seed, ss)
     Nothing ->
      case stripPrefix "-wakeup=" s of
      Just s  -> go (conf { wakeupDly = read s }, seed, ss)
      Nothing ->
       case stripPrefix "-hops=" s of
       Just s  -> go (conf { maxHops = read s }, seed, ss)
       Nothing ->
        case stripPrefix "-maxFish=" s of
        Just s  -> go (conf { maxFish = read s }, seed, ss)
        Nothing ->
         case stripPrefix "-minSched=" s of
         Just s  -> go (conf { minSched = read s }, seed, ss)
         Nothing ->
          case stripPrefix "-minNoWork=" s of
          Just s  -> go (conf { minFishDly = read s }, seed, ss)
          Nothing ->
           case stripPrefix "-maxNoWork=" s of
           Just s  -> go (conf { maxFishDly = read s }, seed, ss)
           Nothing ->
            (conf, seed, s:ss)


main :: IO ()
main = do
  hSetBuffering stdout LineBuffering
  hSetBuffering stderr LineBuffering
  registerStatic
  MPI.defaultWithMPI $ do
    opts_args <- getArgs
    let (conf, _seed, _args) = parseOpts opts_args
    runParIO_ conf hello_world
