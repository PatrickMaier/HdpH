module Main where

import Prelude
import System.IO (stdout, stderr, hSetBuffering, BufferMode(..))

import qualified MP.MPI as MPI


main :: IO ()
main = do
  -- Don't need to change buffering (for this example, at least)
  -- hSetBuffering stdout LineBuffering
  -- hSetBuffering stderr LineBuffering
  MPI.withMPI MPI.defaultBufferSize mainMPI


mainMPI :: IO ()
mainMPI = do
  procs <- MPI.procs
  myRank <- MPI.myRank
  putStrLn ("Hello world from " ++ show myRank ++ " out of " ++ show procs)
