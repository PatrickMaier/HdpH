module Main where

import Prelude
import Control.Monad (when)
import Data.Serialize (encodeLazy, decodeLazy)
import Data.Char (toUpper)
import qualified MP.MPI_ByteString as MPI


main :: IO ()
main = do
  MPI.withMPI 9 mainMPI


mainMPI :: IO ()
mainMPI = do
  procs <- MPI.procs
  myRank <- MPI.myRank
  ranks <- MPI.allRanks
  let sendRank = head ranks
  let recvRank = last ranks
  let msg = encodeLazy ("Hello world, this is a long long string." :: String)

  when (myRank == sendRank) $
    do putStrLn $ "Sender " ++ show myRank
       MPI.send recvRank msg

  when (myRank == recvRank) $
    do putStrLn $ "Receiver " ++ show myRank
       hdl <- MPI.recv
       putStrLn $ "From = " ++ show (MPI.sender hdl)
       msg <- MPI.getMsg hdl
       putStrLn $ "Msg = " ++ show msg
       let val :: String
           val = fromRight $ decodeLazy msg
       putStrLn $ "Value = " ++ show val
       MPI.send sendRank (encodeLazy (map toUpper val))

  when (myRank == sendRank) $
    do hdl <- MPI.recv
       msg <- MPI.getMsg hdl
       putStrLn $ fromRight $ decodeLazy msg

  return ()


fromRight :: Either a b -> b
fromRight (Right y) = y
