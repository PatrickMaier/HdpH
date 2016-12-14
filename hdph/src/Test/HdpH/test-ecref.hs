-- Testing eventually coherent references (ECRefs)
--
-- Author: Patrick Maier
-----------------------------------------------------------------------------

{-# LANGUAGE TemplateHaskell #-}

{-# OPTIONS_GHC -fno-warn-orphans #-}

module Main where

import Prelude
import Control.Exception (evaluate)
import Control.Monad (replicateM, unless)
import Data.IORef
       (IORef, newIORef, readIORef, atomicWriteIORef, atomicModifyIORef')
import Data.List (delete)
import Data.Monoid (mconcat)
import Data.Time.Clock (NominalDiffTime, diffUTCTime, getCurrentTime)
import System.Environment (getArgs)
import System.IO (stdout, stderr, hSetBuffering, BufferMode(..))
import System.IO.Unsafe (unsafePerformIO)

import Control.Parallel.HdpH
       (RTSConf(..), defaultRTSConf, updateConf,
        Par, runParIO_,
        myNode, allNodes, io, fork, new, put, get,
        Thunk(Thunk), Closure, mkClosure, unitC,
        toClosure, ToClosure(locToClosure),
        static, StaticToClosure, staticToClosure,
        StaticDecl, declare, register, here)
import qualified Control.Parallel.HdpH as HdpH (declareStatic)
import Control.Parallel.HdpH.Strategies (parMapM, pushMapM, rcall)
import qualified Control.Parallel.HdpH.Strategies as Strategies (declareStatic)

import Aux.ECRef
       (ECRefDict(ECRefDict), joinWith,
        ECRef, newECRef, freeECRef, readECRef, writeECRef, gatherECRef)
import qualified Aux.ECRef as ECRef (toClosure, declareStatic)


-----------------------------------------------------------------------------
-- Finding the max in the Collatz sequence

f_collatz :: Integer -> Integer
f_collatz n | n <= 1    = 1
            | even n    = n `div` 2
            | otherwise = 3 * n + 1

collatz :: Integer -> [Integer]
collatz n0 = takeWhile (> 1) (iterate f_collatz n0) ++ [1]

max_collatz :: Integer -> Integer
max_collatz = maximum . collatz


push_max_collatz :: Integer -> Par ()
push_max_collatz n0 = do
  let xs = collatz n0
  all <- allNodes
  bound <- newECRef dictC all 0
  _ <- pushMapM all $(mkClosure [| push_max_collatz_abs bound |]) xs
  x_max <- readECRef bound
  x_max' <- gatherECRef bound
  freeECRef bound
  io $ putStrLn $ "push_max_collatz " ++ show n0 ++ " = " ++ show (x_max,x_max')
  io $ putStrLn $ "     max_collatz " ++ show n0 ++ " = " ++ show (maximum xs)

push_max_collatz_abs :: ECRef Integer -> Thunk (Integer -> Par (Closure ()))
push_max_collatz_abs bound = Thunk $ \ n -> do
  _ <- io $ evaluate $ fib (n `mod` 13 + 13)
  writeECRef bound n
  return unitC

-- some Fibonacci computation to slow down task evaluation
fib :: Integer -> Integer
fib n | n <= 1    = 1
      | otherwise = fib (n - 1) + fib (n - 2)

par_max_collatz :: Integer -> Par ()
par_max_collatz n0 = do
  let xs = collatz n0
  all <- allNodes
  bound <- newECRef dictC all 0
  _ <- parMapM $(mkClosure [| push_max_collatz_abs bound |]) xs
  x_max <- readECRef bound
  x_max' <- gatherECRef bound
  freeECRef bound
  io $ putStrLn $ " par_max_collatz " ++ show n0 ++ " = " ++ show (x_max,x_max')
  io $ putStrLn $ "     max_collatz " ++ show n0 ++ " = " ++ show (maximum xs)


-----------------------------------------------------------------------------
-- hammering an ECRef to measure throughput

push_counter :: Integer -> Par ()
push_counter n0 = do
  me <- myNode
  all <- allNodes
  let nodes = if n0 > 0 then all else delete me all
  ((count, limit), t) <- timePar $ do
    bound <- newECRef dictC all 0
    let task = $(mkClosure [| push_counter_abs (bound, n0) |])
    _ <- rcall task nodes
    count <- readECRef bound
    limit <- gatherECRef bound
    freeECRef bound
    return (count, limit)
  io $ putStrLn $ "push_counter " ++ show n0 ++ " = " ++ show (count, limit) ++
                  " {" ++ show t ++ "}"

push_counter_abs :: (ECRef Integer, Integer) -> Thunk (Par (Closure ()))
push_counter_abs (bound, n0) = Thunk $ do
  wait <- new
  fork $ do
    mapM_ (writeECRef bound) [1 .. abs n0]
    put wait unitC
  get wait


-----------------------------------------------------------------------------
-- boring dictionaries, Closures, etc

dict :: ECRefDict Integer
dict = ECRefDict { ECRef.toClosure = toClosureInteger,
                   joinWith = \ x y -> if y <= x then Nothing else Just y }

dictC :: Closure (ECRefDict Integer)
dictC = $(mkClosure [| dict |])

toClosureInteger :: Integer -> Closure Integer
toClosureInteger n = $(mkClosure [| toClosureInteger_abs n |])

toClosureInteger_abs :: Integer -> Thunk Integer
toClosureInteger_abs n = Thunk n

-- time a Par action
timePar :: Par a -> Par (a, NominalDiffTime)
timePar action = do t0 <- io $ getCurrentTime
                    x <- action
                    t1 <- io $ getCurrentTime
                    return (x, diffUTCTime t1 t0)


-----------------------------------------------------------------------------
-- Static declaration (just before 'main')

-- Empty splice; TH hack to make all environment abstractions visible.
$(return [])

-- orphan ToClosure instances (unavoidably so)
instance ToClosure Integer where locToClosure = $(here)

declareStatic :: StaticDecl
declareStatic = mconcat [HdpH.declareStatic,
                         Strategies.declareStatic,
                         ECRef.declareStatic,
                         declare (staticToClosure :: StaticToClosure Integer),
                         declare $(static 'push_max_collatz_abs),
                         declare $(static 'push_counter_abs),
                         declare $(static 'dict),
                         declare $(static 'toClosureInteger_abs)]


-----------------------------------------------------------------------------
-- initialisation, argument processing and 'main'

-- parse runtime system config options; abort if there is an error
parseOpts :: [String] -> IO (RTSConf, [String])
parseOpts args = do
  either_conf <- updateConf args defaultRTSConf
  case either_conf of
    Left err_msg                 -> error $ "parseOpts: " ++ err_msg
    Right (conf, remaining_args) -> return (conf, remaining_args)

-- parse arguments; expects 1 integer n0 (default: n0 = -1234567890)
-- * n >= 10^9:  write terms of Collatz sequence starting at n0 to ECRef using
--               round-robin scheduling (one task per term of sequence)
-- * n <= -10^9: write terms of Collatz sequence starting at -n0 to ECRef using
--               work stealing (one task per term of sequence)
-- * 0 < n0 < 10^9:  one task per node counting from 1 to n0,
--                   continuously writing count to ECRef
-- * 0 > n0 > -10^9: one task per node (excluding root) counting from 1 to n0,
--                   continuously writing count to ECRef
-- * n0 == 0: do nothing
parseArgs :: [String] -> Integer
parseArgs []     = defN0
parseArgs (s1:_) = read s1

defN0 :: Integer
defN0 = -1234567890

main :: IO ()
main = do
  hSetBuffering stdout LineBuffering
  hSetBuffering stderr LineBuffering
  register declareStatic
  opts_args <- getArgs
  (conf, args) <- parseOpts opts_args
  let n0 = parseArgs args
  let action | n0 == 0     = return ()
             | n0 >= 10^9  = push_max_collatz n0
             | n0 <= -10^9 = par_max_collatz (- n0)
             | otherwise   = push_counter n0
  runParIO_ conf action
