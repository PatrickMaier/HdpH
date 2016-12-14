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
import Data.Monoid (mconcat)
import System.Environment (getArgs)
import System.IO (stdout, stderr, hSetBuffering, BufferMode(..))
import System.IO.Unsafe (unsafePerformIO)

import Control.Parallel.HdpH
       (RTSConf(..), defaultRTSConf, updateConf,
        Par, runParIO_,
        allNodes, io, pushTo,
        Thunk(Thunk), Closure, mkClosure,
        toClosure, ToClosure(locToClosure),
        static, StaticToClosure, staticToClosure,
        StaticDecl, declare, register, here)
import qualified Control.Parallel.HdpH as HdpH (declareStatic)
import Control.Parallel.HdpH.Strategies (parMapM, pushMapM)
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
-- boring dictionaries and Closures

dict :: ECRefDict Integer
dict = ECRefDict { ECRef.toClosure = toClosureInteger,
                   joinWith = \ x y -> if y <= x then Nothing else Just y }

dictC :: Closure (ECRefDict Integer)
dictC = $(mkClosure [| dict |])

toClosureInteger :: Integer -> Closure Integer
toClosureInteger n = $(mkClosure [| toClosureInteger_abs n |])

toClosureInteger_abs :: Integer -> Thunk Integer
toClosureInteger_abs n = Thunk n

unit :: ()
unit = ()

unitC :: Closure ()
unitC = $(mkClosure [| unit |])


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
                         declare $(static 'dict),
                         declare $(static 'toClosureInteger_abs),
                         declare $(static 'unit)]


-----------------------------------------------------------------------------
-- initialisation, argument processing and 'main'

-- parse runtime system config options; abort if there is an error
parseOpts :: [String] -> IO (RTSConf, [String])
parseOpts args = do
  either_conf <- updateConf args defaultRTSConf
  case either_conf of
    Left err_msg                 -> error $ "parseOpts: " ++ err_msg
    Right (conf, remaining_args) -> return (conf, remaining_args)

-- parse arguments; either nothing or start of Collatz sequence
parseArgs :: [String] -> Integer
parseArgs []     = defN0
parseArgs (s1:_) = read s1

defN0 :: Integer
defN0 = 999

main :: IO ()
main = do
  hSetBuffering stdout LineBuffering
  hSetBuffering stderr LineBuffering
  register declareStatic
  opts_args <- getArgs
  (conf, args) <- parseOpts opts_args
  let n0 = parseArgs args
  if n0 < 0
    then runParIO_ conf $ par_max_collatz (- n0)
    else runParIO_ conf $ push_max_collatz n0
