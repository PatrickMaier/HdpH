-- Simple racey semaphores without quantity and without starvation prevention
--
-- Author: Patrick Maier
-------------------------------------------------------------------------------

module Control.Parallel.HdpH.Internal.Data.Sem 
  ( -- * semaphore type
    Sem,     -- no instances

    -- * basic operations
    new,     -- :: IO Sem    
    wait,    -- :: Sem -> IO ()
    signal,  -- :: Sem -> IO ()

    -- * convenience
    signalPeriodically  -- :: Sem -> Int -> IO ()
  ) where

import Prelude hiding (error)
import Control.Concurrent (threadDelay)
import Control.Concurrent.MVar (MVar, newEmptyMVar, takeMVar, putMVar)
import Control.Monad (join)
import Data.Functor ((<$>))
import Data.IORef (IORef, newIORef, atomicModifyIORef)


-------------------------------------------------------------------------------
-- A simple semaphore just maintains a list of threads being blocked on it,
-- and provides operations for waiting and signalling (blocking and wakeing
-- up one thread per operation, respectively).

newtype Sem = Sem (IORef [MVar ()])


-- Creates a new semaphore.
new :: IO Sem
new = Sem <$> newIORef []


-- Blocks calling thread, waiting for a 'signal' on the given semaphore.
-- Note that there can be races between 'wait' and 'signal' in that 'signal'
-- can find no blocked threads (and thus do nothing) just before another
-- thread is about to 'wait'. This effect is due to the semaphore not
-- actually guarding a quantity.
wait :: Sem -> IO ()
wait (Sem sem) = do
  b <- newEmptyMVar
  atomicModifyIORef sem $ \ blocked -> (b:blocked, ())
  takeMVar b


-- Wakes up one blocked thread (actually, the last thread to block) if there
-- are any blocked threads.
signal :: Sem -> IO ()
signal (Sem sem) =
  join $
    atomicModifyIORef sem $ \ blocked ->
      case blocked of
        []   -> ([], return ())
        b:bs -> (bs, putMVar b ())


-- Nonterminating action periodically, every 'interval' microseconds,
-- signalling the given semaphore. This is one way to deal with the races
-- that may arise between 'wait' and 'signal'.
signalPeriodically :: Sem -> Int -> IO ()
signalPeriodically sem interval = do
  threadDelay interval
  signal sem
  signalPeriodically sem interval
