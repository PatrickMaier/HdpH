-- Par monad and thread representation; types
--
-- Author: Patrick Maier
-----------------------------------------------------------------------------

module Control.Parallel.HdpH.Internal.Type.Par
  ( -- * Par monad, threads and sparks
    ParM(..),
    Thread(..),
    ThreadCont(..),
    Spark       -- synonym: Spark m = Closure (ParM m ())
  ) where

import Prelude
import Control.Applicative (Applicative, pure, (<*>))
import Control.Monad (ap)

import Control.Parallel.HdpH.Closure (Closure)


-----------------------------------------------------------------------------
-- Par monad, based on ideas from
--   [1] Claessen "A Poor Man's Concurrency Monad", JFP 9(3), 1999.
--   [2] Marlow et al. "A monad for deterministic parallelism". Haskell 2011.

-- 'ParM m' is a continuation monad, specialised to the return type 'Thread m';
-- 'm' abstracts a monad encapsulating the underlying state.
newtype ParM m a = Par { unPar :: (a -> Thread m) -> Thread m }

instance Functor (ParM m) where
    fmap f p = Par $ \ c -> unPar p (c . f)

-- The Monad instance is where we differ from Control.Monad.Cont,
-- the difference being the use of strict application ($!).
instance Monad (ParM m) where
    return a = Par $ \ c -> c $! a
    p >>= k  = Par $ \ c -> unPar p $ \ a -> unPar (k $! a) c

instance Applicative (ParM m) where
    pure  = return
    (<*>) = ap


-- A thread is a monadic action returning a ThreadCont (telling the scheduler
-- how to continue after executing the monadic action).
-- Note that [2] uses different model, a "Trace" GADT reifying the monadic
-- actions, which are then interpreted by the scheduler.
newtype Thread m = Atom (Bool -> m (ThreadCont m))

-- A ThreadCont either tells the scheduler to continue (constructor ThreadCont)
-- or to terminate the current thread (constructor ThreadDone).
-- In either case, the ThreadCont additionally provides a (possibly empty) list
-- of high priority threads, to be executed before any low priority threads.
data ThreadCont m = ThreadCont ![Thread m] (Thread m)
                  | ThreadDone ![Thread m]


-- A spark is a 'Par' comp returning '()', wrapped into an explicit closure.
type Spark m = Closure (ParM m ())
