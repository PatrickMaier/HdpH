-- Par monad and thread representation; types
--
-- Author: Patrick Maier
-----------------------------------------------------------------------------

module Control.Parallel.HdpH.Internal.Type.Par
  ( -- * Par monad, threads and sparks
    ParM(..),
    Thread(..),
    Spark       -- synonym: Spark m = Closure (ParM m ())
  ) where

import Prelude

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


-- A thread is determined by its actions, as described in this data type.
-- In [2] this type is called 'Trace'.
newtype Thread m = Atom (m (Maybe (Thread m)))  -- atomic action (in monad 'm')
                                                -- result is next action, maybe


-- A spark is a 'Par' comp returning '()', wrapped into an explicit closure.
type Spark m = Closure (ParM m ())
