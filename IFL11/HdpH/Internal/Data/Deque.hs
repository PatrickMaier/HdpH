-- Doubly-ended queue (deque) with size and max size
--
-- Visibility: HdpH.Internal
-- Author: Patrick Maier <P.Maier@hw.ac.uk>
-- Created: 27 Sep 2011
--
-------------------------------------------------------------------------------

module HdpH.Internal.Data.Deque
  ( -- * functional deque
    Deque,        -- no instances
    empty,        -- :: Deque a
    fromList,     -- :: [a] -> Deque a
    pushFront,    -- :: Deque a -> a -> Deque a
    pushBack,     -- :: Deque a -> a -> Deque a
    popFront,     -- :: Deque a -> (Maybe a, Deque a)
    popBack,      -- :: Deque a -> (Maybe a, Deque a)
    first,        -- :: Deque a -> Maybe a
    last,         -- :: Deque a -> Maybe a
    null,         -- :: Deque a -> Bool
    length,       -- :: Deque a -> Int
    maxLength,    -- :: Deque a -> Int

    -- * stateful, concurrently accessible deque
    DequeIO,      -- no instances
    emptyIO,      -- :: IO (DequeIO a)
    fromListIO,   -- :: [a] -> IO (DequeIO a)
    pushFrontIO,  -- :: DequeIO a -> a -> IO ()
    pushBackIO,   -- :: DequeIO a -> a -> IO ()
    popFrontIO,   -- :: DequeIO a -> IO (Maybe a)
    popBackIO,    -- :: DequeIO a -> IO (Maybe a)
    firstIO,      -- :: DequeIO a -> IO (Maybe a)
    lastIO,       -- :: DequeIO a -> IO (Maybe a)
    nullIO,       -- :: DequeIO a -> IO Bool
    lengthIO,     -- :: DequeIO a -> IO Int
    maxLengthIO   -- :: DequeIO a -> IO Int
  ) where

import Prelude hiding (error, last, length, null)
import Data.Functor ((<$>))
import Data.IORef (IORef, newIORef, readIORef, atomicModifyIORef)
import qualified Data.List as List (length)
import Data.Sequence (Seq, (|>), (<|), ViewR((:>)), viewr, ViewL((:<)), viewl)
import qualified Data.Sequence as Seq (empty, null, length, fromList)


-----------------------------------------------------------------------------
-- functional deque with size and max size (with amortised O(1) operations)

data Deque a = Deque { q  :: Seq a,  -- sequence of elements
                       mx :: !Int }  -- maximal length of above sequence

empty :: Deque a
empty = Deque { q = Seq.empty, mx = 0 }

fromList :: [a] -> Deque a
fromList xs = Deque { q = Seq.fromList xs, mx = List.length xs }

pushFront :: Deque a -> a -> Deque a
pushFront dq x = dq { q = x <| q dq, mx = max (length dq + 1) (maxLength dq) }

pushBack :: Deque a -> a -> Deque a
pushBack dq x = dq { q = q dq |> x, mx = max (length dq + 1) (maxLength dq) }

popFront :: Deque a -> (Maybe a, Deque a)
popFront dq = case viewl (q dq) of
                hd :< rest -> (Just hd, dq { q = rest })
                _          -> (Nothing, dq)

popBack :: Deque a -> (Maybe a, Deque a)
popBack dq = case viewr (q dq) of
                rest :> tl -> (Just tl, dq { q = rest })
                _          -> (Nothing, dq)

first :: Deque a -> Maybe a
first dq = case viewl (q dq) of
             x :< _ -> Just x
             _      -> Nothing

last :: Deque a -> Maybe a
last dq = case viewr (q dq) of
            _ :> x -> Just x
            _      -> Nothing

null :: Deque a -> Bool
null = Seq.null . q

length :: Deque a -> Int
length = Seq.length . q

maxLength :: Deque a -> Int
maxLength = mx


-----------------------------------------------------------------------------
-- concurrently accessible deque (in the IO monad) with size and max size;
-- concurrent access is via a global lock on the deque.

newtype DequeIO a = DequeIO (IORef (Deque a))

emptyIO :: IO (DequeIO a)
emptyIO = DequeIO <$> newIORef empty

fromListIO :: [a] -> IO (DequeIO a)
fromListIO xs = DequeIO <$> newIORef (fromList xs)

pushFrontIO :: DequeIO a -> a -> IO ()
pushFrontIO (DequeIO dqRef) x =
  atomicModifyIORef dqRef $ \ dq -> (pushFront dq x, ())

pushBackIO :: DequeIO a -> a -> IO ()
pushBackIO (DequeIO dqRef) x =
  atomicModifyIORef dqRef $ \ dq -> (pushBack dq x, ())

popFrontIO :: DequeIO a -> IO (Maybe a)
popFrontIO (DequeIO dqRef) =
  atomicModifyIORef dqRef $ swap . popFront

popBackIO :: DequeIO a -> IO (Maybe a)
popBackIO (DequeIO dqRef) =
  atomicModifyIORef dqRef $ swap . popBack

firstIO :: DequeIO a -> IO (Maybe a)
firstIO (DequeIO dqRef) = first <$> readIORef dqRef

lastIO :: DequeIO a -> IO (Maybe a)
lastIO (DequeIO dqRef) = last <$> readIORef dqRef

nullIO :: DequeIO a -> IO Bool
nullIO (DequeIO dqRef) = null <$> readIORef dqRef

lengthIO :: DequeIO a -> IO Int
lengthIO (DequeIO dqRef) = length <$> readIORef dqRef

maxLengthIO :: DequeIO a -> IO Int
maxLengthIO (DequeIO dqRef) = maxLength <$> readIORef dqRef


-----------------------------------------------------------------------------
-- auxiliary functions

-- NOTE: Should be exported by Data.Tuple.
swap :: (a,b) -> (b,a)
swap (a,b) = (b,a)
