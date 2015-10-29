-- |
--  Priority Based WorkQueue Implementation
--  Author: Blair Archibald 2015
--  Supports only a single enqueue and dequeue operation unlike the previous
--  Deque Implementation. This is required to ensure priorities are enforced.

module Control.Parallel.HdpH.Internal.Data.PriorityWorkQueue
    (
      Priority
    , WorkQueueIO
    , enqueueTaskIO
    , dequeueTaskIO
    , emptyIO
    , sizeIO
    ) where

import           Data.IORef           (IORef, atomicModifyIORef, newIORef,
                                       readIORef)
import           Data.PQueue.Prio.Min (MinPQueue, empty, insert, minViewWithKey,
                                       size)

type Priority = Integer
type WorkQueue a = MinPQueue Priority a

enqueueTask :: WorkQueue a -> Priority -> a -> WorkQueue a
enqueueTask q p x = insert p x q

dequeueTask :: WorkQueue a -> Maybe ((Priority, a), WorkQueue a)
dequeueTask = minViewWithKey

---------------------------------------------------------------------
-- concurrently accessible version of the WorkQueue (in the IO monad)

newtype WorkQueueIO a = WorkQueueIO (IORef (WorkQueue a))

enqueueTaskIO :: WorkQueueIO a -> Priority -> a -> IO ()
enqueueTaskIO (WorkQueueIO qRef) p x =
  atomicModifyIORef qRef $ \q -> (enqueueTask q p x, ())

dequeueTaskIO :: WorkQueueIO a -> IO (Maybe (Priority, a))
dequeueTaskIO (WorkQueueIO qRef) =
  atomicModifyIORef qRef $ \q -> case dequeueTask q of
                                   Just (a,q') -> (q', Just a)
                                   Nothing     -> (q , Nothing)

emptyIO :: IO (WorkQueueIO a)
emptyIO = WorkQueueIO <$> newIORef empty

sizeIO :: WorkQueueIO a -> IO Int
sizeIO (WorkQueueIO qref) = size <$> readIORef qref
