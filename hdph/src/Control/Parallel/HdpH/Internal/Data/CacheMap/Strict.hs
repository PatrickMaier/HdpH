-- Fixed-size cache memoising stateful entities in IO, random replacement
--
-- Author: Patrick Maier
-------------------------------------------------------------------------------

{-# LANGUAGE MultiParamTypeClasses #-}

module Control.Parallel.HdpH.Internal.Data.CacheMap.Strict
  ( -- * class of key/value type suitable for cache mapping
    CacheMappable (
      create
    , destroy
    )

    -- * concurrently accessible cache map
  , CacheMap  -- no instances
  , empty
  , getEntries
  , getSize
  , resize
  , flush
  , (!)
  ) where

import Prelude
import Control.Monad (when)
import Data.Functor ((<$>))
import Data.IORef (IORef, newIORef, readIORef, atomicModifyIORef')
import Data.Hashable (Hashable)
import Data.HashMap.Strict (HashMap)
import qualified Data.HashMap.Strict as HashMap
       (empty, size, lookup, insert, delete, toList)
import System.Random (randomRIO)


-- | Key/value pairs that can be stored in a CacheMap
class (Eq k, Hashable k) => CacheMappable k v where
  -- | Create a value from the key (upon cache miss).
  create  :: k -> IO v

  -- | Finalize a key/value pair (upon cache eviction).
  destroy :: k -> v -> IO ()


-- internal type; pairing a HashMap with the max number of entries.
data CM k v = CM !Int !(HashMap k v)


-- | A CacheMap is a mutable object mapping keys to values.
--   Only a fixed number of key/value pairs can be stored in a CacheMap
--   unless its size is negative (in which case it is an unbounded cache).
newtype CacheMap k v = CacheMap (IORef (CM k v))


-- | Creates a new CacheMap with space for 0 entries.
--   Use 'resize' to increase the number of entries.
empty :: IO (CacheMap k v)
empty = CacheMap <$> newIORef (CM 0 HashMap.empty)


-- | Returns the current number of cache entries.
getEntries :: CacheMap k v -> IO Int
getEntries  (CacheMap cmRef) = do
  CM _ hm <- readIORef cmRef
  return $! HashMap.size hm


-- | Returns the current cache size.
getSize :: CacheMap k v -> IO Int
getSize (CacheMap cmRef) = do
  CM cacheSize _ <- readIORef cmRef
  return cacheSize


-- | Adjusts maximal number of entries in CacheMap.
--   Flushes CacheMap if it contains more entries than allowed after adjustment;
--   'resize 0' serves as a finalizer.
--   Calling 'resize' with any negative number yields an unbounded CacheMap.
resize :: (CacheMappable k v) => Int -> CacheMap k v -> IO ()
resize newSize (CacheMap cmRef) = do
  maybe_flushed <- atomicModifyIORef' cmRef $ \ (CM cacheSize hm) ->
                     if newSize >= 0 && HashMap.size hm > newSize
                       then (CM newSize HashMap.empty, Just hm)
                       else (CM newSize hm,            Nothing)
  case maybe_flushed of
    Nothing      -> return ()
    Just flushed -> mapM_ (uncurry destroy) $ HashMap.toList flushed


-- | Flushes the CacheMap, destroying all key/value pairs.
flush :: (CacheMappable k v) => CacheMap k v -> IO ()
flush (CacheMap cmRef) = do
  flushed <- atomicModifyIORef' cmRef $ \ (CM cacheSize hm) ->
               (CM cacheSize HashMap.empty, hm)
  mapM_ (uncurry destroy) $ HashMap.toList flushed


-- | CacheMap lookup, with automatic insertion of missing entries
--   and random eviction if there are too many entries.
(!) :: (CacheMappable k v) => CacheMap k v -> k -> IO v
CacheMap cmRef ! key = do
  CM cacheSize hm <- readIORef cmRef
  case HashMap.lookup key hm of
    Just val -> return val  -- fast path: cache hit
    Nothing  -> do          -- slow path: cache miss
      -- create value to be cached
      val <- create key
      -- skip cache update if cache size is set to 0.
      when (cacheSize /= 0) $ do
        -- get random eviction slot (unless cacheSize is negative)
        rnd <- if cacheSize < 0
                 then return (-1)
                 else randomRIO (0, cacheSize - 1)
        -- update cache
        evicted <- atomicModifyIORef' cmRef $ evictAndInsert rnd key val
        -- finalize evicted key/value pair (if there is one)
        case evicted of
          Nothing     -> return ()
          Just (k, v) -> destroy k v
      -- return value
      return val


-- Insert a key/value pair, evicting the rnd-th pair if the map is full;
-- assumes cacheSize < 0 && rnd < 0 or 0 <= rnd < cacheSize.
evictAndInsert :: (Eq k, Hashable k)
               => Int -> k -> v -> CM k v -> (CM k v, Maybe (k, v))
evictAndInsert rnd key val (CM cacheSize hm) =
  if cacheSize < 0 || HashMap.size hm < cacheSize
    then (CM cacheSize $ hm',  Nothing)
    else (CM cacheSize $ hm'', Just (k,v))
      where
        hm'   = HashMap.insert key val hm
        hm''  = HashMap.insert key val $ HashMap.delete k hm
        (k,v) = HashMap.toList hm !! rnd
