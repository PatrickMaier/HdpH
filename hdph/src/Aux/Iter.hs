-- Generic iterators
--
-- 12-15 Feb 2016
--
-- Patrick Maier <C.Patrick.Maier@gmail.com>
--
-- NOTE: Test code commented out
--

{-# LANGUAGE BangPatterns #-}

module Aux.Iter
  ( -- * generic iterators
    Iter
  , newIter
  , stateIter
  , doneIter
  , nextIter
  , runIter
  , skipUntil
  , buffer

    -- * generic monadic iterators (in a monad above IO)
  , IterM
  , newIterM
  , copyIterM
  , stateIterM
  , doneIterM
  , nextIterM
  , runIterM
  , skipUntilM
  , bufferM
  ) where

import Prelude
import Control.Monad.IO.Class (MonadIO(liftIO))
import Data.IORef (IORef, newIORef, readIORef, writeIORef)
-- TESTING ONLY
-- import System.IO.Unsafe (unsafePerformIO)


-------------------------------------------------------------------------------
-- Generic iterators

data Iter s a = Iter { nx :: s -> Maybe (s, a),  -- static
                       st :: Maybe s }           -- dynamic

instance (Show s) => Show (Iter s a) where
  show iter = "Iter {st = " ++ show (st iter) ++ "}"

-- Return iterator's internal state
stateIter :: Iter s a -> Maybe s
stateIter = st

newIter :: (s -> Maybe (s, a)) -> s -> Iter s a
newIter next init = Iter {nx = next, st = Just init}

doneIter :: Iter s a -> Bool
doneIter iter = case st iter of { Nothing -> True; _ -> False }

nextIter :: Iter s a -> (Iter s a, Maybe a)
nextIter iter =
  case st iter of
    Nothing -> (iter, Nothing)  -- iter already done
    Just s  -> case nx iter s of
                 Nothing       -> (iter {st = Nothing}, Nothing)  -- iter done
                 Just (!s', x) -> (iter {st = Just s'}, Just x)

runIter :: Iter s a -> [a]
runIter iter =
  case nextIter iter of
    (_,     Nothing) -> []
    (iter', Just x)  -> x : runIter iter'

skipUntil :: Iter s a -> (a -> Bool) -> Iter s a
skipUntil iter p = iter {nx = loop}
  where
 -- loop :: s -> Maybe (s, a)
    loop s =
      case nx iter s of
        Nothing      -> Nothing
        Just (s', x) -> if p x then Just (s', x) else loop s'

infixl 0 `skipUntil`


-------------------------------------------------------------------------------
-- Generic buffered iterators

type Q a = [a]

sizeQ :: Q a -> Int
sizeQ = length

mtQ :: Q a
mtQ = []

enQ :: Q a -> a -> Q a
enQ q x = q ++ [x]

deQ :: Q a -> Maybe (a, Q a)
deQ []    = Nothing
deQ (x:q) = Just (x, q)

buffer :: Iter s a -> Int -> Iter (Iter s a, Q a) a
buffer iter bufsize
  | bufsize < 1 = error "buffer: bufsize < 1"
  | otherwise   = newIter roll $ refill (iter, mtQ)
    where
      roll :: (Iter s a, Q a) -> Maybe ((Iter s a, Q a), a)
      roll (iter, q) =
        case deQ q of
          Nothing      -> Nothing
          Just (x, q') -> Just (refill (iter, q'), x)
      refill :: (Iter s a, Q a) -> (Iter s a, Q a)
      refill (iter, q) =
        if sizeQ q >= bufsize
          then (iter, q)                         -- buffer full
          else case nextIter iter of
                 (iter', Nothing) -> (iter', q)  -- iter done
                 (iter', Just x)  -> refill (iter', enQ q x)

infixl 0 `buffer`

{-
-- test iterators
it1 = newIter (\n -> if n < 10 then Just (n+1, n) else Nothing) 0
it2 = it1 `skipUntil` even
it3 = it1 `buffer` 3
it4 = it1 `buffer` 5 `skipUntil` odd
-}


-------------------------------------------------------------------------------
-- Generic monadic iterators (in a monad above IO)
--
-- NOTE: These iterators are neither thread nor exception safe.
--       That is, actions that modify state (`nextIterM` and `runIterM`)
--       must be called from a single thread, and any actions used to
--       create iterators (ie. arguments to `newIterM` or `skipUntilM`)
--       must not raise exceptions. (Asynchronous exceptions are just
--       treated as bad luck.)

data IterM m s a = IterM { nxM :: s -> m (Maybe (s, a)),  -- static
                           stM :: IORef (Maybe s) }       -- dynamic

-- Return iterator's internal state
stateIterM :: (MonadIO m) => IterM m s a -> m (Maybe s)
{-# INLINABLE stateIterM #-}
stateIterM = liftIO . readIORef . stM

-- NOTE: Not exception safe;
--   returned iterator may crash if `next` ever throws an exception.
newIterM :: (MonadIO m) => (s -> m (Maybe (s, a))) -> s -> m (IterM m s a)
{-# INLINABLE newIterM #-}
newIterM next init = do
  ref <- liftIO $ newIORef $ Just init
  return $ IterM {nxM = next, stM = ref}

-- Return a deep copy of iterator `iter` (ie. with distinct state).
copyIterM :: (MonadIO m) => IterM m s a -> m (IterM m s a)
{-# INLINABLE copyIterM #-}
copyIterM iter = do
  ref <- liftIO . newIORef =<< stateIterM iter
  return $ iter {stM = ref}

doneIterM :: (MonadIO m) => IterM m s a -> m Bool
{-# INLINABLE doneIterM #-}
doneIterM iter = do
  maybe_s <- liftIO $ readIORef $ stM iter
  case maybe_s of { Nothing -> return True; _ -> return False }

-- NOTE: Not thread safe;
--   `iter` may crash if multiple threads call this function or `runIterM`.
nextIterM :: (MonadIO m) => IterM m s a -> m (Maybe a)
{-# INLINABLE nextIterM #-}
nextIterM iter = do
  let ref = stM iter
  maybe_s <- liftIO $ readIORef ref
  case maybe_s of
    Nothing -> do { liftIO $ writeIORef ref Nothing; return Nothing }
    Just s  -> do
      maybe_next <- nxM iter s
      case maybe_next of
        Nothing       -> do liftIO $ writeIORef ref Nothing
                            return Nothing
        Just (!s', x) -> do liftIO $ writeIORef ref (Just s')
                            return (Just x)

-- NOTE: Not thread safe;
--   `iter` may crash if multiple threads call this function or `nextIterM`.
runIterM :: (MonadIO m) => IterM m s a -> m [a]
{-# INLINABLE runIterM #-}
runIterM iter = buildList
  where
 -- buildList :: m [a]
    buildList = do
      maybe_x <- nextIterM iter
      case maybe_x of
        Nothing -> return []
        Just x  -> (x:) <$> buildList

-- NOTE #1: Returned iterator shares state with `iter`.
-- NOTE #2: Not exception safe;
--   returned iterator may crash if `p` ever throws an exception.
skipUntilM :: (MonadIO m) => (a -> m Bool) -> IterM m s a -> m (IterM m s a)
{-# INLINABLE skipUntilM #-}
skipUntilM p iter = return $ iter {nxM = loop}
  where
 -- loop :: s -> m (Maybe (s, a))
    loop s = do
      maybe_next <- nxM iter s
      case maybe_next of
        Nothing      -> return Nothing
        Just (s', x) -> do
          done <- p x
          if done then return $ Just (s', x) else loop s'


-------------------------------------------------------------------------------
-- Generic monadic buffered iterators (in a monad above IO)

bufferM :: (MonadIO m) => Int -> IterM m s a -> m (IterM m (Q a) a)
{-# INLINABLE bufferM #-}
bufferM bufsize iter
  | bufsize < 1 = error "bufferM: bufsize < 1"
  | otherwise   = do { q0 <- refill mtQ; newIterM roll q0 }
    where
   -- roll :: Q a -> m (Maybe (Q a, a))
      roll q =
        case deQ q of
          Nothing      -> return Nothing
          Just (x, q') -> do { q'' <- refill q'; return $ Just (q'', x) }
   -- refill :: Q a -> m (Q a)
      refill q =
        if sizeQ q >= bufsize
          then return q                    -- buffer full
          else do
            maybe_x <- nextIterM iter
            case maybe_x of
              Nothing -> return q          -- iter done
              Just x  -> refill $ enQ q x  -- tail recurse (until buffer full)

{-
-- monadic test iterators
mit1  = unsafePerformIO $
          newIterM (\n -> return $ if n < 10 then Just (n+1, n) else Nothing) 0
mit2a = unsafePerformIO $
          skipUntilM (return . even) mit1
mit2b = unsafePerformIO $
          copyIterM mit1 >>= skipUntilM (return . odd)
mit3  = unsafePerformIO $
          bufferM 5 mit1
mit4  = unsafePerformIO $
          copyIterM mit2b >>= bufferM 3 >>= skipUntilM (return . (> 4))
-}
