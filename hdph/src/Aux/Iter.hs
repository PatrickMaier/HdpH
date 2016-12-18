-- Generic iterators
--
-- 12-15 Feb 2016
--
-- Patrick Maier <C.Patrick.Maier@gmail.com>
--
-- NOTE: Test code commented out
--

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

    -- * generic iterators in the IO monad
  , IterIO
  , newIterIO
  , copyIterIO
  , stateIterIO
  , doneIterIO
  , nextIterIO
  , runIterIO
  , skipUntilIO
  , bufferIO
  ) where

import Prelude
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
                 Nothing      -> (iter {st = Nothing}, Nothing)  -- iter done
                 Just (s', x) -> (iter {st = Just s'}, Just x)

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
-- Generic iterators in the IO monad
--
-- NOTE: These iterators are neither thread nor exception safe.
--       That is, actions that modify state (`nextIterIO` and `runIterIO`)
--       must be called from a single thread, and any actions used to
--       create iterators (ie. arguments to `newIterIO` or `skipUntilIO`)
--       must not raise exceptions. (Asynchronous exceptions are just
--       treated as bad luck.)

data IterIO s a = IterIO { nxIO :: s -> IO (Maybe (s, a)),  -- static
                           stIO :: IORef (Maybe s) }        -- dynamic

-- Return iterator's internal state
stateIterIO :: IterIO s a -> IO (Maybe s)
stateIterIO = readIORef . stIO

-- NOTE: Not exception safe;
--   returned iterator may crash if `next` ever throws an exception.
newIterIO :: (s -> IO (Maybe (s, a))) -> s -> IO (IterIO s a)
newIterIO next init = do
  ref <- newIORef $ Just init
  return $ IterIO {nxIO = next, stIO = ref}

-- Return a deep copy of iterator `iter` (ie. with distinct state).
copyIterIO :: IterIO s a -> IO (IterIO s a)
copyIterIO iter = do
  ref <- newIORef =<< stateIterIO iter
  return $ iter {stIO = ref}

doneIterIO :: IterIO s a -> IO Bool
doneIterIO iter = do
  maybe_s <- readIORef $ stIO iter
  case maybe_s of { Nothing -> return True; _ -> return False }

-- NOTE: Not thread safe;
--   `iter` may crash if multiple threads call this function or `runIterIO`.
nextIterIO :: IterIO s a -> IO (Maybe a)
nextIterIO iter = do
  let ref = stIO iter
  maybe_s <- readIORef ref
  case maybe_s of
    Nothing -> do { writeIORef ref $ Nothing; return $ Nothing }
    Just s  -> do
      maybe_next <- nxIO iter s
      case maybe_next of
        Nothing      -> do { writeIORef ref $ Nothing; return $ Nothing }
        Just (s', x) -> do { writeIORef ref $ Just s'; return $ Just x }

-- NOTE: Not thread safe;
--   `iter` may crash if multiple threads call this function or `nextIterIO`.
runIterIO :: IterIO s a -> IO [a]
runIterIO iter = buildList
  where
    action = nextIterIO iter
 -- buildList :: IO [a]
    buildList = do
      maybe_x <- action
      case maybe_x of
        Nothing -> return []
        Just x  -> (x:) <$> buildList

-- NOTE #1: Returned iterator shares state with `iter`.
-- NOTE #2: Not exception safe;
--   returned iterator may crash if `p` ever throws an exception.
skipUntilIO :: (a -> IO Bool) -> IterIO s a -> IO (IterIO s a)
skipUntilIO p iter = return $ iter {nxIO = loop}
  where
 -- loop :: s -> IO (Maybe (s, a))
    loop s = do
      maybe_next <- nxIO iter s
      case maybe_next of
        Nothing      -> return Nothing
        Just (s', x) -> do
          done <- p x
          if done then return $ Just (s', x) else loop s'


-------------------------------------------------------------------------------
-- Generic buffered iterators in the IO monad

bufferIO :: Int -> IterIO s a -> IO (IterIO (Q a) a)
bufferIO bufsize iter
  | bufsize < 1 = error "bufferIO: bufsize < 1"
  | otherwise   = do { q0 <- refill mtQ; newIterIO roll q0 }
    where
   -- roll :: Q a -> IO (Maybe (Q a, a))
      roll q =
        case deQ q of
          Nothing      -> return Nothing
          Just (x, q') -> do { q'' <- refill q'; return $ Just (q'', x) }
   -- refill :: Q a -> IO (Q a)
      refill q =
        if sizeQ q >= bufsize
          then return q                    -- buffer full
          else do
            maybe_x <- nextIterIO iter
            case maybe_x of
              Nothing -> return q          -- iter done
              Just x  -> refill $ enQ q x  -- tail recurse (until buffer full)

{-
-- monadic test iterators
mit1  = unsafePerformIO $
          newIterIO (\n -> return $ if n < 10 then Just (n+1, n) else Nothing) 0
mit2a = unsafePerformIO $
          skipUntilIO (return . even) mit1
mit2b = unsafePerformIO $
          copyIterIO mit1 >>= skipUntilIO (return . odd)
mit3  = unsafePerformIO $
          bufferIO 5 mit1
mit4  = unsafePerformIO $
          copyIterIO mit2b >>= bufferIO 3 >>= skipUntilIO (return . (> 4))
-}
