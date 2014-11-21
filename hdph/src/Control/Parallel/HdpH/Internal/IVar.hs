-- Local and global IVars
--
-- Author: Patrick Maier
-----------------------------------------------------------------------------

module Control.Parallel.HdpH.Internal.IVar
  ( -- * local IVar type
    IVar,       -- synonym: IVar m a = IORef <IVarContent m a>

    -- * operations on local IVars
    newIVar,    -- :: IO (IVar m a)
    putIVar,    -- :: IVar m a -> a -> IO ([Thread m], [Thread m])
    getIVar,    -- :: Bool -> IVar m a -> (a -> Thread m) -> IO (Maybe a)
    pollIVar,   -- :: IVar m a -> IO (Maybe a)
    probeIVar,  -- :: IVar m a -> IO Bool

    -- * global IVar type
    GIVar,      -- synonym: GIVar m a = GRef (IVar m a)

    -- * operations on global IVars
    globIVar,   -- :: Int -> IVar m a -> IO (GIVar m a)
    hostGIVar,  -- :: GIVar m a -> Node
    putGIVar    -- :: Int -> GIVar m a -> a -> IO ([Thread m], [Thread m])
  ) where

import Prelude hiding (error)
import Data.Functor ((<$>))
import Data.IORef (IORef, newIORef, readIORef, atomicModifyIORef)
import Data.Maybe (isJust)

import Control.Parallel.HdpH.Internal.Location 
       (Node, debug, dbgGIVar, dbgIVar)
import Control.Parallel.HdpH.Internal.GRef
       (GRef, at, globalise, freeNow, withGRef)
import Control.Parallel.HdpH.Internal.Type.Par (Thread)


-----------------------------------------------------------------------------
-- type of local IVars

-- An IVar is a mutable reference to either a value or two lists (high
-- and low priority) of continuations blocked waiting for a value;
-- the parameter 'm' abstracts a monad (cf. module HdpH.Internal.Type.Par).
type IVar m a = IORef (IVarContent m a)

data IVarContent m a = Full a
                     | Blocked [a -> Thread m] [a -> Thread m]


-----------------------------------------------------------------------------
-- operations on local IVars, borrowing from
--    [1] Marlow et al. "A monad for deterministic parallelism". Haskell 2011.

-- Create a new, empty IVar.
newIVar :: IO (IVar m a)
newIVar = newIORef (Blocked [] [])


-- Write 'x' to the IVar 'v' and return two lists (high and low priority)
-- of blocked threads.
-- Unlike [1], multiple writes fail silently (ie. they do not change
-- the value stored, and return empty lists of threads).
putIVar :: IVar m a -> a -> IO ([Thread m], [Thread m])
putIVar v x = do
  e <- readIORef v
  case e of
    Full _      -> do debug dbgIVar $ "Put to full IVar"
                      return ([],[])
    Blocked _ _ -> do maybe_ts <- atomicModifyIORef v fill_and_unblock
                      case maybe_ts of
                        Nothing        -> do debug dbgIVar $
                                               "Put to full IVar (race)"
                                             return ([],[])
                        Just (hts,lts) -> do debug dbgIVar $
                                               "Put to empty IVar; " ++
                                               "unblocking " ++
                                               show (length hts + length lts) ++
                                               " threads"
                                             return (hts,lts)
      where
     -- fill_and_unblock :: IVarContent m a ->
     --                       (IVarContent m a, Maybe ([Thread m], [Thread m]))
        fill_and_unblock st =
          case st of
            Full _          -> (st,     Nothing)
            Blocked hcs lcs -> (Full x, Just (map ($ x) hcs, map ($ x) lcs))


-- Read from the given IVar 'v' and return the value if it is full.
-- Otherwise return Nothing but add the given continuation 'c' to the list of
-- blocked continuations (to the high priority ones iff 'hi' is True).
getIVar :: Bool -> IVar m a -> (a -> Thread m) -> IO (Maybe a)
getIVar hi v c = do
  e <- readIORef v
  case e of
    Full x      -> do return (Just x)
    Blocked _ _ -> do maybe_x <- atomicModifyIORef v get_or_block
                      case maybe_x of
                        Just _  -> do return maybe_x
                        Nothing -> do debug dbgIVar $ "Blocking on IVar"
                                      return maybe_x
      where
     -- get_or_block :: IVarContent m a -> (IVarContent m a, Maybe a)
        get_or_block st =
          case st of
            Full x                      -> (st,                   Just x)
            Blocked hcs lcs | hi        -> (Blocked (c:hcs) lcs, Nothing)
                            | otherwise -> (Blocked hcs (c:lcs), Nothing)


-- Poll the given IVar 'v' and return its value if full, Nothing otherwise.
-- Does not block.
pollIVar :: IVar m a -> IO (Maybe a)
pollIVar v = do
  e <- readIORef v
  case e of
    Full x      -> return (Just x)
    Blocked _ _ -> return Nothing


-- Probe whether the given IVar is full, returning True if it is.
-- Does not block.
probeIVar :: IVar m a -> IO Bool
probeIVar v = isJust <$> pollIVar v


-----------------------------------------------------------------------------
-- type of global IVars; instances mostly inherited from global references

-- A global IVar is a global reference to an IVar; 'm' abstracts a monad.
-- NOTE: The HdpH interface will restrict the type parameter 'a' to 
--       'Closure b' for some type 'b', but but the type constructor 'GIVar' 
--       does not enforce this restriction.
type GIVar m a = GRef (IVar m a)


-----------------------------------------------------------------------------
-- operations on global IVars

-- Returns node hosting given global IVar.
hostGIVar :: GIVar m a -> Node
hostGIVar = at


-- Globalise the given IVar;
-- the scheduler ID argument may be used for logging.
globIVar :: Int -> IVar m a -> IO (GIVar m a)
globIVar _schedID v = do
  gv <- globalise v
  debug dbgGIVar $ "New global IVar " ++ show gv
  return gv


-- Write 'x' to the locally hosted global IVar 'gv', free 'gv' and return 
-- two lists of blocked threads. Like putIVar, multiple writes fail silently
-- (as do writes to a dead global IVar);
-- the scheduler ID argument may be used for logging.
putGIVar :: Int -> GIVar m a -> a -> IO ([Thread m], [Thread m])
putGIVar _schedID gv x = do
  debug dbgGIVar $ "Put to global IVar " ++ show gv
  ts <- withGRef gv (\ v -> putIVar v x) (return ([],[]))
  freeNow gv  -- free 'gv' immediately; could use 'free' instead of 'freeNow'
  return ts
