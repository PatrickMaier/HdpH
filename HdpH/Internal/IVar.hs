-- Local and global IVars
--
-- Visibility: HpdH, HdpH.Internal
-- Author: Patrick Maier <P.Maier@hw.ac.uk>
-- Created: 15 Jun 2011
--
-----------------------------------------------------------------------------

module HdpH.Internal.IVar
  ( -- * local IVar type
    IVar,       -- synonym: IVar m a = IORef <IVarContent m a>

    -- * operations on local IVars
    newIVar,    -- :: IO (IVar m a)
    putIVar,    -- :: IVar m a -> a -> IO [Thread m]
    getIVar,    -- :: IVar m a -> (a -> Thread m) -> IO (Maybe a)
    pollIVar,   -- :: IVar m a -> IO (Maybe a)
    probeIVar,  -- :: IVar m a -> IO Bool

    -- * global IVar type
    GIVar,      -- synonym: GIVar m a = GRef (IVar m a)

    -- * operations on global IVars
    globIVar,   -- :: IVar m a -> IO (GIVar m a)
    hostGIVar,  -- :: GIVar m a -> NodeId
    putGIVar    -- :: GIVar m a -> a -> IO [Thread m]
  ) where

import Prelude hiding (error)
import Data.Functor ((<$>))
import Data.IORef (IORef, newIORef, readIORef, atomicModifyIORef)
import Data.Maybe (isJust)

import HdpH.Internal.Location (NodeId, debug, dbgGIVar, dbgIVar)
import HdpH.Internal.GRef (GRef, at, globalise, free, withGRef)
import HdpH.Internal.Type.Par (Thread)


-----------------------------------------------------------------------------
-- type of local IVars

-- An IVar is a mutable reference to either a value or a list of blocked
-- continuations (waiting for a value);
-- the parameter 'm' abstracts a monad (cf. module HdpH.Internal.Type.Par).
type IVar m a = IORef (IVarContent m a)

data IVarContent m a = Full a
                     | Blocked [a -> Thread m]


-----------------------------------------------------------------------------
-- operations on local IVars, borrowing from
--    [1] Marlow et al. "A monad for deterministic parallelism". Haskell 2011.

-- Create a new, empty IVar.
newIVar :: IO (IVar m a)
newIVar = newIORef (Blocked [])


-- Write 'x' to the IVar 'v' and return the list of blocked threads.
-- Unlike [1], multiple writes fail silently (ie. they do not change
-- the value stored, and return an empty list of threads).
putIVar :: IVar m a -> a -> IO [Thread m]
putIVar v x = do
  e <- readIORef v
  case e of
    Full _    -> do debug dbgIVar $ "Put to full IVar"
                    return []
    Blocked _ -> do maybe_ts <- atomicModifyIORef v fill_and_unblock
                    case maybe_ts of
                      Nothing -> do debug dbgIVar $ "Put to full IVar"
                                    return []
                      Just ts -> do debug dbgIVar $
                                      "Put to empty IVar; unblocking " ++
                                      show (length ts) ++ " threads"
                                    return ts
      where
     -- fill_and_unblock :: IVarContent m a ->
     --                       (IVarContent m a, Maybe [Thread m])
        fill_and_unblock e =
          case e of
            Full _     -> (e,       Nothing)
            Blocked cs -> (Full x,  Just $ map ($ x) cs)


-- Read from the given IVar 'v' and return the value if it is full.
-- Otherwise add the given continuation 'c' to the list of blocked
-- continuations and return nothing.
getIVar :: IVar m a -> (a -> Thread m) -> IO (Maybe a)
getIVar v c = do
  e <- readIORef v
  case e of
    Full x    -> do return (Just x)
    Blocked _ -> do maybe_x <- atomicModifyIORef v get_or_block
                    case maybe_x of
                      Just _  -> do return maybe_x
                      Nothing -> do debug dbgIVar $ "Blocking on IVar"
                                    return maybe_x
      where
     -- get_or_block :: IVarContent m a -> (IVarContent m a, Maybe a)
        get_or_block e =
          case e of
            Full x     -> (e,              Just x)
            Blocked cs -> (Blocked (c:cs), Nothing)


-- Poll the given IVar 'v' and return its value if full, Nothing otherwise.
-- Does not block.
pollIVar :: IVar m a -> IO (Maybe a)
pollIVar v = do
  e <- readIORef v
  case e of
    Full x    -> return (Just x)
    Blocked _ -> return Nothing


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
hostGIVar :: GIVar m a -> NodeId
hostGIVar = at


-- Globalise the given IVar.
globIVar :: IVar m a -> IO (GIVar m a)
globIVar v = do gv <- globalise v
                debug dbgGIVar $ "New global IVar " ++ show gv
                return gv


-- Write 'x' to the locally hosted global IVar 'gv', free 'gv' and return 
-- the list of blocked threads. Like putIVar, multiple writes fail silently
-- (as do writes to a dead global IVar).
putGIVar :: GIVar m a -> a -> IO [Thread m]
putGIVar gv x = do debug dbgGIVar $ "Put to global IVar " ++ show gv
                   ts <- withGRef gv (\ v -> putIVar v x) (return [])
                   free gv    -- free 'gv' (eventually)
                   return ts
