-- Global references
--
-- Author: Patrick Maier
-----------------------------------------------------------------------------

{-# LANGUAGE StandaloneDeriving #-}

module Control.Parallel.HdpH.Internal.GRef
  ( -- * global references
    GRef,       -- instances: Eq, Ord, Show, NFData, Serialize
    at,         -- :: GRef a -> NodeId

    -- * predicates on global references
    isLocal,    -- :: GRef a -> IO Bool
    isLive,     -- :: GRef a -> IO Bool

    -- * updating the registry
    globalise,  -- :: a -> IO (GRef a)
    free,       -- :: GRef a -> IO ()
    freeNow,    -- :: GRef a -> IO ()

    -- * dereferencing a global reference
    withGRef    -- :: GRef a -> (a -> IO b) -> IO b -> IO b
  ) where

import Prelude hiding (error)
import Control.Concurrent (forkIO)
import Control.DeepSeq (NFData(rnf))
import Control.Monad (unless)
import Data.Functor ((<$>))
import Data.IORef (readIORef, atomicModifyIORef)
import qualified Data.Map as Map (insert, delete, member, lookup)
import Data.Serialize (Serialize)
import qualified Data.Serialize (put, get)
import Unsafe.Coerce (unsafeCoerce)

import Control.Parallel.HdpH.Internal.Location
       (NodeId, myNode, error, debug, dbgGRef)
import Control.Parallel.HdpH.Internal.Misc (AnyType(Any))
import Control.Parallel.HdpH.Internal.Type.GRef
       (GRef(GRef), at, slot, GRefReg, lastSlot, table)
import Control.Parallel.HdpH.Internal.State.GRef (regRef)


-----------------------------------------------------------------------------
-- Key facts about global references
--
-- * A global reference is a globally unique handle naming a Haskell value;
--   the type of the value is reflected in a phantom type argument to the
--   type of global reference, similar to the type of stable names.
--
-- * The link between a global reference and the value it names is established
--   by a registry mapping references to values. The registry mapping a
--   global reference resides on the node hosting its value. All operations
--   involving the reference must be executed on the hosting node; the only
--   exception is the function 'at', projecting a global reference to its
--   hosting node.
--
-- * The life time of a global reference is not linked to the life time of 
--   the named value, and vice versa. One consequence is that global
--   references can never be re-used, unlike stable names.
--
-- * For now, global references must be freed explicitly (from the map
--   on the hosting node). This could (and should) be changed by using
--   weak pointers and finalizers.


-----------------------------------------------------------------------------
-- global references (abstract outwith this module)
-- NOTE: Global references are hyperstrict.

-- Constructs a 'GRef' value of a given node ID and slot (on the given node);
-- ensures the resulting 'GRef' value is hyperstrict;
-- this constructor is not to be exported.
mkGRef :: NodeId -> Integer -> GRef a
mkGRef node i = rnf node `seq` rnf i `seq` GRef { slot = i, at = node }

instance Eq (GRef a) where
  ref1 == ref2 = slot ref1 == slot ref2 && at ref1 == at ref2

instance Ord (GRef a) where
  compare ref1 ref2 = case compare (slot ref1) (slot ref2) of
                        LT -> LT
                        GT -> GT
                        EQ -> compare (at ref1) (at ref2)


-- Show instance (mainly for debugging)
instance Show (GRef a) where
  showsPrec _ ref =
    showString "GRef:" . shows (at ref) . showString "." . shows (slot ref)

instance NFData (GRef a)  -- default instance suffices (due to hyperstrictness)

instance Serialize (GRef a) where
  put ref = Data.Serialize.put (at ref) >>
            Data.Serialize.put (slot ref)
  get = do node <- Data.Serialize.get
           i <- Data.Serialize.get
           return $ mkGRef node i  -- 'mkGRef' ensures result is hyperstrict


-----------------------------------------------------------------------------
-- predicates on global references

-- Monadic projection; True iff the current node hosts the object refered
-- to by the given global 'ref'.
isLocal :: GRef a -> IO Bool
isLocal ref = (at ref ==) <$> myNode


-- Checks if a locally hosted global 'ref' is live.
-- Aborts with an error if 'ref' is a not hosted locally.
isLive :: GRef a -> IO Bool
isLive ref = do
  refIsLocal <- isLocal ref
  unless refIsLocal $
    error $ "HdpH.Internal.GRef.isLive: " ++ show ref ++ " not local"
  reg <- readIORef regRef
  return $ Map.member (slot ref) (table reg)


-----------------------------------------------------------------------------
-- updating the registry

-- Registers its argument as a global object (hosted on the current node),
-- returning a fresh global reference. May block when attempting to access
-- the registry.
globalise :: a -> IO (GRef a)
globalise x = do
  node <- myNode
  ref <- atomicModifyIORef regRef (createEntry (Any x) node)
  debug dbgGRef $ "GRef.globalise " ++ show ref
  return ref


-- Asynchronously frees a locally hosted global 'ref'; no-op if 'ref' is dead.
-- Aborts with an error if 'ref' is a not hosted locally.
free :: GRef a -> IO ()
free ref = do
  refIsLocal <- isLocal ref
  unless refIsLocal $
    error $ "HdpH.Internal.GRef.free: " ++ show ref ++ " not local"
  forkIO $ do debug dbgGRef $ "GRef.free " ++ show ref
              atomicModifyIORef regRef (deleteEntry $ slot ref)
  return ()


-- Frees a locally hosted global 'ref'; no-op if 'ref' is dead.
-- Aborts with an error if 'ref' is a not hosted locally.
freeNow :: GRef a -> IO ()
freeNow ref = do
  refIsLocal <- isLocal ref
  unless refIsLocal $
    error $ "HdpH.Internal.GRef.freeNow: " ++ show ref ++ " not local"
  debug dbgGRef $ "GRef.freeNow " ++ show ref
  atomicModifyIORef regRef (deleteEntry $ slot ref)


-- Create new entry in 'reg' (hosted on 'node') mapping to 'val'; not exported
createEntry :: AnyType -> NodeId -> GRefReg -> (GRefReg, GRef a)
createEntry val node reg =
  ref `seq` (reg', ref) where
    newSlot = lastSlot reg + 1
    ref = mkGRef node newSlot  -- 'seq' above forces hyperstrict 'ref' to NF
    reg' = reg { lastSlot = newSlot,
                 table    = Map.insert newSlot val (table reg) }


-- Delete entry 'slot' from 'reg'; not exported
deleteEntry :: Integer -> GRefReg -> (GRefReg, ())
deleteEntry slot reg =
  (reg { table = Map.delete slot (table reg) }, ())


-----------------------------------------------------------------------------
-- Dereferencing global refs

-- Attempts to dereference a locally hosted global 'ref' and apply 'action'
-- to the refered-to object; executes 'dead' if that is not possible (ie.
-- 'dead' acts as an exception handler) because the global 'ref' is dead.
-- Aborts with an error if 'ref' is a not hosted locally.
withGRef :: GRef a -> (a -> IO b) -> IO b -> IO b
withGRef ref action dead = do
  refIsLocal <- isLocal ref
  unless refIsLocal $
    error $ "HdpH.Internal.GRef.withGRef: " ++ show ref ++ " not local"
  reg <- readIORef regRef
  case Map.lookup (slot ref) (table reg) of
    Nothing      -> do debug dbgGRef $ "GRef.withGRef " ++ show ref ++ " dead"
                       dead
    Just (Any x) -> do action (unsafeCoerce x)
                    -- see below for an argument why unsafeCoerce is safe here


-------------------------------------------------------------------------------
-- Notes on the design of the registry
--
-- * A global reference is represented as a pair consisting of the ID
--   of the hosting node together with its 'slot' in the registry on
--   that node. The slot is an unbounded integer so that there is an
--   infinite supply of slots. (Slots can't be re-used as there is no
--   global garbage collection of global references.)
--
-- * The registry maps slots to values, which are essentially untyped
--   (the type information being swallowed by an existential wrapper).
--   However, the value type information is not lost as it can be
--   recovered from the phantom type argument of its global reference.
--   In fact, the function 'withGRef' super-imposes a reference's phantom
--   type on to its value via 'unsafeCoerce'. The reasons why this is safe
--   are laid out below.


-- Why 'unsafeCoerce' in safe in 'withGRef':
--
-- * Global references can only be created by the function 'globalise'.
--   Whenever this function generates a global reference 'ref' of type
--   'GRef t' it guarantees that 'ref' is globally fresh, ie. its
--   representation does not exist any where else in the system, nor has
--   it ever existed in the past. (Note that freshness relies on the
--   assumption that node IDs themselves are fresh, which is relevant
--   in case nodes can leave and join dynmically.)
--
-- * A consequence of global freshness is that there is a functional relation
--   from representations to phantom types of global references. For all
--   global references 'ref1 :: GRef t1' and 'ref2 :: GRef t2',
--   'at ref1 == at ref2 && slot ref1 == slot ref2' implies the identity
--   of the phantom types t1 and t2.
--
-- * Thus, we can safely super-impose (using 'unsafeCoerce') the phantom type
--   of a global reference on to its value.
