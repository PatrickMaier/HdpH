-- Eventually Coherent References
--
-- Author: Patrick Maier
--
-- This module might be integrated into the HdpH library, alongside Strategies.
-----------------------------------------------------------------------------

{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE GADTs #-}           -- for existential types
{-# LANGUAGE TemplateHaskell #-}

module Aux.ECRef
  ( -- * eventually coherent references
    ECRef,            -- * -> *; instances: Eq, Ord, Show, NFData, Serialize

    -- * dictionary of operations necessary to support ECRefs
    ECRefDict(      -- * -> *; no instances
      ECRefDict,    -- :: (a -> Closure a) -> (a -> a -> Maybe a) -> ECRefDict a
      toClosure,    -- :: ECRefDict a -> a -> Closure a
      joinWith),    -- :: ECRefDict a -> a -> a -> Maybe a

    -- * local accesors (non-blocking)
    creatorECRef,     -- :: ECRef a -> Node
    scopeECRef,       -- :: ECRef a -> Par [Node]
    readECRef,        -- :: ECRef a -> Par (Maybe a)

    -- * non-local write (non-blocking)
    writeECRef,       -- :: ECRef a -> a -> Par ()

    -- * creation and destruction (blocking)
    newECRef,         -- :: Closure (ECRefDict a)-> [Node] -> a -> Par (ECRef a)
    freeECRef,        -- :: ECRef a -> Par ()

    -- * non-local read (blocking)
    gatherECRef,      -- :: ECRef a -> Par (Maybe a)

    -- * this module's Static declaration
    declareStatic     -- :: StaticDecl
  ) where

import Prelude
import Control.DeepSeq (NFData(rnf))
import Control.Monad (zipWithM)
import Data.IORef (IORef, newIORef, readIORef, atomicModifyIORef')
import Data.List (delete, foldl1')
import Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map (empty, insert, delete, lookup)
import Data.Maybe (isJust)
import Data.Monoid (mconcat)
import Data.Serialize (Serialize)
import qualified Data.Serialize (put, get)
import System.IO.Unsafe (unsafePerformIO)
import Unsafe.Coerce (unsafeCoerce)

import Control.Parallel.HdpH
       (Par, Node,
        myNode, io,
        Thunk(Thunk), Closure, mkClosure, unClosure, unitC,
        StaticDecl, static, declare)
import qualified Control.Parallel.HdpH as HdpH (declareStatic)
import Control.Parallel.HdpH.Strategies (rcall, rcall_)
import qualified Control.Parallel.HdpH.Strategies as Strategies (declareStatic)


-----------------------------------------------------------------------------
-- Key facts about eventually coherent references
--
-- Eventually coherent references (ECRefs) are mutable variables similar
-- to IORefs, except for the following differences.
-- * ECRefs are distributed:
--   Each ECRef has a specific /spatial scope/, i.e. set of nodes where it
--   is accessible. The spatial scope is fixed upon an ECRef's creation.
-- * ECRefs act like coherent caches:
--   Reads and writes are non-blocking and go to a copy of the ECRef's value
--   stored on the current node. Writes are automatically propagated to all
--   nodes in scope. There is no guarantee when writes will be propagated
--   (though it is pretty quick), but there is an ordering guarantee, namely
--   that writes initiated by the same HdpH thread will be propagated in
--   the order they were initiated.
-- * ECRefs can accumulate values monotonically similar to LVars:
--   Writes join the new value with the old value, and only update and
--   propagate if the join is strictly greater than the old value.
--   Like in the case of LVars this feature helps to recover deterministic
--   behaviour in the face of non-deterministic distributed updates. (Can
--   we prove this?) The actual join (and the ordering it implies) is a
--   parameter fixed upon an ECRef's creation.
-- * ECRefs must be freed explicitly:
--   This is a wart; it could probably be fixed but would require distributed
--   bookkeeping of where the ECRef is still alive. Freeing isn't a big issue
--   as ECRefs are meant to be used inside skeletons with a well-defined
--   lifetime. However, the fact that ECRefs can be freed explicitly has
--   implications for the type of the read operation; it returns an option
--   type because the ECRef could have been freed already.



-----------------------------------------------------------------------------
-- ECRef dictionary

-- | Dictionary of operations necessary for ECRefs
data ECRefDict a = ECRefDict { toClosure :: a -> Closure a,
                               joinWith  :: a -> a -> Maybe a }

-- | Join (computed via the binary 'joinWith') of a non-empty list.
join :: ECRefDict a -> [a] -> a
join dict = foldl1' $ \ x y -> maybe x id (joinWith dict x y)

-- Motivation for the joinWith operation
--
-- Let (X, \/) be a join-semilattice with binary join \/, i.e. \/ is a
-- binary function on X that is associative, commutative and idempotent.
-- (X, \/) induces a partial order <<= defined by y <<= x iff x \/ y == x.
-- [Choosing symbol <<= to distinguish it from Haskell Ord class member <=.]
--
-- The semantics of the joinWith operation is
--
-- > joinWith :: X -> X -> Maybe X is
-- > joinWith x y | y <<= x   = Nothing        -- in which case: x \/ y == x
-- >              | otherwise = Just (x \/ y)  -- in which case: x \/ y != x
--
-- Note that joinWith combines the subsumption test (whether the join of
-- x and y exceeds x) with the computation of the join. However, joinWith
-- is no longer commutative.
--
-- TODO: Some examples (including 'joinWith x y = Just y', which works
-- but there isn't an ordering anymore.


-----------------------------------------------------------------------------
-- ECRef type (abstract outwith this module)
-- NOTE: ECRefs references are hyperstrict.

-- ECRefLabels, comprising a the creator node's ID plus a unique (on the
-- creator node) slot. Note that the slot, represented by a positive
-- integer, must be unique over the life time of the creator node (ie. it can
-- not be reused).
-- The 'ECRefLabel' type is not to be exported.
data ECRefLabel = ECRefLabel { slot    :: !Integer,
                               creator :: !Node }
                  deriving (Eq, Ord)

-- Constructs an 'ECRefLabel' created by a given node ID in the given slot;
-- ensures the resulting 'ECRefLabel' is hyperstrict.
mkECRefLabel :: Node -> Integer -> ECRefLabel
mkECRefLabel node i =
  rnf node `seq` rnf i `seq` ECRefLabel { slot = i, creator = node }

-- default NFData instance (suffices due to hyperstrictness of ECRefLabel)
instance NFData ECRefLabel where
  rnf x = seq x ()

-- NOTE: Can't derive this instance because 'get' must ensure hyperstrictness
instance Serialize ECRefLabel where
  put label = Data.Serialize.put (creator label) >>
              Data.Serialize.put (slot label)
  get = do node <- Data.Serialize.get
           i <- Data.Serialize.get
           return $! mkECRefLabel node i  -- 'mkECRefLabel' for hyperstrictness!

-- | An 'ECRef' is  phantom type wrapper around an 'ECRefLabel'.
newtype ECRef a = ECRef ECRefLabel deriving (Eq, Ord, NFData, Serialize)

-- Show instance (mainly for debugging)
instance Show (ECRef a) where
  showsPrec _ (ECRef label) =
    showString "ECRef:" . shows (creator label) .
    showString "." . shows (slot label)

-- | Returns the creator of the given 'ECRef'.
creatorECRef :: ECRef a -> Node
creatorECRef (ECRef label) = creator label


-----------------------------------------------------------------------------
-- ECRef registry

-- Existential type wrapping an object stored in an ECRef.
data ECRefObj where
  ECRefObj :: ECRefDict a -> IORef a -> [Node] -> ECRefObj
  -- An ECRefObj consist of
  -- * a dictionary of ECRef operations
  -- * a mutable cell holding the current value
  -- * a list of peer nodes

-- Registry type, comprising of the most recently allocated slot and a table
-- mapping ECRefLabels to ECRefObjs.
data ECRefReg = ECRefReg { lastSlot :: !Integer,
                           table    :: !(Map ECRefLabel ECRefObj) }

-- Registry (initially empty)
regRef :: IORef ECRefReg
regRef = unsafePerformIO $
           newIORef $ ECRefReg { lastSlot = 0, table = Map.empty }
{-# NOINLINE regRef #-}   -- required to protect unsafePerformIO hack

-- Registry lookup
lookupECRef :: ECRef a -> Par (Maybe (ECRefDict a, IORef a, [Node]))
lookupECRef (ECRef label) = do
  reg <- io $ readIORef regRef
  case Map.lookup label (table reg) of
    Nothing                         -> return Nothing
    Just (ECRefObj dict cell peers) -> return $ Just (dict', cell', peers) where
      { dict' = unsafeCoerce dict; cell' = unsafeCoerce cell }
      -- The reasons why 'unsafeCoerce' is safe here are essentially
      -- the same as for GRefs.


-----------------------------------------------------------------------------
-- Local ECRef operations (non-blocking)

-- | Returns the spatial scope of the given 'ECRef'.
-- The spatial scope is the list of nodes that propagate updates to the ECRef;
-- returns '[]' if the current node is not included in the spatial scope,
-- or the ECRef has already been freed.
scopeECRef :: ECRef a -> Par [Node]
scopeECRef ref = do
  maybe_obj <- lookupECRef ref
  case maybe_obj of
    Nothing            -> return []
    Just (_, _, peers) -> do { me <- myNode; return (me:peers) }


-- | Reads the given 'ECRef' locally; returns 'Nothing' if the current node is
-- not included in the spatial scope of the ECRef, or it has been freed already.
readECRef :: ECRef a -> Par (Maybe a)
readECRef ref = do
  maybe_obj <- lookupECRef ref
  case maybe_obj of
    Nothing           -> return Nothing
    Just (_, cell, _) -> fmap Just $ io $ readIORef cell


-----------------------------------------------------------------------------
-- Non-local ECRef operations (non-blocking)

-- | Writes value 'y' to the given ECRef (if current node is in scope).
-- The write joins the old value with 'y' and will only update the ECRef
-- if joinWith does not return Nothing.
-- If an update happens locally the value 'y' is asynchronously (but
-- immediately) propagated to all nodes in scope where it will be joined with
-- the local value, too.
-- Returns immediately without blocking (and without guarantees as to when
-- propagation will be complete).
writeECRef :: ECRef a -> a -> Par ()
writeECRef ref y = do
  maybe_obj <- lookupECRef ref
  case maybe_obj of
    Nothing                  -> return ()
    Just (dict, cell, peers) -> do
      maybe_new_x <- io $ atomicModifyIORef' cell (updateCell dict y)
      case maybe_new_x of
        Nothing -> return ()  -- fast path; no real update
        Just _  -> do
          let yC = toClosure dict y
          let task = $(mkClosure [| writeECRef_abs (ref, yC) |])
          rcall_ task peers

writeECRef_abs :: (ECRef a, Closure a) -> Thunk (Par ())
writeECRef_abs (ref, yC) = Thunk $ do
  maybe_obj <- lookupECRef ref
  case maybe_obj of
    Nothing              -> return ()
    Just (dict, cell, _) -> do
      _ <- io $ atomicModifyIORef' cell (updateCell dict $ unClosure yC)
      return ()

updateCell :: ECRefDict a -> a -> a -> (a, Maybe a)
updateCell dict y old_x =
  case joinWith dict old_x y of
    Nothing    -> (old_x, Nothing)
    Just new_x -> (new_x, Just new_x)


-----------------------------------------------------------------------------
-- Non-local ECRef operations (potentially blocking)

-- | Returns a new ECRef with initial value determined by Closure 'initC',
-- join operation determined by Closure 'joinWithC' and given spatial 'scope';
-- the scope of the resulting ECRef will include the current node (even if
-- missing from 'scope').
-- On return, the ECRef will have been set up on all nodes in 'scope'.
-- May block (and risk descheduling the calling thread).
newECRef :: Closure (ECRefDict a)-> [Node] -> a -> Par (ECRef a)
-- Closure a -> Closure (a -> a -> Maybe a) -> [Node] -> Par (ECRef a)
newECRef dictC scope x = do
  me <- myNode
  ref <- io $ atomicModifyIORef' regRef (createRef me)
  let scope' = me : delete me scope    -- NB: make sure 'me' is in scope
  let xC = toClosure (unClosure dictC) x
  let task = $(mkClosure [| newECRef_abs (ref, dictC, xC, scope') |])
  _ <- rcall task scope'
  return ref

newECRef_abs :: (ECRef a, Closure (ECRefDict a), Closure a, [Node])
             -> Thunk (Par (Closure ()))
newECRef_abs (ref, dictC, xC, scope') = Thunk $ do
  me <- myNode
  let peers = delete me scope'
  cell <- io $ newIORef $ unClosure xC
  let dict = unClosure dictC
  io $ atomicModifyIORef' regRef (createEntry (unClosure dictC) cell peers ref)
  return unitC

createRef :: Node -> ECRefReg -> (ECRefReg, ECRef a)
createRef me reg =
  (reg { lastSlot = newSlot }, ECRef $! mkECRefLabel me newSlot)
    where
      newSlot = lastSlot reg + 1

createEntry :: ECRefDict a -> IORef a -> [Node] -> ECRef a -> ECRefReg
            -> (ECRefReg, ())
createEntry dict cell peers (ECRef label) reg =
  (reg { table = Map.insert label (ECRefObj dict cell peers) (table reg) }, ())


-- | Frees the given ECRef on all nodes in its scope.
-- On return, the given ECRef will have been freed on all nodes in scope,
-- from which point on readECRef will return 'Nothing'.
-- May block (and risk descheduling the calling thread).
freeECRef :: ECRef a -> Par ()
freeECRef ref = do
  scope <- scopeECRef ref
  let task = $(mkClosure [| freeECRef_abs ref |])
  _ <- rcall task scope
  return ()

freeECRef_abs :: ECRef a -> Thunk (Par (Closure ()))
freeECRef_abs ref = Thunk $ do
  io $ atomicModifyIORef' regRef (deleteEntry ref)
  return unitC


-- | Reads from all nodes in the given ECRef's scope and returns the join
-- of all values read; returns Nothing if the current node is not in scope,
-- or the ECRef has been freed.
-- May block (and risk descheduling the calling thread).
-- Note that in any thread executing { x <- gatherECRef r; y <- readECRef r }
-- it is guaranteed that x is less than or equal to y (in the order induced
-- by joinWith) but it is not guaranteed that x is equal to y (because another
-- thread could have initiated a write just after the gather returned).
gatherECRef :: ECRef a -> Par (Maybe a)
gatherECRef ref = do
  maybe_obj <- lookupECRef ref
  case maybe_obj of
    Nothing                  -> return Nothing
    Just (dict, cell, peers) -> do
      x <- io $ readIORef cell
      let xC = toClosure dict x
      let task = $(mkClosure [| gatherECRef_abs (ref, xC) |])
      ys <- fmap (map unClosure) $ rcall task peers
      let !z = join dict (x:ys)
      return (Just z)

gatherECRef_abs :: (ECRef a, Closure a) -> Thunk (Par (Closure a))
gatherECRef_abs (ref, xC) = Thunk $ do
  maybe_obj <- lookupECRef ref
  case maybe_obj of
    Nothing              -> return xC    -- NB: ECRef has vanished, return xC
    Just (dict, cell, _) -> fmap (toClosure dict) $ io $ readIORef cell

deleteEntry :: ECRef a -> ECRefReg -> (ECRefReg, ())
deleteEntry (ECRef label) reg =
  (reg { table = Map.delete label (table reg) }, ())


-----------------------------------------------------------------------------
-- Static declaration (must be at end of module)

-- Empty splice; TH hack to make all environment abstractions visible.
$(return [])

declareStatic :: StaticDecl
declareStatic =
  mconcat
    [HdpH.declareStatic,        -- 'Static' decl of imported modules
     Strategies.declareStatic,
     declare $(static 'writeECRef_abs),
     declare $(static 'newECRef_abs),
     declare $(static 'freeECRef_abs),
     declare $(static 'gatherECRef_abs)]
