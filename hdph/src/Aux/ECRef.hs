-- Eventually Coherent References, and their by-products
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
    ECRef,        -- * -> *; instances: Eq, Ord, Show, NFData, Serialize

    -- * dictionary of operations necessary to support ECRefs
    ECRefDict(    -- * -> *; no instances
      ECRefDict,    -- :: (a -> Closure a) -> (a -> a -> Maybe a) -> ECRefDict a
      toClosure,    -- :: ECRefDict a -> a -> Closure a
      joinWith),    -- :: ECRefDict a -> a -> a -> Maybe a

    -- * local accesors (non-blocking)
    creatorECRef, -- :: ECRef a -> Node
    scopeECRef,   -- :: ECRef a -> Par [Node]
    readECRef,    -- :: ECRef a -> Par (Maybe a)
    readECRef',   -- :: ECRef a -> Par a

    -- * non-local write (non-blocking)
    writeECRef,   -- :: ECRef a -> a -> Par (Maybe a)
    writeECRef',  -- :: ECRef a -> a -> Par ()

    -- * non-local destruction (non-blocking)
    freeECRef,    -- :: ECRef a -> Par ()

    -- * creation (blocking)
    newECRef,     -- :: Closure (ECRefDict a)-> [Node] -> a -> Par (ECRef a)

    -- * non-local read (blocking)
    gatherECRef,  -- :: ECRef a -> Par (Maybe a)
    gatherECRef', -- :: ECRef a -> Par a


    -- * immutable references
    IMRef,        -- * -> *; instances: Eq, Ord, Show, NFData, Serialize

    -- * local accesors (non-blocking, similar to ECRefs)
    creatorIMRef, -- :: IMRef a -> Node
    scopeIMRef,   -- :: IMRef a -> Par [Node]
    readIMRef,    -- :: IMRef a -> Par (Maybe a)
    readIMRef',   -- :: IMRef a -> Par a

    -- * destruction (non-blocking, similar to ECRefs)
    freeIMRef,    -- :: IMRef a -> Par ()

    -- * creation (blocking, similar to ECRefs)
    newIMRef,     -- :: (a -> Closure a) -> [Node] -> a -> Par (IMRef a)


    -- * remote references
    RRef,         -- * -> *; instances: Eq, Ord, Show, NFData, Serialize

    -- * pure operations
    homeRRef,     -- :: RRef a -> Node

    -- * local, non-blocking operations (similar to ECRefs)
    newRRef,      -- :: a -> Par (RRef a)
    freeRRef,     -- :: RRef a -> Par ()
    readRRef,     -- :: RRef a -> Par (Maybe a)
    readRRef',    -- :: RRef a -> Par a
    writeRRef,    -- :: RRef a -> a -> Par (Maybe a)
    writeRRef',   -- :: RRef a -> a -> Par ()

    -- * remote, non-blocking operations
    rfreeRRef,    -- :: RRef a -> Par ()
    rwriteRRef',  -- :: RRef (Closure a) -> Closure a -> Par ()

    -- * remote, blocking operations
    rwriteRRef,   -- :: RRef (Closure a) -> Closure a -> Par (Maybe (Closure a))
    rreadRRef,    -- :: RRef (Closure a) -> Par (Maybe (Closure a))
    rreadRRef',   -- :: RRef (Closure a) -> Par (Closure a)

    -- * this module's Static declaration
    declareStatic -- :: StaticDecl
  ) where

import Prelude
import Control.DeepSeq (NFData(rnf))
import Control.Monad (unless, void)
import Data.IORef (IORef, newIORef, readIORef, atomicModifyIORef')
import Data.List (delete, foldl1')
import Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map (empty, insert, delete, lookup)
import Data.Maybe (isJust)
import Data.Serialize (Serialize)
import qualified Data.Serialize (put, get)
import System.IO.Unsafe (unsafePerformIO)
import Unsafe.Coerce (unsafeCoerce)

import Control.Parallel.HdpH
       (Par, Node, GIVar,
        myNode, allNodes, io, pushTo, new, glob, rput, get,
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
--
-- * ECRefs are distributed:
--   Each ECRef has a specific /spatial scope/, i.e. set of nodes where it
--   is accessible. The spatial scope is fixed upon an ECRef's creation.
--
-- * ECRefs act like coherent caches:
--   Reads and writes are non-blocking and go to a copy of the ECRef's value
--   stored on the current node. Writes are automatically propagated to all
--   nodes in scope. There is no guarantee when writes will be propagated
--   (though it is pretty quick), but there is an ordering guarantee, namely
--   that writes initiated by the same HdpH thread will be propagated in
--   the order they were initiated.
--
-- * ECRefs can accumulate values monotonically similar to LVars:
--   Writes join the new value with the old value, and only update and
--   propagate if the join is strictly greater than the old value.
--   Like in the case of LVars this feature helps to recover deterministic
--   behaviour in the face of non-deterministic distributed updates. (Can
--   we prove this?) The actual join (and the ordering it implies) is a
--   parameter fixed upon an ECRef's creation.
--
-- * ECRefs must be freed explicitly:
--   This is a wart; it could probably be fixed but would require distributed
--   bookkeeping of where the ECRef is still alive. Freeing isn't a big issue
--   as ECRefs are meant to be used inside skeletons with a well-defined
--   lifetime. However, the fact that ECRefs can be freed explicitly has
--   implications for the type of the read operation; it must return an option
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
-- > joinWith :: X -> X -> Maybe X
-- > joinWith x y | y <<= x   = Nothing        -- in which case: x \/ y == x
-- >              | otherwise = Just (x \/ y)  -- in which case: x \/ y != x
--
-- Note that joinWith combines the subsumption test (whether the join of
-- x and y exceeds x) with the computation of the join. However, joinWith
-- is no longer commutative, but the commutative join can be recovered from it
-- > join :: X -> X -> X
-- > join x y = case joinWith x y of { Nothing -> x; Just z -> z }
--
-- Some examples
--
-- * If X is an instance of Ord, the join \/ is max. Consequently,
--   joinWith is definable as
--   > joinWith x y = if y <= x then Nothing else Just y
--
-- * If X is some Set Y, the join \/ is union and the order isSubsetOf.
--   Consequently, joinWith is definable as
--   > joinWith x y = if y isSubsetOf x then Nothing else Just (x `union` y)
--
-- * A very simple definition of joinWith just ignores the old value
--   > joinWith x y = Just y
--   This does work; every write will be propagated and overwrite the old
--   value, making the ECRef behave much like an ordinary mutable reference.
--   However, note that the join recovered from this joinWith is \ x y -> y,
--   i.e. the projection on the second argument. This isn't a proper join
--   as it is not commutative, hence this joinWith leaves the domain of
--   join-semilattices, with implications for determinacy of computations
--   using ECRefs with this joinWith.
--
-- * An even simpler definition of joinWith ignores everything
--   > joinWith x y = Nothing
--   This does work but means that no write will ever succeed, rendering
--   the ECRef an immutable distributed reference.
--   Note that the join recovered from this joinWith is \ x y -> x,
--   i.e. the projection on the second argument. This isn't a proper join
--   as it is not commutative, hence this joinWith also leaves the domain of
--   join-semilattices. However, in this case there are no implications for
--   determinacy, as no writes ever succeed.


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
{-# INLINE lookupECRef #-}
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
{-# INLINE scopeECRef #-}
scopeECRef ref = do
  maybe_obj <- lookupECRef ref
  case maybe_obj of
    Nothing            -> return []
    Just (_, _, peers) -> do { me <- myNode; return (me:peers) }


-- | Reads the given 'ECRef' locally; returns 'Nothing' if the current node is
-- not included in the spatial scope of the ECRef, or it has been freed already.
readECRef :: ECRef a -> Par (Maybe a)
{-# INLINE readECRef #-}
readECRef ref = do
  maybe_obj <- lookupECRef ref
  case maybe_obj of
    Nothing           -> return Nothing
    Just (_, cell, _) -> fmap Just $ io $ readIORef cell

-- | Like 'readECRef' but does not wrap the value read in 'Maybe';
-- aborts with a runtime error if 'readECRef' would have returned Nothing.
readECRef' :: ECRef a -> Par a
{-# INLINE readECRef' #-}
readECRef' ref = do
  maybe_val <- readECRef ref
  case maybe_val of
    Just val -> return val
    Nothing  -> do me <- myNode
                   error $ show me ++ " ECRef.readECRef': " ++
                           show ref ++ " not in scope"


-----------------------------------------------------------------------------
-- Non-local ECRef operations (non-blocking)

-- | Writes value 'y' to the given ECRef (if current node is in scope).
-- The write joins the old value with 'y' and will only update the ECRef
-- if joinWith does not return Nothing.
-- If an update happens locally the value 'y' is asynchronously (but
-- immediately) propagated to all nodes in scope where it will be joined with
-- the local value, too.
-- Returns the new value if there is an update, 'Nothing' otherwise.
-- The function returns immediately without blocking, even if there is an
-- update which is being propagated; there is no guarantees as to when
-- propagation will be complete. (There may be some guarantees available
-- through knowledge of HdpH's message orders, but it's implemenation specific.)
writeECRef :: ECRef a -> a -> Par (Maybe a)
{-# INLINE writeECRef #-}
writeECRef ref !y = do
  maybe_obj <- lookupECRef ref
  case maybe_obj of
    Nothing                  -> return Nothing
    Just (dict, cell, peers) -> do
      maybe_new_x <- io $ atomicModifyIORef' cell (updateCell dict y)
      case maybe_new_x of
        Nothing    -> return Nothing  -- fast path; no update
        just_new_x -> do
          unless (null peers) $ do
            let yC = toClosure dict y
            let task = $(mkClosure [| writeECRef_abs (ref, yC) |])
            rcall_ task peers
          return just_new_x

writeECRef_abs :: (ECRef a, Closure a) -> Thunk (Par ())
{-# INLINE writeECRef_abs #-}
writeECRef_abs (ref, yC) = Thunk $ do
  maybe_obj <- lookupECRef ref
  case maybe_obj of
    Nothing              -> return ()
    Just (dict, cell, _) -> do
      _ <- io $ atomicModifyIORef' cell (updateCell dict $ unClosure yC)
      return ()

updateCell :: ECRefDict a -> a -> a -> (a, Maybe a)
{-# INLINE updateCell #-}
updateCell dict y old_x =
  case joinWith dict old_x y of
    Nothing    -> (old_x, Nothing)
    Just new_x -> (new_x, Just new_x)


-- | Convenience variant of 'writeECRef', suppressing return value.
writeECRef' :: ECRef a -> a -> Par ()
writeECRef' ref = void . writeECRef ref


-- | Frees the given ECRef on all nodes in its scope.
-- After this call, a call to 'readECRef' may return 'Nothing'.
freeECRef :: ECRef a -> Par ()
freeECRef ref = do
  scope <- scopeECRef ref
  let task = $(mkClosure [| freeECRef_abs ref |])
  rcall_ task scope
  return ()

freeECRef_abs :: ECRef a -> Thunk (Par ())
freeECRef_abs ref = Thunk $
  io $ atomicModifyIORef' regRef (deleteEntry ref)

deleteEntry :: ECRef a -> ECRefReg -> (ECRefReg, ())
deleteEntry (ECRef label) reg =
  (reg { table = Map.delete label (table reg) }, ())


-----------------------------------------------------------------------------
-- Non-local ECRef operations (potentially blocking)

-- | Returns a new ECRef with initial value 'x'; join operation and
-- closure conversion are determined by the closure dictionary 'dictC';
-- the scope of the resulting ECRef will include the current node (even if
-- missing from 'scope'); 'scope == []' is interpreted as universal scope.
-- On return, the ECRef will have been set up on all nodes in 'scope'.
-- May block (and risk descheduling the calling thread).
newECRef :: Closure (ECRefDict a)-> [Node] -> a -> Par (ECRef a)
newECRef dictC scope x =
  mkNewECRef dictC scope x (toClosure (unClosure dictC) x)

-- Worker function called by newECRef, newIMRef and newRRef.
-- NOTE: Deliberately non-strict in argument 'xC' when there are no peers;
--       newRRef relies on this behaviour!
mkNewECRef :: Closure (ECRefDict a)-> [Node] -> a -> Closure a -> Par (ECRef a)
mkNewECRef dictC scope x xC = do
  let !dict = unClosure dictC
  -- sort out scope; ensure 'me' is included and 'scope == []' means all nodes
  me <- myNode
  peers <- delete me <$> if null scope then allNodes else return scope
  let scope' = me:peers
  -- create reference
  ref <- io $ atomicModifyIORef' regRef (createRef me)
  -- set up reference cell locally
  cell <- io $ newIORef x
  io $ atomicModifyIORef' regRef (createEntry dict cell peers ref)
  -- set up reference cell at peers (if there are any)
  unless (null peers) $ do
    let task = $(mkClosure [| mkNewECRef_abs (ref, dictC, xC, scope') |])
    void $ rcall task peers
  -- return reference
  return ref

mkNewECRef_abs :: (ECRef a, Closure (ECRefDict a), Closure a, [Node])
               -> Thunk (Par (Closure ()))
mkNewECRef_abs (ref, dictC, xC, scope') = Thunk $ do
  me <- myNode
  let peers = delete me scope'
  cell <- io $ newIORef $ unClosure xC
  io $ atomicModifyIORef' regRef (createEntry (unClosure dictC) cell peers ref)
  return unitC

createRef :: Node -> ECRefReg -> (ECRefReg, ECRef a)
createRef me reg =
  (reg { lastSlot = newSlot }, ECRef $! mkECRefLabel me newSlot)
    where
      newSlot = lastSlot reg + 1

createEntry :: ECRefDict a -> IORef a -> [Node] -> ECRef a -> ECRefReg
            -> (ECRefReg, ())
createEntry !dict cell peers (ECRef label) reg =
  (reg { table = Map.insert label (ECRefObj dict cell peers) (table reg) }, ())


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
      if null peers
        then return (Just x)
        else do
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


-- | Like 'gatherECRef' but does not wrap the value returned in 'Maybe';
-- aborts with a runtime error if 'gatherECRef' would have returned Nothing.
gatherECRef' :: ECRef a -> Par a
gatherECRef' ref = do
  maybe_val <- gatherECRef ref
  case maybe_val of
    Just val -> return val
    Nothing  -> do me <- myNode
                   error $ show me ++ " ECRef.gatherECRef': " ++
                           show ref ++ " not in scope"


-----------------------------------------------------------------------------
-- Immutable references

-- Immutable references (IMRefs) are immutable values that can be created
-- across a spatial scope spanning multiple nodes and referred to globally.
--
-- IMRefs share many properties with ECRefs; in fact, they are implemented
-- as ECRefs. Notably: IMRefs need to be freed explicitly, and reading from
-- an IMRef may return Nothing (in case the IMRef has already been freed,
-- or the reading node does not belong to its scope).
-- However, there are also differences:
-- * IMRefs are immutable, so they do not support any write operations.
-- * There is no gather operation; it isn't necessary as IMRefs never change.

-- | Immutable references are just type synonyms for ECRefs.
newtype IMRef a = IMRef { unIMRef :: ECRef a }
                  deriving (Eq, Ord, Show, NFData, Serialize)

-- | Like creatorECRef.
creatorIMRef :: IMRef a -> Node
creatorIMRef = creatorECRef . unIMRef

-- | Creates a new IMRef with value 'x' on nodes in 'scope'; the scope of
-- the IMRef will include the current node (even if missing from 'scope');
-- 'scope == []' is interpreted as universal scope.
-- The 'toClo' function is used only once to convert 'x' into a closure
-- that can be shipped to other nodes; since the IMRef is immutable,
-- there is no need for a closure conversion in the dictionary.
-- On return, the IMRef will have been set up on all nodes in 'scope'.
-- May block (and risk descheduling the calling thread).
newIMRef :: (a -> Closure a) -> [Node] -> a -> Par (IMRef a)
newIMRef toClo scope x =
  IMRef <$> mkNewECRef rrefDictC scope x (toClo x)
  -- NOTE about 'rrefDictC': The IMRef operations won't access the dictionary
  -- so we can pass any closured dictionary of appropriate type.

-- | Like scopeECRef.
scopeIMRef :: IMRef a -> Par [Node]
scopeIMRef = scopeECRef . unIMRef

-- | Like readECRef.
readIMRef :: IMRef a -> Par (Maybe a)
readIMRef = readECRef . unIMRef

-- | Like readECRef'.
readIMRef' :: IMRef a -> Par a
readIMRef' = readECRef' . unIMRef

-- | Like freeECRef.
freeIMRef :: IMRef a -> Par ()
freeIMRef = freeECRef . unIMRef


-----------------------------------------------------------------------------
-- Remote references

-- Remote references (RRefs) are ordinary mutable variables that can be
-- referred to globally. That is, an RRef is a serialisable handle to
-- the underlying mutable variable.
--
-- RRefs share many properties with ECRefs; in fact, they are implemented
-- as ECRefs. Notably: RRefs need to be freed explicitly, and reading from
-- an RRef may return Nothing (in case the RRef has already been freed).
-- However, there are also differences:
-- * RRefs always live on a single node.
-- * Ordinary RRef operations are only effective on the the RRef's home node.
-- * RRefs may contain non-serialisable values.
-- * RRefs can contain closures, in which case remote read and write
--   operations are supported.

-- | Remote references are just type synonyms for ECRefs.
newtype RRef a = RRef { unRRef :: ECRef a }
                 deriving (Eq, Ord, Show, NFData, Serialize)

-- | Node where the remote reference lives.
homeRRef :: RRef a -> Node
homeRRef = creatorECRef . unRRef

-- | Create a new remote reference.
-- Note that this does not require a closured dictionary as values are never
-- transmitted (and hence can be of any type), and the "join" is predetermined.
-- There is also no scope, as the scope of a remote reference is just the
-- creating node.
newRRef :: a -> Par (RRef a)
newRRef x = do
  me <- myNode
  RRef <$> mkNewECRef rrefDictC [me] x (error "Aux.ECRef.newRRef: PANIC")
  -- NOTE about 'error' call: This argument is never meant to be evaluated.

-- Dictionary for remote refs; note that 'toClosure' will never be called,
-- and that 'joinWith' will always overwrite the old value!
rrefDict :: ECRefDict a
rrefDict = ECRefDict { toClosure = error "Aux.ECRef.toClosure: PANIC",
                       joinWith  = \ _x y -> Just y }

rrefDictC :: Closure (ECRefDict a)
rrefDictC = $(mkClosure [| rrefDict |])

-- | Like freeECRef (but only effective on the the home node).
freeRRef :: RRef a -> Par ()
freeRRef = freeECRef . unRRef

-- | Like writeECRef; will return Nothing unless RRef exists on calling node.
-- Returns the second argument when not returning Nothing.
writeRRef :: RRef a -> a -> Par (Maybe a)
writeRRef = writeECRef . unRRef

-- | Like writeECRef' (but only effective on the the home node).
-- Note that writes always succeed hence this write does not return anything.
writeRRef' :: RRef a -> a -> Par ()
writeRRef' = writeECRef' . unRRef

-- | Like readECRef; will return Nothing anywhere but on the home node.
readRRef :: RRef a -> Par (Maybe a)
readRRef = readECRef . unRRef

-- | Like readECRef'.
readRRef' :: RRef a -> Par a
readRRef' = readECRef' . unRRef


-- | Remote deallocation of RRefs; non-blocking.
rfreeRRef :: RRef a -> Par ()
rfreeRRef !rref =
  pushTo $(mkClosure [| rfreeRRef_abs rref |]) $ homeRRef rref

rfreeRRef_abs :: RRef a -> Thunk (Par ())
rfreeRRef_abs rref = Thunk $ freeRRef rref


-- | Remote write for RRef; non-blocking.
rwriteRRef' :: RRef (Closure a) -> Closure a -> Par ()
rwriteRRef' !rref !clo =
  pushTo $(mkClosure [| rwriteRRef'_abs (rref, clo) |]) $ homeRRef rref

rwriteRRef'_abs :: (RRef (Closure a), Closure a) -> Thunk (Par ())
rwriteRRef'_abs (rref, clo) = Thunk $ writeRRef' rref clo


-- | Remote write for RRef; blocking.
-- Blocks until after the remote write has happened.
-- Returns the second argument if the write was successful, Nothing otherwise.
-- Note that Nothing is returned only if the RRef has already been freed.
rwriteRRef :: RRef (Closure a) -> Closure a -> Par (Maybe (Closure a))
rwriteRRef !rref !clo = do
  v <- new
  gv <- glob v
  pushTo $(mkClosure [| rwriteRRef_abs (rref, clo, gv) |]) $ homeRRef rref
  ok <- unClosure <$> get v
  if ok then return (Just clo) else return Nothing

rwriteRRef_abs :: (RRef (Closure a), Closure a, GIVar (Closure Bool))
               -> Thunk (Par ())
rwriteRRef_abs (rref, clo, gv) = Thunk $
  rput gv =<< toClosureBool . isJust <$> writeRRef rref clo


-- | Remote read for RRef; blocking.
rreadRRef :: RRef (Closure a) -> Par (Maybe (Closure a))
rreadRRef !rref = do
  v <- new
  gv <- glob v
  pushTo $(mkClosure [| rreadRRef_abs (rref, gv) |]) $ homeRRef rref
  unClosure <$> get v

rreadRRef_abs :: (RRef (Closure a), GIVar (Closure (Maybe (Closure a))))
              -> Thunk (Par ())
rreadRRef_abs (rref, gv) = Thunk $
  rput gv =<< toClosureMaybeClosure <$> readRRef rref


-- | Remote read for RRef; blocking.
-- This version is analogue to readECRef' and will crash if the RRef is gone.
rreadRRef' :: RRef (Closure a) -> Par (Closure a)
rreadRRef' !rref = do
  v <- new
  gv <- glob v
  pushTo $(mkClosure [| rreadRRef'_abs (rref, gv) |]) $ homeRRef rref
  get v

rreadRRef'_abs :: (RRef (Closure a), GIVar (Closure a)) -> Thunk (Par ())
rreadRRef'_abs (rref, gv) = Thunk $
  rput gv =<< readRRef' rref


-- Closure conversion for Booleans.
toClosureBool :: Bool -> Closure Bool
toClosureBool !x = $(mkClosure [| toClosureBool_abs x |])

toClosureBool_abs :: Bool -> Thunk Bool
toClosureBool_abs x = Thunk x


-- Closure conversion for options of closures.
toClosureMaybeClosure :: Maybe (Closure a) -> Closure (Maybe (Closure a))
toClosureMaybeClosure !x = $(mkClosure [| toClosureMaybeClosure_abs x |])

toClosureMaybeClosure_abs :: Maybe (Closure a) -> Thunk (Maybe (Closure a))
toClosureMaybeClosure_abs x = Thunk x


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
     declare $(static 'mkNewECRef_abs),
     declare $(static 'freeECRef_abs),
     declare $(static 'gatherECRef_abs),
     declare $(static 'rrefDict),
     declare $(static 'rfreeRRef_abs),
     declare $(static 'rwriteRRef'_abs),
     declare $(static 'rwriteRRef_abs),
     declare $(static 'rreadRRef_abs),
     declare $(static 'rreadRRef'_abs),
     declare $(static 'toClosureBool_abs),
     declare $(static 'toClosureMaybeClosure_abs)]
