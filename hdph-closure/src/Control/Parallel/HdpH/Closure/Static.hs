-- 'Static' support, mimicking
--   [1] Epstein et al. "Haskell for the cloud". Haskell Symposium 2011.
--
-- Author: Patrick Maier
-----------------------------------------------------------------------------

-- The 'Static' type constructor as proposed in [1] is not yet supported
-- by GHC (as of version 7.4.1). This module provides a workaround that
-- necessarily differs in some aspects. However, the type constructor 'Static'
-- itself and its eliminator 'unstatic' work exactly as proposed in [1].
--
-- Differences between 'Control.Parallel.HdpH.Closure.Static' and a builtin
-- 'Static' are:
-- o Static values must be declared at toplevel
-- o Static terms must be explicitly named (via 'staticAs')
-- o Static terms must be explicitly declared and registered before use

{-# OPTIONS_GHC -fno-warn-orphans #-}  -- orphan instances unproblematic; types
                                       -- affected either internal or abstract

module Control.Parallel.HdpH.Closure.Static
  ( -- * 'Static' type constructor
    Static,      -- instances: Eq, Ord, Show, NFData, Serialize

    -- * introducing 'Static'
    staticAs,    -- :: a -> String -> Static a

    -- * eliminating 'Static'
    unstatic,    -- :: Static a -> a

    -- * declaring Static terms
    StaticDecl,  -- instances: Monoid, Show
    declare,     -- :: Static a -> StaticDecl

    -- * registering a Static declaration in the global Static table
    register,    -- :: StaticDecl -> IO ()

    -- * show contents of the global Static table
    showStaticTable  -- :: [String]
  ) where

import Prelude
import Control.DeepSeq (NFData(rnf))
import Control.Exception (evaluate)
import Control.Monad (unless)
import Data.Array (listArray, (!), bounds, elems)
import Data.Bits (shiftL, xor)
import Data.Functor ((<$>))
import Data.IORef (readIORef, atomicModifyIORef)
import Data.Int (Int32)
import Data.List (foldl')
import qualified Data.Map as Map (empty, singleton, union, unions, keys, elems)
import Data.Monoid (Monoid(mempty, mappend, mconcat))
import Data.Serialize (Serialize)
import qualified Data.Serialize (put, get)
import System.IO.Unsafe (unsafePerformIO)
import Unsafe.Coerce (unsafeCoerce)

import Control.Parallel.HdpH.Closure.Static.State (sTblRef)
import Control.Parallel.HdpH.Closure.Static.Type
       (StaticLabel(StaticLabel), hash, name, 
        StaticIndex,
        Static(Static), label, index, unstatic, 
        AnyStatic(Any),
        StaticDecl(StaticDecl), unStaticDecl)


-----------------------------------------------------------------------------
-- Key facts about 'Static'
--
-- o We say that a term 't' /captures/ a variable 'x' if 'x' occurs free
--   in 't' and 'x' is not declared at the top-level. (That is, this notion
--   of capture is sensitive to the context.)
--
-- o We call a term 't' /static/ if it could be declared at the top-level,
--   that is if does not capture any variables. In contrast, we call a
--   term /Static/ if it is of type 'Static t' for some type 't'.
--
-- o In principle, a term 'stat' of type 'Static t' acts as serialisable
--   reference to a static term of type 't'. However, here we realise
--   terms of type 'Static t' as a triple consisting of such a serialisable
--   reference (an index in the global Static table), a reference name
--   (a unique label of type String) and the refered-to static term itself.
--
-- o Only the serialisable reference is actually serialised when a Static term 
--   'stat' is serialised. The link between that reference and the associated 
--   Static term 'stat' is established by a registry, the /Static table/,
--   mapping indices to /registered/ Static terms.
--
-- o There are two ways of obtaining a term 'stat :: Static t'. By introducing
--   it with the smart constructor 'staticAs', or by deserialising it.
--   The latter assumes that the Static table has been initialised before
--   and has been initialised identically across all nodes; failing this
--   requirement may result in horrible crashes.
--
-- o Applications using this module (even indirectly) must go through two
--   distinct phases: 
--   (1) Introduce and register all Static terms (of which, by their nature, 
--       there can only be a statically bounded number) but do neither
--       serialise nor deserialise any Static terms. 
--   (2) Serialise and Deserialise Static terms at will but do not attempt
---      to register any new Static terms.


-----------------------------------------------------------------------------
-- How to register Static terms
--
-- Every module which introduces Static terms via 'staticAs' must export
-- a term 'declareStatic :: StaticDecl' for declaring these terms. The
-- main module must import all these 'declareStatic' terms, concat them 
-- to form a global Static declaration, and register that declaration.
--
-- To demonstrate the simplest example, suppose module 'M1' introduces two
-- Static terms (as top-level variables).
-- 
-- > x_Static :: Static Int
-- > x_Static = staticAs 42 "Fully_Qualified_Name_Of_M1.x"
-- >
-- > f_Static :: Static ([a] -> Int)
-- > f_Static = staticAs f "Fully_Qualified_Name_Of_M1.f"
-- >   where
-- >     f []     = 0
-- >     f (_:xs) = 1 + f xs
--
-- Then 'M1' ought to export
--
-- > declareStatic :: StaticDecl
-- > declareStatic = Data.Monoid.mconcat
-- >                   [declare x_Static,
-- >                    declare f_Static]
--
-- Now suppose module 'M3' imports 'M1'. It doesn't matter whether 'M3'
-- imports the Static terms of 'M1' directly; any function exported by
-- 'M1' may depend on its Static terms being registered, so 'M3' must
-- ensure that registration happens. Suppose further that 'M3' also imports
-- a module 'M2' which requires some other Static terms to be registered,
-- and that 'M3' itself introduces a term 'x_Static :: Static Bool'
-- (eg. by 'x_Static = staticAs True "Fully_Qualified_Name_Of_M3.x").
-- In this case, 'M3' ought to export the following
--
-- > declareStatic :: StaticDecl
-- > declareStatic = Data.Monoid.mconcat
-- >                   [M1.declareStatic,
-- >                    M2.declareStatic,
-- >                    declare x_Static]
--
-- Note that 'M2' may import 'M1' and so the definition of 'M2.declareStatic'
-- may contain a call to 'M1.declareStatic', in which case the above 'mconcat'
-- would process the data in 'M1.declareStatic' twice. This is of no concern,
-- thanks to the defintion of 'mconcat', as long as the reference name
-- (the second argument of 'staticAs') of each Static term is unique.
--
-- The 'Main' module must define a term 'declareStatic', similar to
-- 'M3.declareStatic' above. This term must then be passed to 'register'
-- right at the start of the 'main' action.
--
-- > main :: IO ()
-- > main = do
-- >   register declareStatic
-- >   ...
--
-- The module 'Control.Parallel.HdpH.Closure' offers more convenient support
-- for declaring and registering Static terms linked to explicit closures.


-----------------------------------------------------------------------------
-- Static terms (abstract outwith this module)
--
-- NOTE: Static labels are hyperstrict.
--       Static terms are strict in their label part.
--       Static terms are not strict in their index part, the evaluation
--       of which is delayed until the Static term is scrutinised, eg. by
--       comparisons or by serialisation. However, a Static term
--       produced by deserialisation is strict in its index part.

-- Constructs a 'StaticLabel' out of a name 'nm' (which is usually a fully
-- qualified toplevel variable name).
-- This constructor ensures that the resulting 'StaticLabel' is hyperstrict;
-- this constructor is not to be exported.
-- NOTE: Hyperstrictness is guaranteed by the strictness flags in the data
--       declaration of 'StaticLabel'. For the 'hash' field this already
--       implies full normalisation, computing the hash also fully normalises
--       the 'name' field.
mkStaticLabel :: String -> StaticLabel
mkStaticLabel nm = StaticLabel { hash = hashString nm, name = nm }


-- Constructs a Static term referring to the given static value 'val'
-- and identified by the given reference name 'nm'.
-- NOTE: Strictness flag on 'label' forces that field to WHNF (which conincides
--       with NF due to hyperstrictness of 'StaticLabel').
staticAs :: a -> String -> Static a
staticAs val nm =
  Static { label = lbl, index = findIndex lbl, unstatic = val }
    where
      lbl = mkStaticLabel nm


-----------------------------------------------------------------------------
-- Eq/Ord/Show/NFData instances for 'StaticLabel'

instance Eq StaticLabel where
  lbl1 == lbl2 = hash lbl1 == hash lbl2 &&   -- compare 'hash' first
                 name lbl1 == name lbl2

instance Ord StaticLabel where
  compare lbl1 lbl2 =
    case compare (hash lbl1) (hash lbl2) of  -- compare 'hash' 1st
      LT -> LT
      GT -> GT
      EQ -> compare (name lbl1) (name lbl2)

instance Show StaticLabel where
  showsPrec _ lbl = showString (name lbl)  -- show only reference name

instance NFData StaticLabel where
  rnf x = seq x ()

-----------------------------------------------------------------------------
-- Eq/Ord/Show/NFData instances for 'Static';
-- all of these instance force the 'index' part of a Static term but not the
-- refered-to static term; the 'label' part plays no role except for 'Show'.

instance Eq (Static a) where
  stat1 == stat2 = index stat1 == index stat2

instance Ord (Static a) where
  stat1 <= stat2 = index stat1 <= index stat2

instance Show (Static a) where
  showsPrec _ stat =
    showString "Static[" . shows (index stat) . showString "]=" .
    shows (label stat)

instance NFData (Static a) where
  rnf stat = index stat `seq` ()  -- forcing 'index' also forces strict 'label'


-----------------------------------------------------------------------------
-- Serialize instance for 'Static'

instance Serialize (Static a) where
  put = Data.Serialize.put . index
  get = resolve <$> Data.Serialize.get
        -- NOTES:
        -- o Hyperstrictness is enforced by 'resolve' (and by construction
        --   of the Static table).
        -- o Serialisation crashes if the Static term is not yet registered.
        -- o Deserialisation crashes if the index passed to 'resolve' is
        --   out of bounds (but that can only happen when deserialising
        --   a bytestring that wasn't generated from a registered Static term).


-- Resolves an index obtained from deserialisation into a Static term
-- by accessing the index in the global Static table; assumes that the
-- index is in bounds.
resolve :: StaticIndex -> Static a
resolve i =
  case (unsafePerformIO $ readIORef sTblRef) ! i of
    Any stat -> unsafeCoerce stat

-- Why 'unsafeCoerce' in safe in 'resolve':
--
-- o Static terms can only be introduced by the smart constructor 'staticAs'.
--
-- o We assume that the 'label' part of a Static term (as constructed by 
--   'staticAs') is a /key/.  That is, whenever terms 'stat1 :: Static t1' 
--   and 'stat2 :: Static t2' are introduced by 'staticAs' such that 
--   'label stat1 == label stat2' then 'unstatic stat1 :: t1' is identical 
--   to 'unstatic stat2 :: t2' (which also implies that the types 't1' and 
--   't2' are identical). This assumption is justified if labels are
--   derived from fully qualified top-level names.
--
-- o Likewise, we assume that the 'index' part of a Static term is a /key/.
--   This follows from 'label' being a key and from the way the Static table
--   is constructed by 'register'.
--
-- o Thus, we can safely super-impose (using 'unsafeCoerce') the return type
--   'Static a' of 'resolve' on the returned Static term. This type will
--   have been dictated by the calling context of 'resolve', ie. by the
--   calling context of deserialisation.


-----------------------------------------------------------------------------
-- Operations on existential 'Static' wrapper

instance Show AnyStatic where
  showsPrec _ (Any stat) = shows stat

instance NFData AnyStatic where
  rnf (Any stat) = rnf stat

getLabel :: AnyStatic -> StaticLabel
getLabel (Any stat) = label stat

setIndex :: StaticIndex -> AnyStatic -> AnyStatic
setIndex i (Any stat) = Any $ stat { index = i }


-----------------------------------------------------------------------------
-- Static declarations (abstract outwith this module)

-- Promotes the given Static term to a Static declaration (by mapping the
-- label to the Static term, existentially wrapped).
declare :: Static a -> StaticDecl
declare stat = StaticDecl $ Map.singleton (label stat) (Any stat)
-- ^ Promotes a given @'Static'@ reference to a @'Static'@ declaration
-- consisting exactly of the given @'Static'@ reference (and the static term
-- it refers to).
-- See the tutorial below for how to declare @'Static'@ references.


-- Static declarations form a monoid, at least if we assume that 'label'
-- is a /key/ for Static terms, ie. distinct Static terms will have
-- distinct labels. Note that this assumption is not checked: If it fails
-- then type safety of deserialisation can be compromised. The 'Static'
-- support in module 'HpdH.Closure', however, should guarantee the above
-- /key property/.
instance Monoid StaticDecl where
  mempty = StaticDecl Map.empty
  sd1 `mappend` sd2 = StaticDecl (unStaticDecl sd1 `Map.union` unStaticDecl sd2)
  mconcat = StaticDecl . Map.unions . map unStaticDecl


-- Show instance only for debugging; only shows domain of 'StaticDecl'
instance Show StaticDecl where
  showsPrec _ sd = showString "StaticDecl " .
                   shows (Map.keys $ unStaticDecl sd)


-----------------------------------------------------------------------------
-- Static table

-- | Registers the given @'Static'@ declaration; that is, stores the
-- declaration in the global @'Static'@ /table/. Must be called exactly once,
-- before any operations involving @'Static'@ references (or explicit Closures).
-- See the tutorial below for how to register a @'Static'@ declaration.
register :: StaticDecl -> IO ()
register sd = do
  let stats = zipWith setIndex [1..] (Map.elems $ unStaticDecl sd)
  let table = listArray (1, fromIntegral $ length stats) stats
  evaluate (rnf table)  -- force table to normal form
  ok <- atomicModifyIORef sTblRef $ \ old_table ->
          if snd (bounds old_table) < 1
            then (table, True)
            else (old_table, False)
  unless ok $
    error "Control.Parallel.HdpH.Closure.register: 2nd attempt"


-- Converts a Static label into a Static index by means of binary search;
-- not exported and to be called only after registration.
findIndex :: StaticLabel -> StaticIndex
findIndex lbl = search (bounds table)
  where
    table = unsafePerformIO $ readIORef sTblRef
    search (lower, upper)
      | lower > upper = error msg
      | otherwise     = case cmp of
                          EQ -> i
                          LT -> search (lower, i - 1)
                          GT -> search (i + 1, upper)
        where
          i = (lower + upper) `div` 2
          cmp = compare lbl (getLabel $ table ! i)
          msg = "Control.Parallel.HdpH.Closure.Static.findIndex: " ++
                show lbl ++ " not found"


-- | Emits the contents of the global @'Static'@ table as a list of Strings,
-- one per entry. Useful for debugging; not to be called prior to registration.
showStaticTable :: [String]
showStaticTable = map show $ elems table
  where
    table = unsafePerformIO $ readIORef sTblRef


-----------------------------------------------------------------------------
-- auxiliary stuff: hashing strings; adapted from Data.Hashable.

hashStringWithSalt :: Int32 -> String -> Int32
hashStringWithSalt = foldl' (\ h ch -> h `combine` toEnum (fromEnum ch))
                       where
                         combine :: Int32 -> Int32 -> Int32
                         combine h1 h2 = (h1 + h1 `shiftL` 5) `xor` h2

-- NOTE: Hashing fully evaluates the String argument as a side effect.
hashString :: String -> Int32
hashString = hashStringWithSalt defaultSalt
               where
                 defaultSalt = 5381
