-- 'Static' support; types
--
-- Author: Patrick Maier
-----------------------------------------------------------------------------

{-# LANGUAGE GADTs #-}
{-# LANGUAGE DeriveDataTypeable #-}

module Control.Parallel.HdpH.Closure.Static.Type
  ( -- * 'Static' things (and constituent parts)
    Static(..),
    StaticLabel(..),
    StaticIndex,

    -- * 'Static' declarations and table
    StaticDecl(..),
    StaticTable,
    AnyStatic(..)
  ) where

import Prelude
import Data.Array (Array)
import Data.Int (Int32)
import Data.Map (Map)
import Data.Typeable (Typeable)
import Data.Word (Word32)


-----------------------------------------------------------------------------
-- Static things

-- A Static label consists of a 'name'; additionally, there is 32-bit hash
-- value (for faster comparisons). Note that all fields are strict.
data StaticLabel = StaticLabel { hash :: !Int32,
                                 name :: !String }
                   deriving (Typeable)


-- A Static index, that is an index into the global Static table.
type StaticIndex = Word32


-- A term of type 'Static a' (ie. essentially referring to a term of type 'a'
-- not capturing any free variables) is represented as
-- * a Static label, acting as a key,
-- * an index into the global Static table (also acting as a key), and
-- * the referred-to term (in field 'unstatic').
-- Note that the label is strict but the referred-to term and the index are not;
-- the evaluation of the index is delayed until serialisation.
data Static a = Static { label    :: !StaticLabel,
                         index    :: StaticIndex,
                         unstatic :: a }
                deriving (Typeable)
-- ^ A term of type @Static a@ is a serialisable reference to a /static/ term
-- of type @a@.

-- Existential Static type (wrapper for 'Static t' in heterogenous maps)
data AnyStatic where
  Any :: Static a -> AnyStatic


-----------------------------------------------------------------------------
-- Static declarations

-- A Static declaration maps Static labels to Static terms containing
-- selfsame labels.
-- NOTE: Each image stored in 'StaticDecl' is actually of type 'Static a'
--       for some 'a'. However, to fit into the map, we wrap all these types
--       into the existential 'AnyStatic'.
newtype StaticDecl = StaticDecl { unStaticDecl :: Map StaticLabel AnyStatic }
                     deriving (Typeable)
-- ^ A @'Static'@ declaration is a collection of static terms together with
-- their serialisable @'Static'@ references.
-- @'Static'@ declarations form a monoid, and can be combined by methods of
-- class @'Data.Monoid.Monoid'@.


-----------------------------------------------------------------------------
-- Static table

-- Global Static table, mapping static indices to existentially wrapped
-- Static terms.
type StaticTable = Array StaticIndex AnyStatic
