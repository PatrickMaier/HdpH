-- 'Static' support; types
--
-- Visibility: HdpH.Internal.{Static,State.Static}
-- Author: Patrick Maier <P.Maier@hw.ac.uk>
-- Created: 26 Sep 2011
--
-----------------------------------------------------------------------------

module HdpH.Internal.Type.Static
  ( -- * 'Static' things (and constituent parts)
    Static(..),
    StaticLabel(..),

    -- * 'Static' declarations
    StaticDecl
  ) where

import Prelude
import Data.Int (Int32)
import Data.Map (Map)

import HdpH.Internal.Misc (AnyType)


-----------------------------------------------------------------------------
-- 'Static' things

-- A static label consists of a 'name' and a discriminating 'typerep';
-- additionally, there is 32-bit hash value (for faster comparisons).
-- Note that all fields are strict.
data StaticLabel = StaticLabel { hash    :: !Int32,
                                 name    :: !String,
                                 typerep :: !String }


-- A term of type 'Static a' (ie. essentially refering to a term of type 'a'
-- not capturing any free variables) is represented as a static label,
-- acting as a key, together with the refered-to term (in field 'unstatic').
-- Note that the label is strict but the refered-to term is not.
data Static a = Static { label :: !StaticLabel, unstatic :: a }


-----------------------------------------------------------------------------
-- 'Static' declarations

-- A 'Static' declaration maps static labels to 'Static' terms containing 
-- selfsame labels.
-- NOTE: All images stored in 'StaticDecl' are actually of type 'Static a',
--       for some 'a'. However, to fit into the map, we wrap all these types
--       into the existential 'AnyType'.
type StaticDecl = Map StaticLabel AnyType
