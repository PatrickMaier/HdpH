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
    StaticDecl(..)
  ) where

import Prelude
import Data.Map (Map)

import HdpH.Internal.Misc (AnyType)


-----------------------------------------------------------------------------
-- 'Static' things

-- A static label consists of a 'name' and a discriminating 'typerep'.
data StaticLabel = StaticLabel { name    :: !String,
                                 typerep :: !String }


-- A term of type 'Static a' (ie. essentially refering to a thing of type 'a'
-- not capturing any free variables) is represented as a static label.
-- Note that the phantom type variable 'a' allows to keep track of the type
-- of the actual static thing, so we'll know the type when we need to
-- unwrap a 'Static' term. Additionally, a 'Static' term may have the
-- value it's refering to attached (but only if the 'Static' term originated
-- on the current node).
data Static a = Static { label :: !StaticLabel, value :: Maybe a }


-----------------------------------------------------------------------------
-- 'Static' declarations

-- A 'Static' declaration maps static labels to their declared values
-- (wrapped in an existential type to fit into the map).
newtype StaticDecl = StaticDecl { unStaticDecl :: Map StaticLabel AnyType }
