-- This module implements an /explicit closure/ API as described in [2] below;
-- the closure representation is novel (and as yet unpublished).
-- It makes extensive use of Template Haskell, and due to TH's stage
-- restrictions, internals like the actual closure representation are
-- delegated to module 'Control.Parallel.HdpH.Closure.Internal'.
--
-- Author: Patrick Maier
-----------------------------------------------------------------------------

-- | Explicit Closures, inspired by [1] and refined by [2].
--
-- References:
--
-- (1) Epstein, Black, Peyton-Jones. /Towards Haskell in the Cloud/.
--     Haskell Symposium 2011.
--
-- (2) Maier, Trinder. /Implementing a High-level Distributed-Memory/
--     /Parallel Haskell in Haskell/. IFL 2011.
--

{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE FlexibleInstances #-}    -- req'd for 'ToClosure' instances

module Control.Parallel.HdpH.Closure
  ( -- * Explicit Closures
    -- ** Key facts
    -- $KeyFacts

    -- ** The Closure type constructor
    Closure,

    -- ** Type constructor marking return type of environment abstractions
    Thunk(Thunk),

    -- ** Caveat: Deserialisation is not type safe
    -- $Deserialisation

    -- ** Closure elimination
    unClosure,

    -- ** Forcing Closures
    forceClosure,

    -- ** Categorical operations on function Closures
    -- | A /function Closure/ is a Closure of type @'Closure' (a -> b)@.
    idC,
    termC,
    compC,
    apC,

    -- ** Construction of value Closures
    -- | A /value Closure/ is a Closure, which when eliminated (from a
    -- serialised representation, at least) yields an evaluated value
    -- (rather than an unevaluated thunk).
    ToClosure,
    mkToClosure,
    toClosure,

    -- ** Safe Closure construction
    mkClosure,

    -- * Static terms
    -- | A term @t@ is called /static/ if it could be declared at the toplevel.
    -- That is, @t@ is static if all free variables occuring in @t@ are 
    -- toplevel.

    -- ** The Static type constructor
    Static,

    -- ** Static declaration and registration
    StaticDecl,
    declare,
    register,
    showStaticTable,

    -- ** Static Closure environments
    static,

    -- * This module's Static declaration
    declareStatic

    -- * Tutorial on safe Closure construction
    -- $Tutorial
  ) where

import Prelude
import Control.DeepSeq (NFData, deepseq, force)
import Data.Binary (Binary)
import Data.Constraint (Dict(Dict))
import Data.Monoid (mconcat)
import Data.Serialize (Serialize)

import Control.Parallel.HdpH.Closure.Internal   -- re-export whole module
import Control.Parallel.HdpH.Closure.Static
       (Static, unstatic, StaticDecl, declare, register, showStaticTable)



-- [Constructing families of Closures]
--
-- A problem arises with Closures whose type is not only polymorphic (as eg.
-- the type of @idC@ above) but also constrained by type classes. The problem
-- is that the class constraint has to be resolved statically, as there
-- is no simple way of attaching implicit dictionaries to a Closure (because
-- these dictionaries would have to be serialised as well). However, there is
-- a way of safely constructing such constrained Closures. The idea is to
-- actually construct a family of Closures, indexed by source locations.
-- The indices are produced by certain type class instances, effectively
-- turning the source location-based into a type-based indexing. We illustrate
-- this method on two examples.
--
-- The first example is the function @toClosure@ which converts any suitable
-- value into a Closure, fully forcing the value upon serialisation. The
-- /suitable/ values are those whose type is an instance of both classes
-- @'Data.Serialize.Serialize'@ and @'Control.DeepSeq.NFData'@, so one would
-- expect @toClosure@ to have the  following implementation and @'Static'@
-- declaration:
--
-- > toClosure :: (NFData a, Serialize a) => a -> Closure a
-- > toClosure val = $(mkClosure [| id val |])
-- >
-- > declare $(static 'id)
--
-- However, this does not compile - the last line complains from an ambiguous
-- type variable @a@ in the constrait @(NFData a, Serialize a)@.
--
-- The solution is to define a new type class @ToClosure@ as a subclass of
-- @'Data.Serialize.Serialize'@ and @'Control.DeepSeq.NFData'@, and use
-- instances of @ToClosure@ to index a family of Closures. The indexing is
-- done by @locToClosure@, the only member of class @ToClosure@, as follows.
--
-- > class (NFData a, Serialize a) => ToClosure a where
-- >   locToClosure :: LocT a
-- >
-- > toClosure :: (ToClosure a) => a -> Closure a
-- > toClosure val = $(mkClosureLoc [| toClosure_abs val |]) locToClosure
-- >
-- > toClosure_abs :: a -> Thunk a
-- > toClosure_abs val = Thunk val
--
-- Note that the above splice @$(mkClosureLoc [|id val|])@ generates a family
-- of type @'LocT' a -> 'Closure' a@. Applying that family to the index
-- @locToClosure@ yields the actual Closure. On the face of it, indexing
-- appears to be location based, but actually the family generated by
-- @'mkClosureLoc'@ never evaluates the index; thanks to the phantom type
-- argument of @'LocT'@ the indexing is really done statically on the types.
-- (Though the location information is necessary to tag the associated
-- @'Static'@ dictionaries, and will be evaluated when constructing the
-- @'Static'@ table.)
--
-- What this achieves is reducing the constraint on @toClosure@ from
-- @'Data.Serialize.Serialize'@ and @'Control.DeepSeq.NFData'@ to @ToClosure@.
-- Hence @toClosure@ is only available for types for which the programmer
-- explicitly instantiates  @ToClosure@. These instances are very simple,
-- see the following two samples.
--
-- > instance ToClosure Int where locToClosure = $(here)
-- > instance ToClosure (Closure a) where locToClosure = $(here)
--
-- Note that the two instances are entirely identical, in fact all instances
-- of @ToClosure@ must be identical. In particular, instances must not
-- be recursive, so that the number of types instantiating @ToClosure@
-- matches exactly the number of instance declarations in the source code.
-- (Aside: Non-recursiveness of @ToClosure@ instances is not currently
-- enforced. It may, and should, be enforced by a Template Haskell macro.)
-- All an instance does is record its location in the source code via
-- @locToClosure@, making @locToClosure@ a key for @ToClosure@ instances.
--
-- The programmer must declare the @'Static'@ dictionaries associated with
-- the above family of Closures. More precisely, she must declare one
-- dictionary per @ToClosure@ instance. This is done by defining a
-- family of @'Static'@ dictionaries, similar to the family of Closures.
--
-- > type StaticToClosure a = Static (CDict a a)
-- >
-- > staticToClosure :: (ToClosure a) => StaticToClosure a
-- > staticToClosure = $(staticLoc 'toClosure_abs) locToClosure
--
-- Note that the splice @$(staticLoc 'toClosure_abs)@ above generates a family 
-- of type @'LocT' a -> 'Static' ('CDict' a a)@. Applying that family to the 
-- index @locToClosure@ yields an actual @'Static'@ dictionary. The type synomyn
-- @StaticToClosure@ is a mere convenience to improve readability, as will
-- become evident when actually declaring the @'Static'@ dictionaries
-- (particularly in the second example).
--
-- It remains to actually declare the @'Static'@ dictionaries, one per instance
-- of @ToClosure@. Since @'Static'@ declarations form a monoid, the programmer
-- can simply concatenate all these declarations. So, given the above sample
-- @ToClosure@ instances, the programmer would declare the associated
-- dictionaries as follows.
--
-- > Data.Monoid.mconcat
-- >   [declare (staticToClosure :: StaticToClosure Int),
-- >    declare (staticToClosure :: forall a . StaticToClosure (Closure a))]
--
--
-- The second example of families of Closures is @forceCC@, which wraps
-- @forceC@ into a Closure, where @forceC@ is defined as follows:
--
-- > forceC :: (ToClosure a) => Strategy (Closure a)
-- > forceC clo = return $! forceClosure clo
--
-- Note that @forceC@ lifts @'forceClosure'@ to a /strategy/ in the @Par@
-- monad; please see module @Control.Parallel.HdpH.Strategies@ for the
-- definition of the @Strategy@ type constructor.
--
-- Ideally, @forceCC@ would be implemented simply like this:
--
-- > forceCC :: (ToClosure a) => Closure (Strategy (Closure a))
-- > forceCC = $(mkClosure [|forceC|])
--
-- However, the type class constraint demands that @forceCC@ generate a family
-- of Closures; in fact, it must generate a family of /static/ Closures because 
-- @forceC@, the expression quoted in the Template Haskell splice, is a single
-- toplevel variable; the absence of @Thunk@ in the type of @forceC@ is another
-- hint at static Closure. The actual implementation of @forceCC@ is as follows:
--
-- > class (ToClosure a) => ForceCC a where
-- >   locForceCC :: LocT (Strategy (Closure a))
-- >
-- > forceCC :: (ForceCC a) => Closure (Strategy (Closure a))
-- > forceCC = $(mkClosureLoc [| forceC |]) locForceCC
--
-- Above is the first part of the code, the definition of a new type class
-- @ForceCC@ and the defintion of @forceCC@ itself. Note the complex phantom
-- type argument of @locForceCC@; this is dictated by the splice
-- @$(mkClosureLoc [|forceC|])@ which is of type
-- @'LocT' (Strategy ('Closure' a)) -> 'Closure' (Strategy ('Closure' a))@.
-- Instances of class @ForceCC@ are very similar to instances of @ToClosure@,
-- cf. the following two samples.
--
-- > instance ForceCC Int where locForceCC = $(here)
-- > instance ForceCC (Closure a) where locForceCC = $(here)
--
-- Along with the family of Closures there is a family of @'Static'@
-- dictionaries, defined as follows. 
--
-- > type StaticForceCC a = Static (CDict () (Strategy (Closure a)))
-- >
-- > staticForceCC :: (ForceCC a) => StaticForceCC a
-- > staticForceCC = $(staticLoc 'forceC) locForceCC
--
-- It remains to actually declare the @'Static'@ dictionaries, one per
-- @ForceCC@ instance. Again, this is very similar to declaring the
-- dictionaries linked to @ToClosure@ instances. But note how the type
-- synonym @StaticForceCC@ improves readability of the declaration - just try
-- expanding the last line of the following declaration.
--
-- > Data.Monoid.mconcat
-- >   [declare (staticForceCC :: StaticForceCC Int),
-- >    declare (staticForceCC :: forall a . StaticForceCC (Closure a))]


-----------------------------------------------------------------------------
-- fully forcing Closures

-- | @forceClosure@ fully forces its argument, ie. fully normalises the
-- wrapped thunk. Importantly, @forceClosure@ also updates the closure
-- represention in order to persist normalisation (ie. in order to prevent
-- the closure returning to /unevaluated/ after being serialised and
-- transmitted over the network).
--
-- Note that @forceClosure clo@ does not have the same effect as
-- @'Control.DeepSeq.force' clo@ (because @forceClosure@ updates the closure
-- representation).
-- However, @forceClosure clo@ is essentially the same as
-- @'Control.DeepSeq.force' $ pure $ unClosure clo@.
--
forceClosure :: ToClosure a -> Closure a -> Closure a
forceClosure tc clo = force $ toClosure tc $ unClosure clo


-- Note that it does not make sense to construct a variant of @forceClosure@
-- that would evaluate the thunk inside a Closure head-strict only. The reason 
-- is that serialising such a Closure would turn it into a fully forced one.


{-
-- NOTE: This should work!
type Strategy a = a -> IO a

forceC :: ToClosure a -> Strategy (Closure a)
forceC sp clo = return $! forceClosure sp clo

forceCC :: ToClosure a -> Closure (Strategy (Closure a))
forceCC tc = $(mkClosure [|forceCC_abs tc|])

forceCC_abs tc = Thunk $ forceC tc

tcInt = mkToClosure :: ToClosure Int
tcClosureIntList = mkToClosure :: ToClosure (Closure [Int])
tcStaticInt = mkToClosure :: ToClosure (Static Int)
-}

------------------------------------------------------------------------------
-- Categorical operations on function Closures

-- | Identity arrow wrapped in a Closure.
idC :: Closure (a -> a)
idC = $(mkClosure [| id |])


-- | Terminal arrow wrapped in a Closure.
termC :: Closure (a -> ())
termC = $(mkClosure [| constUnit |])

{-# INLINE constUnit #-}
constUnit :: a -> ()
constUnit = const ()


-- | Composition of function Closures.
compC :: Closure (b -> c) -> Closure (a -> b) -> Closure (a -> c)
compC clo_g clo_f = $(mkClosure [| compC_abs (clo_g, clo_f) |])

{-# INLINE compC_abs #-}
compC_abs :: (Closure (b -> c), Closure (a -> b)) -> Thunk (a -> c)
compC_abs (clo_g, clo_f) = Thunk (unClosure clo_g . unClosure clo_f)


-- | Application of a function Closure to another Closure.
-- Behaves like the @ap@ operation of an applicative functor.
apC :: Closure (a -> b) -> Closure a -> Closure b
apC clo_f clo_x = $(mkClosure [| apC_abs (clo_f, clo_x) |])

{-# INLINE apC_abs #-}
apC_abs :: (Closure (a -> b), Closure a) -> Thunk b
apC_abs (clo_f, clo_x) = Thunk (unClosure clo_f $ unClosure clo_x)


-------------

-- TODO: 'toClosureCD :: Closure (Dict (...)) -> a -> Closure a' as a generic
-- variant of 'toClosure' that stores a Closure with a Dict (uses more space).

cdInt = $(mkClosure [| dInt |])
dInt = Dict :: Dict (NFData Int, Binary Int, Serialize Int)

cdClosureInt = $(mkClosure [| dClosureInt |])
dClosureInt = Dict :: Dict (NFData (Closure Int), Binary (Closure Int), Serialize (Closure Int))

toClosureCD :: Closure (Dict (NFData a, Binary a, Serialize a)) -> a -> Closure a
toClosureCD cd x = undefined

-- NOTE: This cannot work because it would require that normalisation
--       and serialisation change their behaviour dependent on part
--       of the environment, ie. in a sense we'd need some kind of
--       dependent pair type for the env, instead of just a pair.
--
--       The old style CDict type would have allowed this, though.
--
--       We can still use the Dict trick to cope with overloading,
--       and then use `apC` to compose (provided both args are Closures).
--       We just can't use the closured Dict to change the 
--       behaviour of Closures themselves.
--
--       Making Dict Static will often be sufficient.


-----------------------------------------------------------------------------
-- Static declaration (must be at end of module)

-- Empty splice; TH hack to make all environment abstractions visible.
$(return [])

declareStatic :: StaticDecl
declareStatic = mconcat
  [declare $(static 'id),
   declare $(static 'constUnit),
   declare $(static 'compC_abs),
--   declare $(static 'dInt),
--   declare $(static 'dClosureInt),
--   declare $(static 'forceCC_abs),
--   declare tcInt,
--   declare tcClosureIntList,
--   declare tcStaticInt,
   declare $(static 'apC_abs)]
