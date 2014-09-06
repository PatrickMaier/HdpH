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

    -- ** Closure dictionary type constructor    
    CDict,

    -- ** Type constructor marking return type of closure abstractions
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
    toClosure,
    ToClosure(
      locToClosure
    ),
    StaticToClosure,
    staticToClosure,

    -- ** Safe Closure construction
    mkClosure,
    mkClosureLoc,
    LocT,
    here,

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

    -- ** Static Closure dictionaries
    -- | A @'Static'@ /closure dictionary/ is a term of type
    -- @'Static' ('CDict' env a)@, for some type @a@ and (existentially
    -- quantified) environment type @env@.
    static,
    staticLoc,

    -- * This module's Static declaration
    declareStatic

    -- * Tutorial on safe Closure construction
    -- $Tutorial
  ) where

import Prelude
import Control.DeepSeq (NFData, deepseq)
import Data.Monoid (mconcat)
import Data.Serialize (Serialize)

import Control.Parallel.HdpH.Closure.Internal   -- re-export whole module
import Control.Parallel.HdpH.Closure.Static
       (Static, StaticDecl, declare, register, showStaticTable)


-----------------------------------------------------------------------------
-- $KeyFacts
--
-- An /explicit Closure/ is a term of type @Closure t@, wrapping a thunk
-- of type @t@. Henceforth, we will write /Closure/ (capitalised) to mean 
-- an explicit Closure, and /closure/ (in lower case) to mean an ordinary
-- Haskell closure (ie. a thunk).
--
-- The @'Closure'@ type constructor is abstract (see module
-- 'Control.Parallel.HdpH.Closure.Internal' for its internal representation).
--
-- The function @'unClosure'@ returns the thunk wrapped in a Closure.
--
-- Article [2] proposes a Closure construction macro @$(mkClosure [|...|])@
-- where the @...@ is an arbitrary thunk. Due to current limitations of the
-- GHC (namely missing support for the @'Static'@ type constructor),
-- this module provides only limited variants of @$(mkClosure [|...|])@.
-- The restrictions on the the thunk @...@ are
--
-- * either @...@ is a toplevel variable (also called a /static closure/),
--
-- * or @...@ is a toplevel variable (also called a /closure abstraction/)
--   applied to a tuple of local variables.
--
-- As a matter of nomenclature, functions operating on Closures will either
-- contain the string @Closure@ (like @'unClosure'@ and @'mkClosure'@) or will
-- end in the suffix @C@ (like the categorical operations @'idC'@ and
-- @'compC'@).
--
-- Some identities involving Closures:
--
-- (1) @unClosure $ toClosure x = x@
--
-- (2) @unClosure $(mkClosure [| stat_clo |]) = stat_clo@
--
-- (3) @unClosure $(mkClosure [| clo_abs free_vars |]) = clo_abs free_vars@
--


-----------------------------------------------------------------------------
-- $Deserialisation
--
-- Deserialising a serialised value via the methods of class
-- @'Data.Binary.Binary'@ (or @'Data.Serialize.Serialize'@) is not type safe.
-- For instance, the compiler will happily assign type @Int -> Bool@ to
--
-- > decodeBool . encodeInt where
-- >     decodeBool = decode :: ByteString -> Bool
-- >     encodeInt  = encode :: Int -> ByteString
--
-- This type coercion will only fail at runtime, when (and if) the decoder for
-- @Bool@ stumbles over unexepected input values. Hence deserialising can be 
-- viewed as an assertion that the serialised byte string represents a value
-- of the type @decode@ expects to see. A well-written decoder will check
-- this assertion on deserialisation, and will abort with an error message
-- if the assertion fails.
--
-- HdpH treats Closure deserialisation similarly as an assertion that the
-- serialised byte string represents a value of the expected Closure type.
-- However, HdpH does not check whether the assertion holds. Instead it
-- subverts the type system via @unsafeCoerce@, with potentially disastrous
-- consequences (like seg faults) in cases where the assertion fails.
--
-- The up side of this design choice is a simpler Closure representation
-- without runtime type reflection.
-- The down side is that Closure deserialisation has to be treated with
-- /extreme care/.
-- HdpH, for instance, avoids the pitfalls of Closure deserialisation by
-- making sure only Closures of the fixed type @Closure (Par ())@ are
-- serialised and deserialised.


-----------------------------------------------------------------------------
-- $Tutorial
--
-- This guide will demonstrate the construction of explicit Closures
-- in HdpH through a series of examples.
--
--
-- [Constructing plain Closures]
--
-- The first example is the definition of the Closure transformation
-- @apC@. It is defined in [2] (where it is called @mapClosure@) by eliminating
-- the Closures of its arguments, and constructing a new Closure of the the
-- resulting application, as follows:
--
-- > apC :: Closure (a -> b) -> Closure a -> Closure b
-- > apC clo_f clo_x =
-- >   $(mkClosure [|unClosure clo_f $ unClosure clo_x|])
--
-- If @'Static'@ were fully supported by GHC then @'mkClosure'@ would abstract
-- the free local variables (here @clo_f@ and @clo_x@) in its quoted argument,
-- serialise a tuple of these variables, and construct a suitable @'Static'@
-- closure dictionary. In short, expanding the above Template Haskell splice
-- would yield the following definition:
--
-- > apC clo_f clo_x =
-- >   let env = (clo_f, clo_x)
-- >       dict = mkCDict $ \ env -> let (clo_f, clo_x) = env
-- >                                 in unClosure clo_f $ unClosure clo_x
-- >     in Closure (static dict) env
--
-- However, the current implementation of @'mkClosure'@ cannot do this because
-- there is no term former @static@. Instead, the current implementation
-- of @'mkClosure'@ expects its quoted argument to be in a one of two special
-- forms: either it is a single /toplevel/ variable, or an application of a
-- /toplevel/ variable to a tuple of free variables. To distinguish the two
-- forms, Closures constructed from single toplevel variables will be called
-- /static/ (because like static terms they do not capture any variables).
--
-- Here is the definition of @apC@ as an example of how to construct a
-- general, non-static Closure.
--
-- > apC clo_f clo_x =
-- >   $(mkClosure [|apC_abs (clo_f, clo_x)|])
-- >
-- > apC_abs :: (Closure (a -> b), Closure a) -> Thunk b
-- > apC_abs (clo_f, clo_x) = Thunk (unClosure clo_f $ unClosure clo_x)
--
-- First, the programmer must manually define a toplevel /closure abstraction/
-- @apC_abs@ which abstracts a tuple @(clo_f, clo_x)@ of free local variables
-- occuring in the expression to be converted into a Closure. Note that the
-- return type/value of the closure abstraction must be marked by the
-- type/value constructor @Thunk@. (Usually, the programmer would want the
-- closure abstraction @apC_abs@ to be inlined.)
--
-- Second, the programmer constructs the explicit Closure via a Template
-- Haskell splice @$(mkClosure [|apC_abs (clo_f, clo_x)|])@, where the quoted 
-- expression @apC_abs (clo_f, clo_x)@ is an application of the toplevel closure
-- abstraction to a tuple of free local variables. It is important
-- that this actual tuple of variables matches /exactly/ the formal tuple
-- in the definition of the closure abstraction. In fact, best practice is
-- for the quoted expression to textually match the left-hand side of the
-- definition of the closure abstraction.
--
-- Finally, a @'Static'@ dictionary corresponding to the toplevel closure
-- abstraction must be declared. To this end, this module exports a Template
-- Haskell function @'static'@ which converts the name of a toplevel closure
-- abstraction into a @'Static'@ dictionary, and a function @'declare'@ which
-- turns the @'Static'@ dictionary into a @'Static'@ declaration. These 
-- should be used as follows; see also the definition of @declareStatic@ below.
--
-- > declare $(static 'apC_abs)
--
--
-- The second example is the definition of the static Closure @idC@, which
-- lifts the identity function to a function Closure.
-- The definition is a follows; see also code towards the end of this file.
--
-- > idC :: Closure (a -> a)
-- > idC = $(mkClosure [|id|])
--
-- The expression to be converted into a Closure, the variable @'id'@, does not
-- contain any free local variables, so there is no need to abstract a tuple
-- of free variables. In fact, the expression is already a toplevel variable,
-- so there is also no need to define a new toplevel closure abstraction either.
-- Thus, the programmer constructs the static Closure via the the Template
-- Haskell splice @$(mkClosure [|id|])@, where the quoted expression @'id'@
-- is a toplevel variable.
--
-- The programmer must also declare a @'Static'@ dictionary corresponding to
-- the toplevel variable of which the static Closure was created. This is done
-- as follows; see also the definition of @declareStatic@ below. 
--
-- > declare $(static 'id)
--
-- Note the absence of the type constructor @Thunk@ in the type of @id@;
-- this absence signals that @id@ is a /static/ closure, hence expects
-- no environment.
--
--
-- [Constructing families of Closures]
--
-- A problem arises with Closures whose type is not only polymorphic (as eg.
-- the type of @idC@ above) but also constrained by type classes. The problem
-- is that the class constraint has to be resolved statically, as there
-- is no way of attaching implicit dictionaries to a Closure (because these
-- dictionaries would have to be serialised as well). However, there is a
-- way of safely constructing such constrained Closures. The idea is to
-- actually construct a family of Closures, indexed by source locations.
-- The indices are produced by certain type class instances, effectively
-- turning the source location-based into a type-based indexing. We illustrate
-- this method on two examples.
--
-- The first example is the function @toClosure@ which converts any suitable
-- value into a Closure, fully forcing the value upon serialisation. Thus the 
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
--
--
-- [Registering @'Static'@ dictionaries]
--
-- All @'Static'@ dictionaries need to be declared, as shown above.
-- By convention, each module must concatenate all such @'Static'@ declarations
-- (including declarations in imported modules) into a single @'Static'@
-- declaration. The @Main@ module, finally, must /register/ its @'Static'@
-- declaration (which by convention includes all declarations in imported
-- modules) at the beginning of the @main@ function, to create the @'Static'@
-- table. This section shows how to concatenate and register @'Static'@
-- declarations.
--
-- First, concatenating @'Static'@ declarations. As shown above, a @'Static'@
-- dictionary can be turned into a @'Static'@ declaration (of type
-- @'StaticDecl'@) by applying @'declare'@. The type @'StaticDecl'@ is an
-- instance of class @'Data.Monoid.Monoid'@, so @'Static'@ declarations can be
-- concatenated via the @'mconcat'@ operation. In fact, @'StaticDecl'@ is not
-- just a monoid but a commutative and idempotent monoid, thanks to the
-- way @'Static'@ dictionaries are constructed. That means that @'Static'@
-- declarations can be concatenated repeatedly and in any order without
-- changing the result.
--
-- As a matter of convention, every module which constructs explicit Closures
-- must export a term @declareStatic :: StaticDecl@, a comprehensive declaration
-- of all the module's @'Static'@ dictionaries, including all @'Static'@
-- dictionaries of imported modules. The latter is required because the module
-- may call imported functions which in turn depend on their modules'
-- @'Static'@ dictionaries.
--
-- > declareStatic :: StaticDecl
-- > declareStatic = Data.Monoid.mconcat
-- >   [declare $(static 'id),
-- >    declare $(static 'constUnit),
-- >    declare $(static 'compC_abs),
-- >    declare $(static 'apC_abs),
-- >    declare (staticToClosure :: forall a . StaticToClosure (Closure a)),
-- >    declare (staticToClosure :: forall a . StaticToClosure [Closure a]),
-- >    declare (staticToClosure :: forall a . StaticToClosure (Maybe (Closure a)))]
--
-- Above is a simple example of such a comprehensive Static declaration,
-- the declaration of this module. Note that the order of the list argument
-- to @'mconcat'@ does not matter because because @'StaticDecl'@ is a
-- commutative monoid. Below is a more complex example, taken from a HdpH
-- program.
--
-- > declareStatic :: StaticDecl
-- > declareStatic = Data.Monoid.mconcat
-- >   [Control.Parallel.HdpH.declareStatic,
-- >    Control.Parallel.HdpH.Strategies.declareStatic,
-- >    declare (staticToClosure :: StaticToClosure Int),
-- >    declare (staticToClosure :: StaticToClosure [Int]),
-- >    declare (staticToClosure :: StaticToClosure Integer),
-- >    declare (staticForceCC :: StaticForceCC Integer),
-- >    declare $(static 'spark_sum_euler_abs),
-- >    declare $(static 'sum_totient),
-- >    declare $(static 'totient)]
--
-- This declaration does concatenate the Static declarations of two imported
-- modules, @Control.Parllel.HdpH@ and @Control.Parallel.HdpH.Strategies@.
-- Actually, the latter also imports the former, so
-- @Control.Parallel.HdpH.Strategies.declareStatic@ already concatenates
-- @Control.Parallel.HdpH.declareStatic@, and so the above call to
-- @Control.Parallel.HdpH.declareStatic@ could be omitted. However, that
-- omission is only justified in the knowledge of implementation details of
-- @Control.Parallel.HdpH.Strategies@ (namely its import graph). The convention
-- that @'Static'@ declarations of /all/ imported modules must be concatenated,
-- regardless of whether they are transitively concatenated by other imported
-- modules, is more robust; the resulting @'Static'@ declaration is the same
-- anyway, thanks to @'StaticDecl'@ being an idempotent monoid.
--
-- A general rule how to deal with imports and @'Static'@ declarations:
-- If module @M1@ imports another module @M2@, and
-- if @M2@ exports a variable @declareStatic :: StaticDecl@
-- then @M1@ must export its own @declareStatic :: StaticDecl@,
-- which needs to concatenate @M2.declareStatic@ (amongst other declarations).
-- Note that the rule applies even if @M1@ ostensibly imports "nothing"
-- from @M2@, eg. @M1@ imports @M2@ with the clause @import M2 ()@. For
-- this clause may still import instance declarations from @M2@, and those
-- instances may depend on M2's @'Static'@ dictionaries.
--
--
-- Finally, registering a @'Static'@ declaration. This must be done exactly once
-- in the @Main@ module, right at the start of @main@, by calling the function
-- @'register'@ on the @Main@ module's comprehensive @'Static'@ declaration.
-- This is shown in sample code below, taken from a HdpH program.
-- Note that any actions executed prior to @'register'@ (ie. the @hSetBuffering@
-- calls) must not involve explicit Closures.
--
-- > main :: IO ()
-- > main = do
-- >   hSetBuffering stdout LineBuffering
-- >   hSetBuffering stderr LineBuffering
-- >   register declareStatic
-- >   ...


-----------------------------------------------------------------------------
-- Static declaration

-- 'ToClosure' instance for Closures, lists of Closures and option Closures;
-- these instances only record the indexing location; not that all instances
-- of 'ToClosure' look like this.
instance ToClosure (Closure a) where locToClosure = $(here)
instance ToClosure [Closure a] where locToClosure = $(here)
instance ToClosure (Maybe (Closure a)) where locToClosure = $(here)

declareStatic :: StaticDecl
declareStatic = mconcat
  [declare $(static 'id),
   declare $(static 'constUnit),
   declare $(static 'compC_abs),
   declare $(static 'apC_abs),
   declare (staticToClosure :: forall a . StaticToClosure (Closure a)),
   declare (staticToClosure :: forall a . StaticToClosure [Closure a]),
   declare (staticToClosure :: forall a . StaticToClosure (Maybe (Closure a)))]
     -- Declaration of indexed Static dictionaries for 'toClosure';
     -- note that all declarations of 'staticToClosure' look like this.


-----------------------------------------------------------------------------
-- Value Closure construction

-- | @toClosure x@ constructs a value Closure wrapping @x@, provided the
-- type of @x@ is an instance of class @'ToClosure'@.
-- Note that the serialised representation of the resulting Closure stores a
-- serialised representation (as per class @'Data.Serialize.Serialize'@) of
-- @x@, so serialising the resulting Closure will force @x@ (hence could be
-- costly). However, Closure construction itself is cheap.
toClosure :: (ToClosure a) => a -> Closure a
toClosure val = $(mkClosureLoc [| toClosure_abs val |]) locToClosure

{-# INLINE toClosure_abs #-}
toClosure_abs :: a -> Thunk a
toClosure_abs val = Thunk val


-- | Indexing class, recording types which support the @'toClosure'@ operation;
-- see the tutorial below for a more thorough explanation.
-- Note that @ToClosure@ is a subclass of @'Data.Serialize.Serialize'@
-- and @'Control.DeepSeq.NFData'@.
class (NFData a, Serialize a) => ToClosure a where
  -- | Only method of class @ToClosure@, recording the source location
  -- where an instance of @ToClosure@ is declared.
  locToClosure :: LocT a

-- | Type synonym for declaring the @'Static'@ dictionary required by
-- @'ToClosure'@ instances; see the tutorial below for a more thorough 
-- explanation.
type StaticToClosure a = Static (CDict a a)

-- | @'Static'@ dictionary required by a @'ToClosure'@ instance;
-- see the tutorial below for a more thorough explanation.
staticToClosure :: (ToClosure a) => StaticToClosure a
staticToClosure = $(staticLoc 'toClosure_abs) locToClosure


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
-- @ @'Control.DeepSeq.force' $ toClosure $ unClosure clo@.
--
forceClosure :: (NFData a, ToClosure a) => Closure a -> Closure a
forceClosure clo = x `deepseq` toClosure x where x = unClosure clo

-- Note that it does not make sense to construct a variant of @forceClosure@
-- that would evaluate the thunk inside a Closure head-strict only. The reason 
-- is that serialising such a Closure would turn it into a fully forced one.


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
