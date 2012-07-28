-- Closure representation, inspired by [1] and refined by [2].
--   [1] Epstein et al. "Haskell for the cloud". Haskell Symposium 2011.
--   [2] Maier, Trinder. "Implementing a High-level Distributed-Memory
--       Parallel Haskell in Haskell". IFL 2011.
--
-- Visibility: public
-- Author: Patrick Maier <P.Maier@hw.ac.uk>
-- Created: 12 May 2011
--
-----------------------------------------------------------------------------

-- This module implements /explicit closures/ as described in [2].
-- It makes extensive use of Template Haskell, and due to TH's stage
-- restrictions, internals like the actual closure representation are
-- delegated to module 'HdpH.Closure.Internal'.

{-# LANGUAGE ScopedTypeVariables #-}  -- for type annotations in Static decl
{-# LANGUAGE TemplateHaskell #-}

module HdpH.Closure
  ( -- * source locations with phantom type attached
    LocT,             -- instances: Eq, Ord, Show
    here,             -- :: ExpQ

    -- * serialised environment
    Env,              -- synonym: Data.ByteString.Lazy.ByteString
    encodeEnv,        -- :: (Serialize a) => a -> Env
    decodeEnv,        -- :: (Serialize a) => Env -> a

    -- * 'Static' type constructor
    Static,           -- instances: Eq, Ord, Show, NFData, Serialize

    -- * Static declaration and registration
    StaticDecl,       -- instances: Monoid, Show
    declare,          -- :: Static a -> StaticDecl
    register,         -- :: StaticDecl -> IO ()
    showStaticTable,  -- :: [String]

    -- * explicit 'Closure' type constructor
    Closure,          -- instances: Show, NFData, Serialize

    -- * unsafe Closure introduction
    unsafeMkClosure,  -- :: a -> Static (Env -> a) -> Env -> Closure a

    -- * eliminating Closures
    unClosure,        -- :: Closure a -> a

    -- * Template Haskell macros for safe Closure construction
    mkClosure,        -- :: ExpQ -> ExpQ
    mkClosureLoc,     -- :: ExpQ -> ExpQ

    -- * macros for declaring Static deserializers implicit in Closure constr
    static,           -- :: Name -> ExpQ
    staticLoc,        -- :: Name -> ExpQ
    static_,          -- :: Name -> ExpQ
    staticLoc_,       -- :: Name -> ExpQ

    -- * Closure conversion for values
    toClosure,        -- :: (ToClosure a) => a -> Closure a
    ToClosure(        -- context: Serialize
      locToClosure      -- :: LocT a
    ),
    StaticToClosure,  -- * -> *; synonym: Static (Env -> _ )
    staticToClosure,  -- :: (ToClosure a) => StaticToClosure a

    -- * categorical operations on function Closures
    idC,    -- :: Closure (a -> a)
    termC,  -- :: Closure (a -> ())
    compC,  -- :: Closure (b -> c) -> Closure (a -> b) -> Closure (a -> c)
    apC,    -- :: Closure (a -> b) -> Closure a -> Closure b

    -- * Static decl for 'idC', 'termC, 'compC', 'apC' and 'ToClosure' inst
    declareStatic     -- :: StaticDecl
  ) where

import Prelude
import Data.Monoid (mconcat)
import Data.Serialize (Serialize)

import HdpH.Closure.Internal   -- re-export whole module
import HdpH.Closure.Static
       (Static, StaticDecl, declare, register, showStaticTable)


-----------------------------------------------------------------------------
-- Key facts about explicit closures
--
-- * An /explicit closure/ is a term of type 'Closure t', wrapping a thunk
--   of type 't'. Henceforth, we will write /Closure/ (capitalised) to mean 
--   an explicit Closure, and /closure/ (in lower case) to mean an ordinary
--   Haskell closure (ie. a thunk).
--
-- * The 'Closure' type constructor is abstract (see reference [2] or
--   module 'HdpH.Closure.Internal for its internal /dual/ representation).
--
-- * The function 'unClosure' returns the thunk wrapped in a Closure.
--
-- * The function 'unsafeMkClosure' constructs Closures but is considered
--   *unsafe* because it exposes the internal dual representation, and
--   relies on the programmer to guarantee consistency.
--
-- * [2] proposes a *safe* Closure construction '$(mkClosure [|...|])' where
--   the '...' is an arbitrary thunk. Due to current limitations of the
--   GHC (namely missing native support for the 'Static' type constructor),
--   this module provides only limited variants of '$(mkClosure [|...|])'.
--   The restrictions on the the thunk '...' are
--   * Either '...' is a toplevel variable (also called a /static closure/),
--   * or '...' is a toplevel variable (also called a /closure abstraction/)
--     applied to a tuple of local variables.
--
-- * As a matter of nomenclature, functions operating on Closures will either
--   contain the string 'Closure' (like 'unClosure' and 'mkClosure') or will
--   end in the suffix 'C' (like the categorical operations 'idC' and 'compC').
--
-- * Here are a few identities involving Closures:
--   (1) forall x fun env . unClosure $ unsafeMkClosure x fun env == x
--
--   (2) forall x . unClosure $ toClosure x == x
--
--   (3) forall stat_clo . unClosure $(mkClosure [| stat_clo |]) == stat_clo
--
--   (4) forall clo_abs free_vars .
--         unClosure $(mkClosure [| clo_abs free_vars |]) == clo_abs free_vars


-----------------------------------------------------------------------------
-- Tutorial on safe construction of explicit Closures
--
-- This guide will demonstrate the safe construction of explicit Closures
-- in HdpH through a series of examples.
--
--
-- CONSTRUCTING PLAIN CLOSURES
--
-- The first example is the definition of the Closure transformation
-- 'apC'. It is defined in [2] (where it is called 'mapClosure') by eliminating
-- the Closures of its arguments, and constructing a new Closure of the the
-- resulting application, as follows:
--
-- > apC :: Closure (a -> b) -> Closure a -> Closure b
-- > apC clo_f clo_x =
-- >   $(mkClosure [|unClosure clo_f $ unClosure clo_x|])
--
-- If 'Static' were fully supported by GHC then 'mkClosure' would abstract
-- the free local variables (here 'clo_f' and 'clo_x') in its quoted argument,
-- serialise a tuple of these variables, and construct a suitable Static
-- deserialiser. In short, the expanding the above Template Haskell splice
-- would yield the following definition:
--
-- > apC clo_f clo_x =
-- >   let thk = unClosure clo_f $ unClosure clo_x
-- >       env = encodeEnv (clo_f, clo_x)
-- >       fun = static $ \ env -> let (clo_f, clo_x) = decodeEnv env
-- >                                 in unClosure clo_f $ unClosure clo_x
-- >     in unsafeMkClosure thk fun env
--
-- However, the current implementation of 'mkClosure' cannot do this because
-- there is no term former 'static'. Instead, the current implementation
-- of 'mkClosure' expects its quoted argument to be in a one of two special
-- forms: either it is a single /toplevel/ variable, or an application of a
-- /toplevel/ variable to a tuple of free variables. To distinguish the two
-- forms, Closures constructed from single toplevel variables will be called
-- /static/ (because like static terms they do not capture any variables).
--
-- Here is the definition of 'apC' as an example of how to construct a
-- general, non-static Closure.
--
-- > apC clo_f clo_x =
-- >   $(mkClosure [|apC_abs (clo_f, clo_x)|])
-- >
-- > apC_abs :: (Closure (a -> b), Closure a) -> b
-- > apC_abs (clo_f, clo_x) = unClosure clo_f $ unClosure clo_x
--
-- First, the programmer must manually define a toplevel /closure abstraction/
-- 'apC_abs' which abstracts a tuple '(clo_f, clo_x)' of free local variables
-- occuring in the expression to be converted into a Closure. (Usually, the
-- programmer would want the closure abstraction 'apC_abs' to be inlined.)
--
-- Second, the programmer constructs the explicit Closure via a Template 
-- Haskell splice '$(mkClosure [|...|])', where the quoted expression
-- 'apC_abs (clo_f, clo_x)' is an application of the toplevel closure
-- abstraction to a tuple of free local variables. It is important
-- that this actual tuple of variables matches *exactly* the formal tuple
-- in the definition of the closure abstraction. In fact, best practice is
-- for the quoted expression to textually match the left-hand side of the
-- definition of the closure abstraction.
--
-- Finally, a Static deserialiser corresponding to the toplevel closure
-- abstraction must be declared. To this end, this module exports a Template
-- Haskell function 'static' which converts the name of a toplevel closure
-- abstraction into a Static deserialiser, and a function 'declare' which
-- turns the Static deserialiser into a Static declaration. These should be
-- used as follows; see also the definition of 'declareStatic' below.
--
-- > declare $(static 'apC_abs)
--
--
-- The second example is the definition of the static Closure 'idC', which
-- lifts the identity function to a function Closure.
-- The definition is a follows; see also code towards the end of this file.
--
-- > idC :: Closure (a -> a)
-- > idC = $(mkClosure [|id|])
--
-- The expression to be converted into a Closure, the variable 'id', does not
-- contain any free local variables, so there is no need to abstract a tuple
-- of free variables. In fact, the expression is already a toplevel variable,
-- so there is also no need to define a new toplevel closure abstraction either.
-- Thus, the programmer constructs the static Closure via the the Template
-- Haskell splice '$(mkClosure [|...|])', where the quoted expression 'id'
-- is a toplevel variable.
--
-- The programmer must also declare a Static deserialiser corresponding to
-- the toplevel variable of which the static Closure was created. This is done
-- as follows; see also the definition of 'declareStatic' below. 
--
-- > declare $(static_ 'id)
--
-- Note the use of 'static_' instead of 'static' to create a Static deserialiser
-- for a *static* Closure, ie. a Static deserialiser that does not actually
-- deserialise any free variables. Accidentally mixing up 'static' and 'static_'
-- will be caught by the type checker, but will likely result in unintelligible
-- error messages.
--
--
-- CONSTRUCTING FAMILIES OF CLOSURES
--
-- A problem arises with Closures whose type is not only polymorphic (as eg.
-- the type of 'idC' above) but also constrained by type classes. The problem
-- here is that the class constraint has to be resolved statically, as there
-- is no way of attaching implicit dictionaries to a Closure (because these
-- dictionaries would have to be serialised as well). However, there is a
-- way of safely constructing such constrained Closures. The idea is to
-- actually construct a family of Closures, indexed by source locations.
-- The indices are produced by certain type class instances, effectively
-- turning the location-based into a type-based indexing. We illustrate
-- this method on two examples.
--
-- The first example is the function 'toClosure' which converts any suitable
-- value into a Closure, forcing the value upon serialisation. The *suitable*
-- values are those whose type is an instance of class 'Serialize', so one
-- would expect 'toClosure' to have the following implementation and Static
-- declaration:
--
-- > toClosure :: (Serialize a) => a -> Closure a
-- > toClosure val = $(mkClosure [| id val |])
-- >
-- > declare $(static 'id)
--
-- However, this does not compile - the last line complains from an ambiguous
-- type variable 'a' in the constrait 'Serialize a'.
--
-- The solution is to define a new type class 'ToClosure' as a subclass of
-- the given 'Serialize' constraint, and use instances of 'ToClosure' to
-- index a family of Closures. The indexing is done by 'locToClosure', the only
-- member of class 'ToClosure', as follows.
--
-- > class (Serialise a) => ToClosure a where
-- >   locToClosure :: LocT a
-- >
-- > toClosure :: (ToClosure a) => a -> Closure a
-- > toClosure val = $(mkClosureLoc [| id val |]) locToClosure
--
-- Note that the above splice '$(mkClosureLoc [|...|])' generates a family
-- of type 'LocT a -> Closure a'. Applying that family to the index
-- 'locToClosure' yields the actual Closure. On the face of it, indexing
-- appears to be location based, but actually the family generated by
-- 'mkClosureLoc' never evaluates the index; thanks to the phantom type
-- argument of 'LocT' the indexing is really done statically on the types.
-- (Though the location information is necessary to tag the associated
-- Static deserialisers, and will be evaluated when constructing the
-- Static table.)
--
-- What this achieves is reducing the constraint on 'toClosure' from
-- 'Serialize' to 'ToClosure'. Hence 'toClosure' is only available for types
-- for which the programmer explicitly instantiates 'ToClosure'. These
-- instances are very simple, see the following two samples.
--
-- > instance ToClosure Int where locToClosure = $(here)
-- > instance ToClosure (Closure a) where locToClosure = $(here)
--
-- Note that the two instances are entirely identical, in fact all instances
-- of 'ToClosure' must be identical. In particular, instances must not
-- be recursive, so that the number of types instantiating 'ToClosure'
-- matches exactly the number of instance declarations in the source code.
-- All an instance does is record its location in the source code via
-- 'locToClosure', making 'locToClosure' a key for 'ToClosure' instances.
--
-- The programmer must declare the Static deserialisers associated with
-- the above family of Closures. More precisely, she must declare one
-- deserialiser per 'ToClosure' instance. This is done by defining a
-- family of Static deserialisers, similar to the family of Closures.
--
-- > type StaticToClosure a = Static (Env -> a)
-- >
-- > staticToClosure :: (ToClosure a) => StaticToClosure a
-- > staticToClosure = $(staticLoc 'id) locToClosure
--
-- Note that the splice '$(staticLoc [|...|])' above generates a family of
-- type 'LocT a -> Static (Env -> a)'. Applying that family to the index
-- 'locToClosure' yields an actual Static deserialiser. The type synomyn
-- 'StaticToClosure' is there to improve readability, as will become evident
-- when actually declaring the Static deserialisers (particularly in the
-- second example).
--
-- It remains to actually declare the Static deserialisers, one per instance
-- of 'ToClosure'. Since Static declarations form a monoid, the programmer
-- can simply concatenate all these declarations. So, given the above sample
-- 'ToClosure' instances, the programmer would declare the associated
-- deserialisers as follows.
--
-- > Data.Monoid.mconcat
-- >   [declare (staticToClosure :: StaticToClosure Int),
-- >    declare (staticToClosure :: forall a . StaticToClosure (Closure a))]
--
--
-- The second example of families of Closures is 'forceCC' which wraps
-- 'forceC :: (NFData a, ToClosure a) => Strategy (Closure a)' into a Closure
-- and should have the following signature.
--
-- > forceCC :: (NFData a, ToClosure a) => Closure (Strategy (Closure a))
--
-- Please see module 'HdpH.Strategies' for a definitions 'forceC' and the
-- 'Strategy' type constructor. Because of the type class constraint, 'forceCC'
-- must generate a family of Closures; in fact, it must generate a family of
-- *static* Closures because 'forceC' is a single toplevel variable.
--
-- > class (NFData a, ToClosure a) => ForceCC a where
-- >   locForceCC :: LocT (Strategy (Closure a))
-- >
-- > forceCC :: (ForceCC a) => Closure (Strategy (Closure a))
-- > forceCC = $(mkClosureLoc [| forceC |]) locForceCC
--
-- Above is the first part of the code, the definition of a new type class
-- 'ForceCC' and the defintion of 'forceCC' itself. Note the complex phantom
-- type argument of 'locForceCC'; this is dictated by the splice
-- '$(mkClosureLoc [|forceC|])' which is of type
-- 'LocT (Strategy (Closure a)) -> Closure (Strategy (Closure a))'.
-- Instances of class 'ForceCC' are very similar to instances of 'ToClosure',
-- cf. the following two samples.
--
-- > instance ForceCC Int where locForceCC = $(here)
-- > instance ForceCC (Closure a) where locForceCC = $(here)
--
-- Along with the family of Closures there is a family of deserialisers,
-- defined as follows. Note the use of 'staticLoc_' because 'forceCC'
-- generates a family of *static* Closures.
--
-- > type StaticForceCC a = Static (Env -> Strategy (Closure a))
-- >
-- > staticForceCC :: (ForceCC a) => StaticForceCC a
-- > staticForceCC = $(staticLoc_ 'forceC) locForceCC
--
-- It remains to actually declare the Static deserialisers, one per 'ForceCC'
-- instance. Again, this is very similar to declaring the deserialisers linked
-- to 'ToClosure' instances. But note how the type synonym 'StaticForceCC'
-- improves readability of the declaration - just try expanding the last line
-- of the declaration.
--
-- > Data.Monoid.mconcat
-- >   [declare (staticForceCC :: StaticForceCC Int),
-- >    declare (staticForceCC :: forall a . StaticForceCC (Closure a))]
--
--
-- REGISTERING STATIC DESERIALISERS
--
-- All Static deserialisers need to be declared, as shown above.
-- By convention, each module must concatenate all these Static declarations
-- (including declarations in imported modules) into a single Static
-- declaration. The 'Main' module, finally, must /register/ its Static
-- declaration (which by convention includes all declarations in imported
-- modules) at the beginning of the 'main' function, to create the Static table.
-- This section shows how to concatenate and register Static declarations.
--
-- First, concatenating Static declarations. As shown above, a Static
-- deserialiser can be turned into a Static declaration (of type 'StaticDecl')
-- by applying 'declare'. The type 'StaticDecl' is an instance of class
-- 'Monoid' (from module 'Data.Monoid'), so Static declarations can be
-- concatenated via the 'mconcat' operation. In fact, 'StaticDecl' is not
-- just a monoid but a commutative and idempotent monoid, thanks to the
-- way Static deserialisers are constructed. That means that Static
-- declarations can be concatenated repeatedly and in any order without
-- changing the result.
--
-- As a matter of convention, every module which constructs explicit Closures
-- must export a term 'declareStatic :: StaticDecl', a comprehensive declaration
-- of all the module's Static deserialisers, including all Static deserialisers
-- of imported modules. The latter is required because the module may call 
-- imported functions which in turn depend on their modules' Static
-- deserialisers.
--
-- > declareStatic :: StaticDecl
-- > declareStatic = Data.Monoid.mconcat
-- >   [declare $(static_ 'id),
-- >    declare $(static_ 'constUnit),
-- >    declare $(static 'compC_abs),
-- >    declare $(static 'apC_abs),
-- >    declare (staticToClosure :: forall a . StaticToClosure (Closure a))]
--
-- Above is a simple example of such a comprehensive Static declaration,
-- the declaration of this module. Note that the order of the list argument
-- to 'mconcat' does not matter because because 'StaticDecl' is a commutative
-- monoid. Below is a more complex example, taken from 'TEST/HdpH/sumeuler.hs'.
--
-- > declareStatic :: StaticDecl
-- > declareStatic = Data.Monoid.mconcat
-- >   [HdpH.declareStatic,
-- >    HdpH.Strategies.declareStatic,
-- >    declare (staticToClosure :: StaticToClosure Int),
-- >    declare (staticToClosure :: StaticToClosure [Int]),
-- >    declare (staticToClosure :: StaticToClosure Integer),
-- >    declare (staticForceCC :: StaticForceCC Integer),
-- >    declare $(static 'spark_sum_euler_abs),
-- >    declare $(static_ 'sum_totient),
-- >    declare $(static_ 'totient)]
--
-- This declaration does concatenate the Static declarations of two imported
-- modules, 'HdpH' and 'HdpH.Strategies'. Actually, 'HdpH.Strategies' also
-- imports 'HpdH', so 'HdpH.Strategies.declareStatic' already concatenates
-- 'HdpH.declareStatic', and so the above call to 'HdpH.declareStatic'
-- could be omitted. However, that omission is only justified in the
-- knowledge of implementation details of 'HdpH.Strategies' (namely its
-- import graph). The convention that Static declarations of *all* imported
-- modules must be concatenated, regardless of whether they are transitively
-- concatenated by other imported modules, is more robust; the resulting
-- Static declaration is the same anyway, thanks to 'StaticDecl' being
-- an idempotent monoid.
--
-- Here is general rule for how to deal with imports and Static declarations:
-- If module 'M1' imports another module 'M2', and
-- if 'M2' exports a variable 'declareStatic :: StaticDecl'
-- then 'M1' must export its own 'declareStatic :: StaticDecls',
-- which needs to concatenate 'M2.declareStatic' (amongst other declarations).
-- Note that the rule applies even if 'M1' ostensibly imports "nothing"
-- from 'M2', eg. 'M1' imports 'M2' with the clause 'import M2 ()'. For
-- this clause may still import instance declarations from 'M2', and those
-- instances may depend on M2's Static deserialisers.
--
--
-- Finally, registering a Static declaration. This must be done exactly once
-- in the 'Main' module, right at the start of 'main', by calling the function
-- 'register' on the 'Main' module's comprehensive Static declaration.
-- This is shown in sample code below, taken from 'TEST/HdpH/sumeuler.hs'.
-- Note that any actions executed prior to 'register' (ie. the 'hSetBuffering'
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

-- 'ToClosure' instance for Closures, only recording indexing location;
-- all instances of 'ToClosure' look like this.
instance ToClosure (Closure a) where locToClosure = $(here)

declareStatic :: StaticDecl
declareStatic = mconcat
  [declare $(static_ 'id),
   declare $(static_ 'constUnit),
   declare $(static 'compC_abs),
   declare $(static 'apC_abs),
   declare (staticToClosure :: forall a . StaticToClosure (Closure a))]
     -- Declaration of indexed Static deserialiser for 'toClosure';
     -- all declarations of 'staticToClosure' look like this.


-----------------------------------------------------------------------------
-- Closure conversion for values

-- Converting a value into a Closure, forcing the value on serialisation.
-- This is done by turning the identify function into a location-indexed
-- family of closure abstractions, yet only for types that instantiate
-- the class 'ToClosure', so that the locations of 'ToClosure' instances
-- serve as indices.
toClosure :: (ToClosure a) => a -> Closure a
toClosure val = $(mkClosureLoc [| id val |]) locToClosure

-- Indexing class for 'toClosure', see above for a sample instance.
-- This class also records the context that 'toClosure' requires.
class (Serialize a) => ToClosure a where
  locToClosure :: LocT a

-- Type synonym for declaring the Static deserialisers required by 'toClosure',
-- see above for a sample declaration.
type StaticToClosure a = Static (Env -> a)

-- Static deserialisers required by 'toClosure', see above for a sample decl.
staticToClosure :: (ToClosure a) => StaticToClosure a
staticToClosure = $(staticLoc 'id) locToClosure


------------------------------------------------------------------------------
-- categorical operations on Closures

-- identity arrow wrapped in a Closure
idC :: Closure (a -> a)
idC = $(mkClosure [| id |])


-- terminal arrow wrapped in a Closure
termC :: Closure (a -> ())
termC = $(mkClosure [| constUnit |])

{-# INLINE constUnit #-}
constUnit :: a -> ()
constUnit = const ()


-- composition of function Closures
compC :: Closure (b -> c) -> Closure (a -> b) -> Closure (a -> c)
compC clo_g clo_f = $(mkClosure [| compC_abs (clo_g, clo_f) |])

{-# INLINE compC_abs #-}
compC_abs :: (Closure (b -> c), Closure (a -> b)) -> (a -> c)
compC_abs (clo_g, clo_f) = unClosure clo_g . unClosure clo_f


-- applying a function Closure to another Closure (without forcing anything)
apC :: Closure (a -> b) -> Closure a -> Closure b
apC clo_f clo_x = $(mkClosure [| apC_abs (clo_f, clo_x) |])

{-# INLINE apC_abs #-}
apC_abs :: (Closure (a -> b), Closure a) -> b
apC_abs (clo_f, clo_x) = unClosure clo_f $ unClosure clo_x
