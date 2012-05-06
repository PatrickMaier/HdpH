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
-- delegated to module 'HdpH.Internal.Closure'.


{-# LANGUAGE ScopedTypeVariables #-}  -- req'd for phantom type annotations
{-# LANGUAGE TemplateHaskell #-}

module HdpH.Closure
  ( -- * serialised environment
    Env,              -- synonym: Data.ByteString.Lazy.ByteString
    encodeEnv,        -- :: (Serialize a) => a -> Env
    decodeEnv,        -- :: (Serialize a) => Env -> a

    -- * static deserializers
    StaticId(         -- context: Serialize, Typeable
      staticIdTD        -- :: a -> Static (Env -> a)
    ),

    -- * Closure type constructor
    Closure,          -- instances: Show, NFData, Serialize, Typeable

    -- * introducing and eliminating closures
    unsafeMkClosure,  -- :: a -> Static (Env -> a) -> Env -> Closure a
    unClosure,        -- :: Closure a -> a

    -- * Template Haskell macros for closure construction
    mkClosure,        -- :: ExpQ -> ExpQ
    mkClosureTD,      -- :: ExpQ -> ExpQ

    -- * macros for registering static deserializers used in closure cons
    static,           -- :: Name -> ExpQ
    staticTD,         -- :: Name -> ExpQ
    static_,          -- :: Name -> ExpQ
    staticTD_,        -- :: Name -> ExpQ

    -- * closure conversion for values
    toClosure,        -- :: (StaticId a) => a -> Closure a

    -- * categorical operations on function closures
    idClosure,   -- :: Closure (a -> a)
    compClosure, -- :: Closure (b -> c) -> Closure (a -> b) -> Closure (a -> c)
    mapClosure,  -- :: Closure (a -> b) -> Closure a -> Closure b

    -- * Static declaration and registration;
    --   exported to top-level module 'HdpH' but not re-exported by 'HdpH'
    registerStatic  -- :: IO ()
  ) where

import Prelude hiding (error)
import Control.DeepSeq (NFData, rnf)
import Data.Serialize (Serialize)
import Data.Typeable (Typeable)

import HdpH.Internal.Closure   -- re-export whole module
import HdpH.Internal.Static (Static, register)


-----------------------------------------------------------------------------
-- Key facts about explicit closures
--
-- * An /explicit closure/ is an expression of type 'Closure t', wrapping
--   a thunk of type 't'.
--
-- * The 'Closure' type constructor is abstract (see module
--   'HdpH.Internal.Closure or reference [2] for its internal /dual/
--   representation).
--
-- * The function 'unClosure' returns the thunk wrapped in an explicit closure.
--
-- * The function 'unsafeMkClosure' constructs explicit closures but is 
--   considered *unsafe* because it exposes the internal dual representation,
--   and relies on the programmer to guarantee consistency.
--
-- * [2] proposes a *safe* explicit closure construction '$(mkClosure [|...|])'
--   where the '...' is an arbitrary thunk. Due to current limitations of
--   the GHC (namely missing support for the 'Static' type constructor),
--   this module provides only limited variants of '$(mkClosure [|...|])',
--   where the thunk is either a toplevel variable, or where the thunk is
--   a toplevel variable applied to a tuple of local variables.


-----------------------------------------------------------------------------
-- User's guide to explicit closures
--
-- This guide will demonstrate the safe construction of explicit closures
-- in HdpH through a series of examples.
--
--
-- CONSTRUCTING PLAIN CLOSURES
--
-- The first example is the definition of the closure transformation
-- 'mapClosure'. In [2], 'mapClosure' is defined by eliminating the
-- explicit closures of its arguments, and constructing a new closure
-- of the the resulting application, as follows:
--
-- > mapClosure :: Closure (a -> b) -> Closure a -> Closure b
-- > mapClosure clo_f clo_x =
-- >   $(mkClosure [|unClosure clo_f $ unClosure clo_x|])
--
-- If 'Static' were fully supported by GHC then 'mkClosure' would abstract
-- the free local variables (here 'clo_f' and 'clo_x') in its quoted argument,
-- serialise a tuple of these variables, and construct a suitable /static/
-- deserialiser. In short, the above Template Haskell splice would expand to
-- the following definition:
--
-- > mapClosure clo_f clo_x = 
-- >   let thk = unClosure clo_f $ unClosure clo_x
-- >       env = encodeEnv (clo_f, clo_x)
-- >       fun = static (\env -> let (clo_f, clo_x) = decodeEnv env
-- >                               in unClosure clo_f $ unClosure clo_x)
-- >     in unsafeMkClosure thk fun env
--
-- However, the current implementation of 'mkClosure' cannot do this because
-- there is no term former 'static'. Instead, the current implementation
-- of 'mkClosure' expects its quoted argument to be in a one of two special
-- forms: either it is a single /toplevel/ variable, or an application of a
-- /toplevel/ variable to a tuple of free variables. To distinguish the two
-- forms, we will call explicit closures constructed from single toplevel
-- variables /static/.
--
-- We present the definition of 'mapClosure', which can also be found at the
-- end of this file, as an example of how to construct a general, non-static
-- explicit closure. The definition is as follows:
--
-- > mapClosure clo_f clo_x =
-- >   $(mkClosure [|mapClosure_abs (clo_f, clo_x)|])
-- >
-- > mapClosure_abs :: (Closure (a -> b), Closure a) -> b
-- > mapClosure_abs (clo_f, clo_x) = unClosure clo_f $ unClosure clo_x
--
-- First, the programmer must manually define a toplevel /closure abstraction/
-- 'mapClosure_abs' which abstracts a tuple '(clo_f, clo_x)' of free local
-- variables occuring in the expression to be converted into an explicit
-- closure. (Usually, the programmer would want the closure abstraction
-- 'mapClosure_abs' to be inlined.) 
--
-- Second, the programmer constructs the explicit closure via a Template 
-- Haskell splice '$(mkClosure [|...|])', where the quoted expression
-- 'mapClosure_abs (clo_f, clo_x)' is an application of the toplevel
-- closure abstraction to a tuple of free local variables. It is important
-- that this actual tuple of variables matches *exactly* the formal tuple
-- in the definition of the closure abstraction. In fact, best practice is
-- for quoted expression to textually match the left-hand side of the
-- definition of the closure abstraction.
--
-- Finally, a static deserialiser corresponding to the toplevel closure
-- abstraction must be registered. To this end, HdpH exports a function 
-- 'register' and this module exports a Template Haskell function 'static'
-- which converts the name of a toplevel closure abstraction into a
-- static deserialiser. The programmer should use this as follows;
-- see also the definition of 'registerStatic' below. 
--
-- > register $(static 'mapClosure_abs)
--
--
-- The second example is the definition of the static explicit closure
-- 'idClosure', which lifts the identity function to a function closure.
-- The definition is a follows; see also code towards the end of this file.
--
-- > idClosure :: Closure (a -> a)
-- > idClosure = $(mkClosure [|id|])
--
-- The expression to be converted into an explicit closure, the variable 'id',
-- does not contain any free local variables, so there is no need to
-- abstract a tuple of free variables. In fact, in this case the expression
-- is already a toplevel variable, so there is also no need to define a new
-- toplevel closure abstraction either. Thus, the programmer constructs the
-- static explicit closure via the the Template Haskell splice
-- '$(mkClosure [|...|])', where the quoted expression 'id' is a toplevel 
-- variable.
-- 
-- The programmer must also register a static deserialiser corresponding to
-- the toplevel variable out of which the static explicit closure was created.
-- This is done as follows; see also the definition of 'registerStatic' below. 
--
-- > register $(static_ 'id)
--
-- Note the use of 'static_' instead of 'static' to create a static
-- deserialiser for a *static* explicit closure, ie. a static deserialiser
-- that does not actually deserialise any free variables. Accidentally
-- mixing up 'static' and 'static_' will be caught by the type checker,
-- but will result in unintelligible error messages.
--
--
-- CONSTRUCTING FAMILIES OF CLOSURES DISCRIMINATED BY TYPE
--
-- The third example is the function 'toClosure' which converts any suitable
-- value into an explicit closure (which will force the value upon
-- serialisation). The *suitable* values are those whose type is an instance
-- of class 'StaticId' -- about which more later -- hence the actual explicit
-- closure contructed by 'toClosure' depends on the type of its argument.
-- Therefore, 'toClosure' uses 'mkClosureTD' to generate a type-indexed
-- family of closures, and then uses the type of its argument to select
-- the actual closure, as shown below (and further below in this file).
--
-- > toClosure :: (StaticId a) => a -> Closure a
-- > toClosure val = $(mkClosureTD [|id val|]) val
--
-- Here, 'mkClosureTD' expects the same type of arguments as 'mkClosure',
-- that is, the identity function 'id' serves as a toplevel closure abstraction
-- and 'val' is a 1-tuple of free local variables. Note that the Template
-- Haskell splice produces a type-indexed family of explicit closures, ie.
-- a function taking a dummy argument (here the second occurence of 'val' --
-- which could have been replaced with '(undefined :: a)') for the sake of
-- supplying the index type (here 'a').
--
-- As before, a static deserialiser corresponding to the toplevel closure
-- abstraction 'id' must be registered. However, this is now linked to
-- instantiating class 'StaticId' and will be discussed below.
--
--
-- Finally, the fourth example is the function 'forceClosureClosure' (see
-- module 'HdpH.Strategies') which returns a "fully forcing" closure strategy.
-- It does so by generating a type-indexed family of static explicit closures
-- as follows.
--
-- > forceClosureClosure :: (StaticForceClosure a)
-- >                     => Closure (Strategy (Closure a))
-- > forceClosureClosure = $(mkClosureTD [|forceClosure|]) (undefined :: a)
--
-- The toplevel closure abstraction 'forceClosure' is defined in module
-- 'HdpH.Strategies' as is the class 'StaticForceClosure'. Note that
-- 'mkClosureTD' works in the same way as above, except for taking only
-- a toplevel closure abstraction as argument, as expected in the case of 
-- *static* explicit closures. Note also that '(undefined :: a)' is the only
-- way of passing the type index since there is other expression of type 'a'.
--
-- A static deserialiser corresponding to the toplevel closure abstraction
-- 'forceClosure' must be registered. Again, this is linked to instantiating 
-- class 'StaticForceClosure' and will be discussed below.
--
--
-- REGISTERING STATIC DESERIALISERS
--
-- For every toplevel closure abstraction occuring in a call to 'mkClosure' or
-- 'mkClosureTD', a corresponding static deserialiser must be constructed
-- (via 'static', 'static_', 'staticTD', or 'staticTD_') and registered
-- (via 'register' from module 'HdpH'). Registration must happen before any
-- closures are deserialised. To this end, every module that constructs
-- explicit closures, or that imports modules that do so, must obey the
-- following conventions:
--
-- * Every module M that constructs explicit closures, or instantiates the
--   classes 'StaticId' or 'StaticForceClosure', or imports modules that
--   export a function 'registerStatic :: IO ()', must itself export a 
--   function 'registerStatic :: IO ()'.
--
-- * The definition of 'registerStatic' in a module M must
--   * call all 'registerStatic' functions of imported modules,
--   * call 'register $(static 'clo_abs)' for all toplevel closure 
--     abstractions 'clo_abs' occuring in non-static explicit closures 
--     constructed by 'mkClosure',
--   * call 'register $(static_ 'clo_abs)' for all toplevel closure
--     abstractions 'clo_abs' occuring in static explicit closures 
--     constructed by 'mkClosure',
--   * call 'register $ staticIdTD (undefined :: t)' for all types 't'
--     for which M defines an instance of class 'StaticId', and
--   * call 'register $ staticForceClosureTD (undefined :: t)' for all
--     types 't' for which M defines an instance of class 'StaticForceClosure'.
--
-- * The 'main' function in the 'Main' module must call its 'registerStatic'
--   first thing, or at least before calling any function that might
--   deserialise explicit closures.
--
-- An example of the definition of 'registerStatic' and the 'StaticId'
-- instances can be in this file. A more complex example can be found in
-- 'TEST/HdpH/sumeuler.hs'; here is an extract of the relevant code.
--
-- > import HdpH
-- > import HdpH.Strategies hiding (registerStatic)
-- > import qualified HdpH.Strategies (registerStatic)
-- >
-- > instance StaticId Int
-- > instance StaticId [Int]
-- > instance StaticId Integer
-- > instance StaticForceClosure Integer
-- >
-- > registerStatic :: IO ()
-- > registerStatic = do
-- >   HdpH.Strategies.registerStatic
-- >   register $ staticIdTD (undefined :: Int)
-- >   register $ staticIdTD (undefined :: [Int])
-- >   register $ staticIdTD (undefined :: Integer)
-- >   register $ staticForceClosureTD (undefined :: Integer)
-- >   register $(static 'spark_sum_euler_abs)
-- >   register $(static_ 'sum_totient)
-- >   register $(static_ 'totient)
-- >
-- > main :: IO ()
-- > main = do
-- >   hSetBuffering stdout LineBuffering
-- >   hSetBuffering stderr LineBuffering
-- >   registerStatic
-- >   ...
--
-- Here, 'registerStatic' first calls 'registerStatic' of the imported
-- module 'HpdH.Strategies'. Then it registers the static deserialisers
-- linked to the instances of classes 'StaticId' and 'StaticForceClosure'.
-- Finally, it registers the closure abstractions 'spark_sum_euler_abs',
-- 'sum_totient' and 'totient', the latter two being used to generate
-- static explicit closures. Note that 'main' calls 'registerStatic'
-- early on -- definitely before it any mention of explicit closures.


-----------------------------------------------------------------------------
-- 'Static' registration

-- static deserializer for explicit Closures; see below
instance StaticId (Closure a)

registerStatic :: IO ()
registerStatic = do
  register $ staticIdTD (undefined :: Closure a)
  register $(static_ 'id)
  register $(static 'compClosure_abs)
  register $(static 'mapClosure_abs)


-----------------------------------------------------------------------------
-- static deserialisers

-- Static deserialisers corresponding to the identity function.
-- NOTE: Do not override default class methods when instantiating.
class (Serialize a, Typeable a) => StaticId a where
  -- static deserializer to be registered for every class instance;
  -- argument serves as type discriminator but is never evaluated
  staticIdTD :: a -> Static (Env -> a)
  staticIdTD typearg = $(staticTD 'id) typearg


-----------------------------------------------------------------------------
-- closure conversion for values

-- converting a value into a closure (forcing the value on serialisation)
toClosure :: (StaticId a) => a -> Closure a
toClosure val = $(mkClosureTD [| id val |]) val
                -- Trailing 'val' is type discriminiator argument.


-- Some identities about closures:
-- 'forall x fun env . unClosure $ unsafeMkClosure x fun env == x'
-- 'forall x . unClosure $ toClosure x == x'
-- 'forall clo_abs free_vars .
--    unClosure $(mkClosure [| clo_abs free_vars |]) == clo_abs free_vars'


------------------------------------------------------------------------------
-- categorical operations on closures

-- identity wrapped in a closure
idClosure :: Closure (a -> a)
idClosure = $(mkClosure [| id |])


-- composition of function closures
compClosure :: Closure (b -> c) -> Closure (a -> b) -> Closure (a -> c)
compClosure clo_g clo_f = $(mkClosure [| compClosure_abs (clo_g, clo_f) |])

{-# INLINE compClosure_abs #-}
compClosure_abs :: (Closure (b -> c), Closure (a -> b)) -> (a -> c)
compClosure_abs (clo_g, clo_f) = unClosure clo_g . unClosure clo_f


-- map a closure by applying a function closure (without forcing anything)
mapClosure :: Closure (a -> b) -> Closure a -> Closure b
mapClosure clo_f clo_x = $(mkClosure [| mapClosure_abs (clo_f, clo_x) |])

{-# INLINE mapClosure_abs #-}
mapClosure_abs :: (Closure (a -> b), Closure a) -> b
mapClosure_abs (clo_f, clo_x) = unClosure clo_f $ unClosure clo_x
