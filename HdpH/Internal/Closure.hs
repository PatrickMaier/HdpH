-- Closure representation, inspired by
--   [1] Epstein et al. "Haskell for the cloud". Haskell Symposium 2011.
-- Internal module necessary to satisfy Template Haskell stage restrictions.
--
-- Visibility: HdpH.Closure
-- Author: Patrick Maier <P.Maier@hw.ac.uk>
-- Created: 12 May 2011
--
-----------------------------------------------------------------------------

{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE TemplateHaskell #-}

module HdpH.Internal.Closure
  ( -- * serialised environment
    Env,       -- synonym: Data.ByteString.Lazy.ByteString
    encodeEnv, -- :: (Serialize a) => a -> Env
    decodeEnv, -- :: (Serialize a) => Env -> a

    -- * Closure type constructor; constructor exported to HdpH.Closure only
    Closure(..),      -- instances: Show, NFData, Serialize, Typeable

    -- * introducing and eliminating closures
    unsafeMkClosure,  -- :: a -> Static (Env -> a) -> Env -> Closure a
    unClosure,        -- :: Closure a -> a

    -- * safe closure construction
    mkClosure,        -- :: ExpQ -> ExpQ
    mkClosureTD,      -- :: ExpQ -> ExpQ

    -- * static deserializers
    static,           -- :: Name -> ExpQ
    staticTD,         -- :: Name -> ExpQ
    static_,          -- :: Name -> ExpQ
    staticTD_         -- :: Name -> ExpQ
  ) where

import Prelude hiding (error)
import Control.DeepSeq (NFData(rnf))
import Data.ByteString.Lazy as Lazy (ByteString)
import Data.Serialize (Serialize)
import qualified Data.Serialize (put, get)
import Data.Typeable (Typeable, Typeable1, typeOf, typeOfDefault)
import Language.Haskell.TH
       (Exp(AppE, VarE, TupE), ExpQ,
        appsE, lam1E, conE, varE, global, litE, sigE, varP, stringL)
import Language.Haskell.TH.Syntax
       (Name(Name), NameFlavour(NameG, NameL), NameSpace(VarName), newName,
        PkgName, pkgString, ModName, modString, OccName, occString)

import HdpH.Internal.Misc (Void, encodeLazy, decodeLazy)
import HdpH.Internal.Static (Static, unstatic, staticAs, staticAsTD)


-----------------------------------------------------------------------------
-- closure type constructor

-- Serialiased environment
type Env = Lazy.ByteString

-- introducing an environment
encodeEnv :: (Serialize a) => a -> Env
encodeEnv = encodeLazy

-- eliminating an environment
decodeEnv :: (Serialize a) => Env -> a
decodeEnv = decodeLazy


-- An explicit 'Closure' (of type 'a') maintains a dual representation
-- of an actual closure of type 'a'.
-- * The first argument is the actual closure value, the /thunk/.
-- * The second and third argument comprise a serialisable representation
--   of the thunk, consisting of a (static) function expecting a
--   (serialised) environment, plus such a serialised environment.
--
-- The former rep is used for computing with closures while the latter
-- is used for serialising and communicating closures across the network.
--
data Closure a = Closure
                   a                    -- actual thunk
                   (Static (Env -> a))  -- static environment deserialiser
                   Env                  -- serialised environment

-- 'Typeable' instance 'Closure' ignores phantom type argument (subst w/ Void)
deriving instance Typeable1 Closure

instance Typeable (Closure a) where
  typeOf _ = typeOfDefault (undefined :: Closure Void)


-----------------------------------------------------------------------------
-- NFData/Serialize instances for Closure;
-- note that these instances are uniform for all 'Closure a' types since
-- they ignore the type argument 'a'.

-- NOTE: These instances obey a contract between Serialize and NFData
-- * A type is an instance of NFData iff it is an instance of Serialize.
-- * A value is forced by 'rnf' to the same extent it is forced by
--   'rnf . encode'.

-- NOTE: Phil is unhappy with this contract. He argues that 'rnf' should
--       fully force the closure (and thus evaluate further than 'rnf . encode'
--       would). His argument is essentially that 'rnf' should act on
--       explicit closures as it acts on thunks. Which would mean
--       that 'NFData (Closure a)' can only be defined given a '(Serialize a)'
--       context.
--
--       Here is the killer argument against Phil's proposal: Because of the
--       type of 'rnf', it can only evaluate its argument as a side effect;
--       it cannot actually change its representation. However, forcing an
--       explicit closure must change its representation (at least if we
--       want to preserve the evaluation status across serialisation).

instance NFData (Closure a) where
  -- force all fields apart from the actual thunk
  rnf (Closure _ fun env) = rnf fun `seq` rnf env


instance Serialize (Closure a) where
  -- serialise all fields apart from the actual thunk
  put (Closure _ fun env) = Data.Serialize.put fun >>
                            Data.Serialize.put env

  -- deserialise all fields and lazily re-instate the actual thunk
  get = do fun <- Data.Serialize.get
           env <- Data.Serialize.get
           let thk = (unstatic fun) env
           return $ Closure thk fun env


instance Show (Closure a) where  -- for debugging only; show serialisable rep
  showsPrec _ (Closure _ fun env) = showString "Closure(" . shows fun .
                                    showString "," . shows env . showString ")"


-----------------------------------------------------------------------------
-- introducing and eliminating closures

-- Eliminating a closure (aka. un-closure conversion) by returning its thunk
{-# INLINE unClosure #-}
unClosure :: Closure a -> a
unClosure (Closure thk _ _) = thk


-- Introducing a closure (aka. closure conversion)
-- NOTE: unsafe version; '(unstatic fun) env' and 'thk' must evaluate to same.
{-# INLINE unsafeMkClosure #-}
unsafeMkClosure :: a -> Static (Env -> a) -> Env -> Closure a
unsafeMkClosure thk fun env = Closure thk fun env


-----------------------------------------------------------------------------
-- constructing closures from closure abstractions

-- Template Haskell transformation converting a thunk into an explicit closure.
-- The thunk must either be single toplevel closure abstraction (in which case
-- the result is a static explicit closure), or an application of a toplevel
-- closure abstraction to a tuple of local variables.
mkClosure :: ExpQ -> ExpQ
mkClosure thkQ = do
  thk <- thkQ
  case thk of
    AppE (VarE clo_abs) free_vars
      | isGlobalVarName clo_abs && isLocalVars free_vars -> do
          let envQ = appsE [global 'encodeEnv, return free_vars]
          let funQ = static clo_abs
          appsE [conE 'Closure, thkQ, funQ, envQ]
    VarE clo
      | isGlobalVarName clo -> do
          let envQ = [| encodeEnv () |]
          let funQ = static_ clo
          appsE [conE 'Closure, thkQ, funQ, envQ]
    _ -> fail $ "HdpH.Internal.Closure.mkClosure: argument not " ++
                "'globalVar' or 'globalVar (localVar_1,...,localVar_n)'"


-- Template Haskell transformation converting a thunk into a type
-- discriminating (that's what 'TD' stands for) family of explicit closures.
-- The thunk must either be single toplevel closure abstraction (in which case
-- the result is a family of static explicit closure), or an application of a
-- toplevel closure abstraction to a tuple of local variables.
-- Note that the family index 'typearg' is never going to be evaluated (as it
-- is only passed to 'staticTD' or 'staticTD_') hence it may be 'undefined'.
mkClosureTD :: ExpQ -> ExpQ
mkClosureTD thkQ = do
  typearg <- newName "typearg"
  thk <- thkQ
  case thk of
    AppE (VarE clo_abs) free_vars
      | isGlobalVarName clo_abs && isLocalVars free_vars -> do
          let envQ = appsE [global 'encodeEnv, return free_vars]
          let funQ = staticTD clo_abs
          lam1E (varP typearg) $
            appsE [conE 'Closure, thkQ, appsE [funQ, varE typearg], envQ]
    VarE clo
      | isGlobalVarName clo -> do
          let envQ = [| encodeEnv () |]
          let funQ = staticTD_ clo
          lam1E (varP typearg) $
            appsE [conE 'Closure, thkQ, appsE [funQ, varE typearg], envQ]
    _ -> fail $ "HdpH.Internal.Closure.mkClosureTD: argument not " ++
                "'globalVar' or 'globalVar (localVar_1,...,localVar_n)'"


-----------------------------------------------------------------------------
-- static deserializers

-- Template Haskell transformation converting a toplevel closure abstraction
-- (ie. any toplevel function, given by its name) into a static deserialiser
-- (by calling 'mkStatic' below).
static :: Name -> ExpQ
static name =
  case tryShowClosureAbs name of
    Just label -> appsE [global 'mkStatic, varE name, litE (stringL label)]
    Nothing    -> fail $ "HdpH.Internal.Closure.static: " ++
                         show name ++ " not a global variable name"

{-# INLINE mkStatic #-}
mkStatic :: (Serialize a)
         => (a -> b) -> String -> Static (Env -> b)
mkStatic clo_abs label =
  staticAs (clo_abs . decodeEnv) label


-- Template Haskell transformation converting a static toplevel closure
-- abstraction (ie. any toplevel value, given by its name) into a static
-- deserialiser (by calling 'mkStatic_' below).
static_ :: Name -> ExpQ
static_ name =
  case tryShowClosureAbs_ name of
    Just label -> appsE [global 'mkStatic_, varE name, litE (stringL label)]
    Nothing    -> fail $ "HdpH.Internal.Closure.static_: " ++
                         show name ++ " not a global variable name"

{-# INLINE mkStatic_ #-}
mkStatic_ :: b -> String -> Static (Env -> b)
mkStatic_ clo label =
  staticAs (const clo) label


-- Template Haskell transformation converting a toplevel closure abstraction
-- (ie. any toplevel function, given by its name) into a type discriminating
-- (that's what 'TD' stands for) family of static deserialisers.
-- This is done by calling 'mkStaticTD' below; note that the 'typearg' in
-- 'mkStaticTD' is never going to be evaluated, hence may be 'undefined'.
staticTD :: Name -> ExpQ
staticTD name =
  case tryShowClosureAbs name of
    Just label -> appsE [global 'mkStaticTD, varE name, litE (stringL label)]
    Nothing    -> fail $ "HdpH.Internal.Closure.staticTD: " ++
                         show name ++ " not a global variable name"

{-# INLINE mkStaticTD #-}
mkStaticTD :: (Serialize a, Typeable t)
           => (a -> b) -> String -> (t -> Static (Env -> b))
mkStaticTD clo_abs label =
  \ typearg -> staticAsTD (clo_abs . decodeEnv) label typearg


-- Template Haskell transformation converting a static toplevel closure
-- abstraction (ie. any toplevel value, given by its name) into a type
-- discriminating (that's what 'TD' stands for) family of static deserialisers.
-- This is done by calling 'mkStaticTD_' below; note that the 'typearg' in
-- 'mkStaticTD_' is never going to be evaluated, hence may be 'undefined'.
staticTD_ :: Name -> ExpQ
staticTD_ name =
  case tryShowClosureAbs_ name of
    Just label -> appsE [global 'mkStaticTD_, varE name, litE (stringL label)]
    Nothing    -> fail $ "HdpH.Internal.Closure.staticTD_: " ++
                         show name ++ " not a global variable name"

{-# INLINE mkStaticTD_ #-}
mkStaticTD_ :: (Typeable t) => b -> String -> (t -> Static (Env -> b))
mkStaticTD_ clo label =
  \ typearg -> staticAsTD (const clo) label typearg


-----------------------------------------------------------------------------
-- auxiliary stuff: operations involving variable names

-- Returns a String representation of the argument if it is a global variable,
-- Nothing otherwise.
{-# INLINE tryShowClosureAbs #-}
tryShowClosureAbs :: Name -> Maybe String
tryShowClosureAbs name =
  case name of
    Name occ (NameG VarName pkg mod) ->
      Just $ pkgString pkg ++ "//" ++ modString mod ++ "." ++ occString occ
                              -- 2 slashes separating package from module
    _ -> Nothing


-- Returns a String representation of the argument if it is a global variable,
-- Nothing otherwise. Note that no two strings produced by 'tryShowClosureAbs'
-- and 'tryShowClosureAbs_', respectively, can ever coincide.
{-# INLINE tryShowClosureAbs_ #-}
tryShowClosureAbs_ :: Name -> Maybe String
tryShowClosureAbs_ name =
  case name of
    Name occ (NameG VarName pkg mod) ->
      Just $ pkgString pkg ++ "///" ++ modString mod ++ "." ++ occString occ
                              -- 3 slashes separating package from module
    _ -> Nothing


-- True iff the expression is a global variable.
{-# INLINE isGlobalVarName #-}
isGlobalVarName :: Name -> Bool
isGlobalVarName (Name _ (NameG VarName _ _)) = True
isGlobalVarName _                            = False


-- True iff the expression is a local variable.
{-# INLINE isLocalVar #-}
isLocalVar :: Exp -> Bool
isLocalVar (VarE (Name _ (NameL _))) = True
isLocalVar _                         = False

-- True iff the expression is a (possibly empty) tuple of local variables.
{-# INLINE isLocalVarTuple #-}
isLocalVarTuple :: Exp -> Bool
isLocalVarTuple (TupE exps) = all isLocalVar exps
isLocalVarTuple _           = False

-- True iff the expression is a local variable or a tuple of local variables.
{-# INLINE isLocalVars #-}
isLocalVars :: Exp -> Bool
isLocalVars exp = isLocalVar exp || isLocalVarTuple exp
