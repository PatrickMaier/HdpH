-- Closure representation, inspired by [1] and refined by [2].
--   [1] Epstein et al. "Haskell for the cloud". Haskell Symposium 2011.
--   [2] Maier, Trinder. "Implementing a High-level Distributed-Memory
--       Parallel Haskell in Haskell". IFL 2011.
-- Internal module necessary to satisfy Template Haskell stage restrictions.
--
-- Author: Patrick Maier
-----------------------------------------------------------------------------

-- The module 'Control.Parallel.HdpH.Closure' implements an /explicit closure/
-- API as described in [2]; the closure representation is novel (and as yet
-- unpublished).
-- Due to Template Haskell stage restrictions, the module has to be split into
-- two halves. This is the half that that defines internals like the actual
-- closure representation and Template Haskell macros on it.
-- The higher level stuff (including a tutorial on expclicit closures) is in
-- module 'Control.Parallel.HdpH.Closure'.

{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module Control.Parallel.HdpH.Closure.Internal
  ( -- source locations with phantom type attached
    LocT,             -- instances: Eq, Ord, Show
    here,             -- :: ExpQ

    -- Closure type constructor
    Closure,          -- instances: Show, NFData, Serialize
    
    -- Closure dictionary type constructor
    CDict,            -- no instances

    -- Type/data constructor marking return type of closure abstractions
    Thunk(Thunk),     -- no instances

    -- eliminating Closures
    unClosure,        -- :: Closure a -> a

    -- safe Closure construction
    mkClosure,        -- :: ExpQ -> ExpQ
    mkClosureLoc,     -- :: ExpQ -> ExpQ

    -- static Closure dictionaries
    static,           -- :: Name -> ExpQ
    staticLoc         -- :: Name -> ExpQ
  ) where

import Prelude hiding (exp)
import Control.DeepSeq (NFData(rnf))
import Control.Monad (unless)
import Data.ByteString.Lazy (ByteString, unpack)
import Data.Serialize (Serialize(put, get), Get, Put)
import Language.Haskell.TH
       (Q, Lit, Exp(AppE, VarE, TupE, ConE), ExpQ, Info(DataConI, VarI), reify,
        Type(AppT, ArrowT, ConT, ForallT),
        appsE, lam1E, conE, varE, global, litE, varP, stringL, tupleDataName)
import qualified Language.Haskell.TH as TH (Loc(..), location)
import Language.Haskell.TH.Syntax
       (Name(Name), NameFlavour(NameG), NameSpace(VarName, DataName),
        newName, pkgString, modString, occString)

import Control.Parallel.HdpH.Closure.Static (Static, unstatic, staticAs)


-----------------------------------------------------------------------------
-- source locations with phantom type attached

-- | A value of type @'LocT a'@ is a representation of a Haskell source location
-- (or more precisely, the location of a Template Haskell splice, as produced by
-- @'here'@). Additionally, this location is annotated with a phantom type 'a',
-- which is used for mapping location indexing to type indexing.
newtype LocT a = LocT { unLocT :: String } deriving (Eq, Ord)

instance Show (LocT a) where
  show = unLocT

-- | Template Haskell construct returning its own location when spliced.
here :: ExpQ
here = do
  loc <- TH.location
  appsE [conE 'LocT, litE $ stringL $ showLoc loc]


-----------------------------------------------------------------------------
-- 'Closure' type constructor

-- | Newtype wrapper marking return type of environment abstractions.
newtype Thunk a = Thunk a


-- | Abstract closure dictionary.
data CDict env a =
       CDict { putEnv :: env -> Put,       -- environment serialiser
               getEnv :: Get env,          -- environment deserialiser
               rnfEnv :: env -> (),        -- environment normaliser
               absEnv :: env -> Thunk a }  -- environment abstraction

-- | Construct a closure dictionary from a given environment abstraction.
mkCDict :: (NFData env, Serialize env) => (env -> Thunk a) -> CDict env a
mkCDict abs = CDict { putEnv = put, getEnv = get, rnfEnv = rnf, absEnv = abs }


-- | Abstract type of explicit closures.
-- Explicit closures consist of an existentially quantified environment
-- and a static dictionary of functions to serialize, deserialize, normalize
-- and evaluate that environment.
-- Explicit closures are hyperstrict in the static dictionary but not in the 
-- environment. An explicit closure is in NF iff its environment is in NF,
-- including any explicit closures contained in the environment.
data Closure a where
  Closure :: !(Static (CDict env a)) -> env -> Closure a

instance NFData (Closure a) where
  -- force env but not static dict (because that's already in NF)
  rnf (Closure st_dict env) = rnfEnv (unstatic st_dict) env

-- Explicit closures are serialisable (using the static dictionary).
instance Serialize (Closure a) where
  put (Closure st_dict env) = do put st_dict 
                                 putEnv (unstatic st_dict) env
  get = do st_dict <- get
           env <- getEnv (unstatic st_dict)
           return (Closure st_dict env)

instance Show (Closure a) where  -- for debugging only; show static dict only
  showsPrec _ (Closure st_dict _env) =
    showString "Closure(" . shows st_dict . showString ")"


-----------------------------------------------------------------------------
-- eliminating Closures

-- | Closure elimination.
-- Unwraps, ie. forces, an explicit closure (by applying 'absEnv').
{-# INLINE unClosure #-}
unClosure :: Closure a -> a
unClosure (Closure st_dict env) =
  case absEnv (unstatic st_dict) env of { Thunk x -> x }


-----------------------------------------------------------------------------
-- safely constructing Closures from closure abstractions

-- | Template Haskell transformation constructing a Closure from an expression.
-- The expression must either be a single static closure (in which case the
-- result is a /static/ Closure), or an application of an environment
-- abstraction to a tuple of variables.
-- See the tutorial below for how to use @mkClosure@.
mkClosure :: ExpQ -> ExpQ
mkClosure expQ = do
  let prf = "Control.Parallel.HdpH.Closure.mkClosure: "
  let err_msg = prf ++ "argument not of the form " ++
                "'static_closure' or 'environment_abstraction (var,...,var)'"
  let panic_msg = prf ++ "impossible case"
  exp <- expQ
  (name, env, is_abs) <- case exp of
    AppE (VarE clo_abs_name) vars -> return (clo_abs_name,  vars, True)
    AppE (ConE clo_abs_name) vars -> return (clo_abs_name,  vars, True)
    VarE stat_clo_name            -> return (stat_clo_name, unit, False)
    ConE stat_clo_name            -> return (stat_clo_name, unit, False)
    _                             -> fail err_msg
  unless (isGlobalName name) $ fail err_msg
  unless (isVarTuple env) $ fail err_msg
  ty <- typeClosureAbs (const panic_msg) name
  unless (isClosureAbsType ty == is_abs) $ fail err_msg
  let st_dictQ = static name
  let envQ = return env
  appsE [conE 'Closure, st_dictQ, envQ]


-- | Template Haskell transformation constructing a family of Closures from an
-- expression. The family is indexed by location (that's what the suffix @Loc@
-- stands for).
-- The expression must either be a single static closure (in which case the
-- result is a family of /static/ Closures), or an application of an environment
-- abstraction to a tuple of variables.
-- See the tutorial below for how to use @mkClosureLoc@.
mkClosureLoc :: ExpQ -> ExpQ
mkClosureLoc expQ = do
  let prf = "Control.Parallel.HdpH.Closure.mkClosureLoc: "
  let err_msg = prf ++ "argument not of the form " ++
                "'static_closure' or 'environment_abstraction (var,...,var)'"
  let panic_msg = prf ++ "impossible case"
  exp <- expQ
  (name, env, is_abs) <- case exp of
    AppE (VarE clo_abs_name) vars -> return (clo_abs_name,  vars, True)
    AppE (ConE clo_abs_name) vars -> return (clo_abs_name,  vars, True)
    VarE stat_clo_name            -> return (stat_clo_name, unit, False)
    ConE stat_clo_name            -> return (stat_clo_name, unit, False)
    _                             -> fail err_msg
  unless (isGlobalName name) $ fail err_msg
  unless (isVarTuple env) $ fail err_msg
  ty <- typeClosureAbs (const panic_msg) name
  unless (isClosureAbsType ty == is_abs) $ fail err_msg
  let st_dict_LocQ = staticLoc name
  let envQ = return env
  loc <- newName "loc"
  lam1E (varP loc) $ appsE [conE 'Closure, appsE [st_dict_LocQ, varE loc], envQ]


-----------------------------------------------------------------------------
-- Static closure dictionaries

-- | Template Haskell transformation converting a static closure or environment
-- abstraction (given by its name) into a @'Static'@ closure dictionary.
-- See the tutorial below for how to use @static@.
static :: Name -> ExpQ
static name = do
  let prf = "Control.Parallel.HdpH.Closure.static: "
  let mkErrMsg1 nm = prf ++ nm ++ " not a global variable or data constructor"
  let mkErrMsg2 nm = prf ++ nm ++ " not a variable or data constructor"
  (expQ, label) <- labelClosureAbs mkErrMsg1 name
  ty <- typeClosureAbs mkErrMsg2 name
  if isClosureAbsType ty
    then appsE [global 'mkStatic,  expQ, litE label]
    else appsE [global 'mkStatic_, expQ, litE label]

-- Called by 'static' if argument names an environment abstraction.
{-# INLINE mkStatic #-}
mkStatic :: (NFData env, Serialize env)
         => (env -> Thunk a) -> String -> Static (CDict env a)
mkStatic env_abs label =
  staticAs (mkCDict env_abs) label

-- Called by 'static' if argument names a static closure.
{-# INLINE mkStatic_ #-}
mkStatic_ :: a -> String -> Static (CDict () a)
mkStatic_ stat_clo label =
  staticAs (mkCDict $ const $ Thunk stat_clo) label   -- inject 'Thunk'


-- | Template Haskell transformation converting a static closure or environment
-- abstraction (given by its name) into a family of @'Static'@ closure
-- dictionaries indexed by location (that's what the suffix @Loc@ stands for).
-- See the tutorial below for how to use @staticLoc@.
staticLoc :: Name -> ExpQ
staticLoc name = do
  let prf = "Control.Parallel.HdpH.Closure.staticLoc: "
  let mkErrMsg1 nm = prf ++ nm ++ " not a global variable or data constructor"
  let mkErrMsg2 nm = prf ++ nm ++ " not a variable or data constructor"
  (expQ, label) <- labelClosureAbs mkErrMsg1 name
  ty <- typeClosureAbs mkErrMsg2 name
  if isClosureAbsType ty
    then appsE [global 'mkStaticLoc,  expQ, litE label]
    else appsE [global 'mkStaticLoc_, expQ, litE label]

-- Called by 'staticLoc' if argument names an environment abstraction.
{-# INLINE mkStaticLoc #-}
mkStaticLoc :: (NFData env, Serialize env)
            => (env -> Thunk a) -> String -> (LocT a -> Static (CDict env a))
mkStaticLoc env_abs label =
  \ loc -> staticAs (mkCDict env_abs) $
             label ++ "{loc=" ++ show loc ++ "}"

-- Called by 'staticLoc' if argument names a static closure.
{-# INLINE mkStaticLoc_ #-}
mkStaticLoc_ :: a -> String -> (LocT a -> Static (CDict () a))
mkStaticLoc_ stat_clo label =
  \ loc -> staticAs (mkCDict $ const $ Thunk stat_clo) $   -- inject 'Thunk'
             label ++ "{loc=" ++ show loc ++ "}"


-----------------------------------------------------------------------------
-- auxiliary stuff: mostly operations involving variable names

-- Expects the second argument to be a global variable or constructor name.
-- If so, returns a pair consisting of an expression (the closure abstraction
-- or static closure named by the argument) and a label representing the name;
-- otherwise fails with an error message constructed by the first argument.
{-# INLINE labelClosureAbs #-}
labelClosureAbs :: (String -> String) -> Name -> Q (ExpQ, Lit)
labelClosureAbs mkErrMsg name =
  let mkLabel pkg' mod' occ' = stringL $ pkgString pkg' ++ "/" ++
                                         modString mod' ++ "." ++
                                         occString occ'
    in case name of
         Name occ' (NameG VarName pkg' mod') ->
           return (varE name, mkLabel pkg' mod' occ')
         Name occ' (NameG DataName pkg' mod') ->
           return (conE name, mkLabel pkg' mod' occ')
         _ -> fail $ mkErrMsg $ show name


-- Expects the second argument to be a global variable or constructor name.
-- If so, returns its type; otherwise fails with an error message constructed
-- by the first argument.
{-# INLINE typeClosureAbs #-}
typeClosureAbs :: (String -> String) -> Name -> Q Type
typeClosureAbs mkErrMsg name = do
  info <- reify name
  case info of
    DataConI _ ty _ _ -> return ty
    VarI     _ ty _ _ -> return ty
    _                 -> fail $ mkErrMsg $ show name


-- @isClosureAbsType ty@ is True iff @ty@ is of the form
-- @forall a_1 ... a_n . b -> Thunk c@.
isClosureAbsType :: Type -> Bool
isClosureAbsType ty =
  case ty of
    AppT (AppT ArrowT _) (AppT (ConT tyCon) _) -> tyCon == ''Thunk
    ForallT _ [] ty'                           -> isClosureAbsType ty'
    _                                          -> False


-- True iff the argument is a global variable or constructor name.
{-# INLINE isGlobalName #-}
isGlobalName :: Name -> Bool
isGlobalName name = case name of
                      Name _ (NameG VarName  _ _) -> True  -- global variable
                      Name _ (NameG DataName _ _) -> True  -- data constructor
                      _                           -> False


-- True iff the expression is a variable.
{-# INLINE isVar #-}
isVar :: Exp -> Bool
isVar exp = case exp of
              VarE _ -> True
              _      -> False

-- True iff the expression is a variable or a (possibly empty) tuple of vars.
-- NOTE: Empty tuples are only properly recognized from GHC 7.4 onwards
--       (prior to 7.4 empty tuples were represented as unit values).
{-# INLINE isVarTuple #-}
isVarTuple :: Exp -> Bool
isVarTuple exp = case exp of
                   TupE exps -> all isVar exps           -- non-empty tuples
                   ConE name -> name == tupleDataName 0  -- empty tuple
                   _         -> isVar exp                -- single variable


-- Template Haskell expression for empty tuple.
unit :: Exp
unit = ConE $ tupleDataName 0


-----------------------------------------------------------------------------
-- auxiliary stuff: various show operations

-- Show Template Haskell location.
-- Should be defined in module Language.Haskell.TH.
{-# INLINE showLoc #-}
showLoc :: TH.Loc -> String
showLoc loc = showsLoc loc ""
  where
    showsLoc loc' = showString (TH.loc_package loc')  . showString "/" .
                    showString (TH.loc_module loc')   . showString ":" .
                    showString (TH.loc_filename loc') . showString "@" .
                    shows (TH.loc_start loc')         . showString "-" .
                    shows (TH.loc_end loc')
