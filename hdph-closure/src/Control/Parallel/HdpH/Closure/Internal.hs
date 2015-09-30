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
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE DeriveDataTypeable #-}

module Control.Parallel.HdpH.Closure.Internal
  ( -- Closure type constructor
    Closure,          -- instances: Show, NFData, Binary, Serialize, Typeable

    -- Type/data constructor marking return type of environment abstractions
    Thunk(Thunk),     -- no instances (except Typeable)

    -- eliminating Closures
    unClosure,        -- :: Closure a -> a

    -- value Closure construction
    ToClosure,        -- synonym (to Static of some internal type)
    mkToClosure,      -- :: (NFData a, Binary a, Serialize a, Typeable a)
                      -- => ToClosure a
    toClosure,        -- :: ToClosure a -> a -> Closure a

    -- safe Closure construction
    mkClosure,        -- :: ExpQ -> ExpQ

    -- static Closure environment construction
    static            -- :: Name -> ExpQ
  ) where

import Prelude hiding (exp, abs)
import Control.DeepSeq (NFData(rnf))
import Control.Monad (unless)
import Data.Binary (Binary)
import qualified Data.Binary (put, get)
import Data.Constraint (Dict(Dict))
import Data.Serialize (Serialize)
import qualified Data.Serialize (put, get)
import Data.Typeable (Typeable, typeRep, Proxy(Proxy))
import Language.Haskell.TH
       (Q, Exp(AppE, VarE, LitE, TupE, ConE), ExpQ,
        Type(AppT, ArrowT, ConT, ForallT),
        Info(DataConI, VarI), reify, tupleDataName, stringL)
import qualified Language.Haskell.TH as TH (Loc(..), location)
import Language.Haskell.TH.Syntax
       (Name(Name), NameFlavour(NameG), NameSpace(VarName, DataName),
        pkgString, modString, occString)

import Control.Parallel.HdpH.Closure.Static (Static, unstatic, staticAs)


-----------------------------------------------------------------------------
-- 'Closure' type constructor

-- | Newtype wrapper marking return type of environment abstractions.
newtype Thunk a = Thunk a deriving (Typeable)


-- | Internal pair type.
data Pair a b = Pair a b deriving (Typeable)


-- | Static closure environment, parametrized by dynamic environment 'd' and
-- closure result type 'a'.
--
-- A static closure environment consists of an explicit dictionary for a
-- number of type classes (to normalize and serialize the dynamic environment)
-- and a function to evaluate the dynamic environment.
type StaticEnv d a =
  Static (Pair (Dict (NFData d, Binary d, Serialize d)) (d -> Thunk a))


-- | Explicit closure (abstract type).
-- Explicit closures consist of an existentially quantified dynamic environment
-- and a hyperstrict static closure environment. An explicit closure is in NF
-- iff its dynamic environment is in NF, including any explicit closures
-- contained in the environment.
data Closure a where
  Closure :: !(StaticEnv d a) -> d -> Closure a
  deriving (Typeable)

instance NFData (Closure a) where
  -- force dyn env but not stat env (because that's already in NF)
  rnf (Closure s d) = case unstatic s of
                        Pair Dict _ -> rnf d

-- Explicit closures are serialisable (using the static dictionary).
instance Binary (Closure a) where
  put (Closure s d) = do Data.Binary.put s
                         case unstatic s of
                           Pair Dict _ -> Data.Binary.put d
  get = do s <- Data.Binary.get
           d <- case unstatic s of
                  Pair Dict _ -> Data.Binary.get
           return (Closure s d)

instance Serialize (Closure a) where
  put (Closure s d) = do Data.Serialize.put s
                         case unstatic s of
                           Pair Dict _ -> Data.Serialize.put d
  get = do s <- Data.Serialize.get
           d <- case unstatic s of
                  Pair Dict _ -> Data.Serialize.get
           return (Closure s d)

instance Show (Closure a) where  -- for debugging only; show static env only
  showsPrec _ (Closure s _d) =
    showString "Closure(" . shows s . showString ")"


-----------------------------------------------------------------------------
-- eliminating Closures

-- | Closure elimination.
-- Unwraps, ie. forces, an explicit closure (by applying static fun eval).
{-# INLINE unClosure #-}
unClosure :: Closure a -> a
unClosure (Closure s d) =
  case unstatic s of
    Pair _ eval -> case eval d of
                     Thunk x -> x


-----------------------------------------------------------------------------
-- introducing value Closures

type ToClosure a = StaticEnv a a

toClosure :: ToClosure a -> a -> Closure a
toClosure = Closure

mkToClosure :: forall a . (NFData a, Binary a, Serialize a, Typeable a)
            => ToClosure a
mkToClosure =
  mkStatic Thunk label
    where
      label = "ToClosure<<" ++ show (typeRep (Proxy :: Proxy a)) ++ ">>"


-----------------------------------------------------------------------------
-- safely constructing Closures from eval abstractions

-- | Template Haskell transformation constructing a Closure from an expression.
-- The expression must either be a single static closure (in which case the
-- result is a /static/ Closure), or an application of an eval
-- abstraction to a tuple of variables.
-- See the tutorial below for how to use @mkClosure@.
mkClosure :: ExpQ -> ExpQ
mkClosure expQ = do
  let prf = "Control.Parallel.HdpH.Closure.mkClosure: "
  let err_msg = prf ++ "argument not of the form " ++
                "'static_closure' or 'eval_abstraction (var,...,var)'"
  exp <- expQ
  (name, d, is_abs) <- case exp of
    AppE (VarE clo_abs_name) vars -> return (clo_abs_name,  vars, True)
    AppE (ConE clo_abs_name) vars -> return (clo_abs_name,  vars, True)
    VarE stat_clo_name            -> return (stat_clo_name, unit, False)
    ConE stat_clo_name            -> return (stat_clo_name, unit, False)
    _                             -> fail err_msg
  unless (isGlobalName name) $ fail err_msg
  unless (isVarTuple d) $ fail err_msg
-- Note: 'typeClosureAbs' commented to avoid calling 'reify' in 'mkClosure';
--       this may lead to less comprehensible error messages but should
--       have no consequences otherwise.
--  ty <- typeClosureAbs (const $ prf ++ "impossible case") name
--  unless (isClosureAbsType ty == is_abs) $ fail err_msg
  let sQ = staticInternal prf is_abs name
  let dQ = return d
  [| Closure $sQ $dQ |]


-----------------------------------------------------------------------------
-- Static closure environment

-- | Template Haskell transformation converting a static closure or eval
-- abstraction (given by its name) into a static closure environment.
-- See the tutorial below for how to use @static@.
static :: Name -> ExpQ
static name = do
  let prf = "Control.Parallel.HdpH.Closure.static: "
  let mkErrMsg nm = prf ++ nm ++ " not a variable or data constructor"
  ty <- typeClosureAbs mkErrMsg name
  staticInternal prf (isClosureAbsType ty) name

-- Called by 'static' and 'mkClosure'.
staticInternal :: String -> Bool -> Name -> ExpQ
staticInternal prf is_abs name = do
  let mkErrMsg nm = prf ++ nm ++ " not a global variable or data constructor"
  (expQ, labelQ) <- labelClosureAbs mkErrMsg name
  if is_abs
    then [| mkStatic  $expQ $labelQ |]  -- name is eval abstraction
    else [| mkStatic_ $expQ $labelQ |]  -- name is static closure

-- Called by 'staticInternal' if argument names an eval abstraction.
{-# INLINE mkStatic #-}
mkStatic :: (NFData d, Binary d, Serialize d)
         => (d -> Thunk a) -> String -> StaticEnv d a
mkStatic eval label =
  staticAs (Pair Dict eval) label

-- Called by 'staticInternal' if argument names a static closure.
{-# INLINE mkStatic_ #-}
mkStatic_ :: a -> String -> StaticEnv () a
mkStatic_ stat_clo label =
  staticAs (Pair Dict eval) label
    where
      eval () = Thunk stat_clo   -- inject 'Thunk'


-----------------------------------------------------------------------------
-- auxiliary stuff: mostly operations involving variable names

-- Expects the second argument to be a global variable or constructor name.
-- If so, returns a pair of expressions, the first of which is the denotation
-- of the given name, and the second is a string expression uniquely labelling
-- the given name; otherwise fails with an error message constructed by the
-- first argument.
{-# INLINE labelClosureAbs #-}
labelClosureAbs :: (String -> String) -> Name -> Q (ExpQ, ExpQ)
labelClosureAbs mkErrMsg name =
  let mkLabel pkg' mod' occ' =
        LitE $ stringL $
          pkgString pkg' ++ "/" ++ modString mod' ++ "." ++ occString occ'
    in case name of
         Name occ' (NameG VarName pkg' mod') ->
           return (return $ VarE name, return $ mkLabel pkg' mod' occ')
         Name occ' (NameG DataName pkg' mod') ->
           return (return $ ConE name, return $ mkLabel pkg' mod' occ')
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
