-- Closure representation, inspired by [1] and refined by [2].
--   [1] Epstein et al. "Haskell for the cloud". Haskell Symposium 2011.
--   [2] Maier, Trinder. "Implementing a High-level Distributed-Memory
--       Parallel Haskell in Haskell". IFL 2011.
-- Internal module necessary to satisfy Template Haskell stage restrictions.
--
-- Author: Patrick Maier
-----------------------------------------------------------------------------

-- The module 'Control.Parallel.HdpH.Closure' implements /explicit closures/ 
-- as described in [2]. Due to Template Haskell stage restrictions, the module
-- has to be split into two halves. This is the half that that defines internals
-- like the actual closure representation and Template Haskell macros on it.
-- The higher level stuff (including a tutorial on expclicit closures) is in
-- module 'Control.Parallel.HdpH.Closure'.

{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module Control.Parallel.HdpH.Closure.Internal
  ( -- source locations with phantom type attached
    LocT,             -- instances: Eq, Ord, Show
    here,             -- :: ExpQ

    -- serialised environment
    Env,              -- instances: Eq, Ord, Show, NFData, Serialize
    encodeEnv,        -- :: (Serialize a) => a -> Env
    decodeEnv,        -- :: (Serialize a) => Env -> a

    -- Closure type constructor
    Closure,          -- instances: Show, NFData, Serialize

    -- Type/data constructor marking return type of closure abstractions
    Thunk(Thunk),     -- no instances

    -- introducing and eliminating Closures
    unsafeMkClosure,  -- :: a -> Static (Env -> a) -> Env -> Closure a
    unClosure,        -- :: Closure a -> a

    -- safe Closure construction
    mkClosure,        -- :: ExpQ -> ExpQ
    mkClosureLoc,     -- :: ExpQ -> ExpQ

    -- static deserializers
    static,           -- :: Name -> ExpQ
    staticLoc         -- :: Name -> ExpQ
  ) where

import Prelude hiding (exp)
import Control.DeepSeq (NFData(rnf))
import Control.Monad (unless)
import Data.ByteString.Lazy (ByteString, unpack)
import Data.Serialize (Serialize)
import qualified Data.Serialize (put, get, encodeLazy, decodeLazy)
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
-- (or more precisely, the location of a Template Haskell slice, as produced by
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
-- serialised environments

-- | Abstract type of serialised environments.
newtype Env = Env ByteString deriving (Eq, Ord, NFData, Serialize)

instance Show Env where
  showsPrec _ (Env env) = shows env


-- | Creates a serialised environment from a given value of type @a@.
encodeEnv :: (Serialize a) => a -> Env
encodeEnv x = Env $ Data.Serialize.encodeLazy x

-- | Deserialises a serialised environment producing a value of type @a@.
-- Note that the programmer asserts that the environment can be deserialised
-- at type @a@, a type mismatch may abort or crash the program.
decodeEnv :: (Serialize a) => Env -> a
decodeEnv (Env env) =
  case Data.Serialize.decodeLazy env of
    Right x  -> x
    Left msg -> error $ "Control.Parallel.HdpH.Closure.decodeEnv " ++
                        showEnvPrefix 20 env ++ ": " ++ msg


-----------------------------------------------------------------------------
-- 'Closure' type constructor

-- | Newtype wrapper marking return type of closure abstractions.
newtype Thunk a = Thunk a


-- | An explicit Closure, ie. a term of type @Closure a@, maintains a dual
-- representation of an actual closure (ie. a thunk) of type @a@.
--
-- (1) One half of that representation is the actual closure, the /thunk/
--     of type @a@.
--
-- (2) The other half is a serialisable representation of the thunk,
--     consisting of a @'Static'@ environment deserialiser, of type
--     @'Static' ('Env' -> a)@, plus a serialised environment of type @'Env'@.
--
-- Representation (1) is used for computing with Closures while
-- representation (2) is used for serialising and communicating Closures
-- across the network.
--
data Closure a = Closure
                   (Thunk a)                  -- actual thunk
                   (Static (Env -> Thunk a))  -- Static environment deserialiser
                   Env                        -- serialised environment


-----------------------------------------------------------------------------
-- Show/NFData/Serialize instances for 'Closure'
--
-- Note that these instances are uniform for all 'Closure a' types since
-- they ignore the type argument 'a'.

-- NOTE: These instances obey a contract between Serialize and NFData
-- * A type is an instance of NFData iff it is an instance of Serialize.
-- * A value is forced by 'rnf' to the same extent it is forced by
--   'rnf . encode'.

-- NOTE: Phil is unhappy with this contract. He argues that 'rnf' should
--       fully force the Closure (and thus evaluate further than 'rnf . encode'
--       would). His argument is essentially that 'rnf' should act on
--       explicit Closures as it acts on thunks. Which would mean
--       that 'NFData (Closure a)' can only be defined given a '(Serialize a)'
--       context.
--
--       Here is the killer argument against Phil's proposal: Because of the
--       type of 'rnf', it can only evaluate its argument as a side effect;
--       it cannot actually change its representation. However, forcing an
--       explicit Closure must change its representation (at least if we
--       want to preserve the evaluation status across serialisation).

instance NFData (Closure a) where
  -- force the serialisable rep but not the actual thunk
  rnf (Closure _ fun env) = rnf fun `seq` rnf env


instance Serialize (Closure a) where
  -- serialise the serialisable rep but not the actual thunk
  put (Closure _ fun env) = Data.Serialize.put fun >>
                            Data.Serialize.put env

  -- deserialise the serialisable rep and lazily re-instate the actual thunk
  get = do fun <- Data.Serialize.get
           env <- Data.Serialize.get
           let thk = (unstatic fun) env
           return $ Closure thk fun env


instance Show (Closure a) where  -- for debugging only; show serialisable rep
  showsPrec _ (Closure _ fun env) = showString "Closure(" . shows fun .
                                    showString "," . shows env . showString ")"


-----------------------------------------------------------------------------
-- introducing and eliminating Closures

-- | Eliminates a Closure by returning its thunk.
-- This operation is cheap.
{-# INLINE unClosure #-}
unClosure :: Closure a -> a
unClosure (Closure (Thunk x) _ _) = x


-- | @unsafeMkClosure thk fun env@ constructs a Closure that
--
-- (1) wraps the thunk @thk@ and
--
-- (2) whose serialised representation consists of the @'Static'@ deserialiser
--    @fun@ and the serialised environment @env@.
--
-- This operation is cheap and does not require Template Haskell support,
-- but it is /unsafe/ because it relies on the programmer to ensure that
-- both closure representations evaluate to the same term.
{-# INLINE unsafeMkClosure #-}
unsafeMkClosure :: Thunk a -> Static (Env -> Thunk a) -> Env -> Closure a
unsafeMkClosure thk fun env = Closure thk fun env


-----------------------------------------------------------------------------
-- safely constructing Closures from closure abstractions

-- | Template Haskell transformation constructing a Closure from an expression.
-- The expression must either be a single static closure (in which case the
-- result is a /static/ Closure), or an application of a closure abstraction
-- to a tuple of variables.
-- See the tutorial below for how to use @mkClosure@.
mkClosure :: ExpQ -> ExpQ
mkClosure expQ = do
  let prf = "Control.Parallel.HdpH.Closure.mkClosure: "
  let err_msg = prf ++ "argument not of the form " ++
                "'static_closure' or 'closure_abstraction (var,...,var)'"
  let panic_msg = prf ++ "impossible case"
  exp <- expQ
  (name, free_vars, is_abs) <- case exp of
    AppE (VarE clo_abs_name) vars -> return (clo_abs_name,  vars, True)
    AppE (ConE clo_abs_name) vars -> return (clo_abs_name,  vars, True)
    VarE stat_clo_name            -> return (stat_clo_name, unit, False)
    ConE stat_clo_name            -> return (stat_clo_name, unit, False)
    _                             -> fail err_msg
  unless (isGlobalName name) $ fail err_msg
  unless (isVarTuple free_vars) $ fail err_msg
  ty <- typeClosureAbs (const panic_msg) name
  unless (isClosureAbsType ty == is_abs) $ fail err_msg
  let funQ = static name
  let envQ = appsE [global 'encodeEnv, return free_vars]
  let thkQ | is_abs    = expQ
           | otherwise = appsE [conE 'Thunk, expQ]   -- inject 'Thunk'
  appsE [conE 'Closure, thkQ, funQ, envQ]


-- | Template Haskell transformation constructing a family of Closures from an
-- expression. The family is indexed by location (that's what the suffix @Loc@
-- stands for).
-- The expression must either be a single static closure (in which case the
-- result is a family of /static/ Closures), or an application of a closure
-- abstraction to a tuple of variables.
-- See the tutorial below for how to use @mkClosureLoc@.
mkClosureLoc :: ExpQ -> ExpQ
mkClosureLoc expQ = do
  let prf = "Control.Parallel.HdpH.Closure.mkClosureLoc: "
  let err_msg = prf ++ "argument not of the form " ++
                "'static_closure' or 'closure_abstraction (var,...,var)'"
  let panic_msg = prf ++ "impossible case"
  exp <- expQ
  (name, free_vars, is_abs) <- case exp of
    AppE (VarE clo_abs_name) vars -> return (clo_abs_name,  vars, True)
    AppE (ConE clo_abs_name) vars -> return (clo_abs_name,  vars, True)
    VarE stat_clo_name            -> return (stat_clo_name, unit, False)
    ConE stat_clo_name            -> return (stat_clo_name, unit, False)
    _                             -> fail err_msg
  unless (isGlobalName name) $ fail err_msg
  unless (isVarTuple free_vars) $ fail err_msg
  ty <- typeClosureAbs (const panic_msg) name
  unless (isClosureAbsType ty == is_abs) $ fail err_msg
  let funQ = staticLoc name
  let envQ = appsE [global 'encodeEnv, return free_vars]
  let thkQ | is_abs    = expQ
           | otherwise = appsE [conE 'Thunk, expQ]   -- inject 'Thunk'
  loc <- newName "loc"
  lam1E (varP loc) $ appsE [conE 'Closure, thkQ, appsE [funQ, varE loc], envQ]


-----------------------------------------------------------------------------
-- Static deserializers

-- | Template Haskell transformation converting a static closure or closure
-- abstraction (given by its name) into a @'Static'@ deserialiser.
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

-- Called by 'static' if argument names a closure abstraction.
{-# INLINE mkStatic #-}
mkStatic :: (Serialize a)
         => (a -> Thunk b) -> String -> Static (Env -> Thunk b)
mkStatic clo_abs label =
  staticAs (clo_abs . decodeEnv) label

-- Called by 'static' if argument names a static closure.
{-# INLINE mkStatic_ #-}
mkStatic_ :: b -> String -> Static (Env -> Thunk b)
mkStatic_ stat_clo label =
  staticAs (const $ Thunk stat_clo) label   -- inject 'Thunk'


-- | Template Haskell transformation converting a static closure or closure
-- abstraction (given by its name) into a family of @'Static'@ deserialisers
-- indexed by location (that's what the suffix @Loc@ stands for).
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

-- Called by 'staticLoc' if argument names a closure abstraction.
{-# INLINE mkStaticLoc #-}
mkStaticLoc :: (Serialize a)
            => (a -> Thunk b) -> String -> (LocT b -> Static (Env -> Thunk b))
mkStaticLoc clo_abs label =
  \ loc -> staticAs (clo_abs . decodeEnv) $
             label ++ "{loc=" ++ show loc ++ "}"

-- Called by 'staticLoc' if argument names a static closure.
{-# INLINE mkStaticLoc_ #-}
mkStaticLoc_ :: b -> String -> (LocT b -> Static (Env -> Thunk b))
mkStaticLoc_ stat_clo label =
  \ loc -> staticAs (const $ Thunk stat_clo) $   -- inject 'Thunk'
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


-----------------------------------------------------------------------------
-- auxiliary stuff: various show operations

-- Template Haskell expression for empty tuple.
unit :: Exp
unit = ConE $ tupleDataName 0


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


-- Show first 'n' bytes of 'env'.
showEnvPrefix :: Int -> ByteString -> String
showEnvPrefix n env = showListUpto n (unpack env) ""


-- Show first 'n' list elements
showListUpto :: (Show a) => Int -> [a] -> String -> String
showListUpto _ []     = showString "[]"
showListUpto n (x:xs) = showString "[" . shows x . go (n - 1) xs
  where
    go _ [] = showString "]"
    go k (z:zs) | k > 0     = showString "," . shows z . go (k - 1) zs
                | otherwise = showString ",...]"
