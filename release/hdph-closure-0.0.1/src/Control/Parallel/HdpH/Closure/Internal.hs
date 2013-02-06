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

    -- introducing and eliminating Closures
    unsafeMkClosure,  -- :: a -> Static (Env -> a) -> Env -> Closure a
    unClosure,        -- :: Closure a -> a

    -- safe Closure construction
    mkClosure,        -- :: ExpQ -> ExpQ
    mkClosureLoc,     -- :: ExpQ -> ExpQ

    -- static deserializers
    static,           -- :: Name -> ExpQ
    staticLoc,        -- :: Name -> ExpQ
    static_,          -- :: Name -> ExpQ
    staticLoc_        -- :: Name -> ExpQ
  ) where

import Prelude
import Control.DeepSeq (NFData(rnf))
import Data.ByteString.Lazy (ByteString, unpack, foldl')
import Data.Functor ((<$>))
import Data.Serialize (Serialize)
import qualified Data.Serialize (put, get, encodeLazy, decodeLazy)
import Language.Haskell.TH
       (Exp(AppE, VarE, TupE, ConE), ExpQ, Lit,
        appsE, lam1E, conE, varE, global, litE, varP, stringL, tupleDataName)
import qualified Language.Haskell.TH as TH (Loc(..), location)
import Language.Haskell.TH.Syntax
       (Name(Name), NameFlavour(NameG, NameL), NameSpace(VarName, DataName),
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
newtype Env = Env ByteString deriving (Eq, Ord)

instance Show Env where
  showsPrec _ (Env env) = shows env

instance NFData Env where
  rnf (Env env) = foldl' (\ _ -> rnf) () env

instance Serialize Env where
  put (Env env) = Data.Serialize.put env
  get = Env <$> Data.Serialize.get

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
                   a                    -- actual thunk
                   (Static (Env -> a))  -- Static environment deserialiser
                   Env                  -- serialised environment


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
unClosure (Closure thk _ _) = thk


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
unsafeMkClosure :: a -> Static (Env -> a) -> Env -> Closure a
unsafeMkClosure thk fun env = Closure thk fun env


-----------------------------------------------------------------------------
-- safely constructing Closures from closure abstractions

-- | Template Haskell transformation constructing a Closure from a given thunk.
-- The thunk must either be a single toplevel closure (in which case the result
-- is a /static/ Closure), or an application of a toplevel closure
-- abstraction to a tuple of local variables.
-- See the tutorial below for how to use @mkClosure@.
mkClosure :: ExpQ -> ExpQ
mkClosure thkQ = do
  thk <- thkQ
  let funQ = case thk of
               AppE (VarE clo_abs_name) _ -> static clo_abs_name
               AppE (ConE clo_abs_name) _ -> static clo_abs_name
               VarE clo_name              -> static_ clo_name
               ConE clo_name              -> static_ clo_name
               _ -> error "mkClosure: impossible case"
  let envQ = case thk of
               AppE _ free_vars -> appsE [global 'encodeEnv, return free_vars]
               _                -> [| encodeEnv () |]
  let cloQ = appsE [conE 'Closure, thkQ, funQ, envQ]
  case thk of
    AppE (VarE clo_abs_name) free_vars
      | isGlobalVarName clo_abs_name && isLocalVars free_vars -> cloQ
    AppE (ConE clo_abs_name) free_vars
      | isGlobalDataName clo_abs_name && isLocalVars free_vars -> cloQ
    VarE clo_name
      | isGlobalVarName clo_name -> cloQ
    ConE clo_name
      | isGlobalDataName clo_name -> cloQ
    _ -> fail $ "Control.Parallel.HdpH.Closure.mkClosure: " ++
                "argument not of the form 'globalVarOrCon' or " ++
                "'globalVarOrCon (localVar_1,...,localVar_n)'"


-- | Template Haskell transformation constructing a family of Closures from a
-- given thunk. The family is indexed by location (that's what the suffix @Loc@
-- stands for).
-- The thunk must either be a single toplevel closure (in which case the result
-- is a family of /static/ Closures), or an application of a toplevel closure
-- abstraction to a tuple of local variables.
-- See the tutorial below for how to use @mkClosureLoc@.
mkClosureLoc :: ExpQ -> ExpQ
mkClosureLoc thkQ = do
  thk <- thkQ
  let funQ = case thk of
               AppE (VarE clo_abs_name) _ -> staticLoc clo_abs_name
               AppE (ConE clo_abs_name) _ -> staticLoc clo_abs_name
               VarE clo_name              -> staticLoc_ clo_name
               ConE clo_name              -> staticLoc_ clo_name
               _ -> error "mkClosureLoc: impossible case"
  let envQ = case thk of
               AppE _ free_vars -> appsE [global 'encodeEnv, return free_vars]
               _                -> [| encodeEnv () |]
  loc <- newName "loc"
  let cloQ = lam1E (varP loc) $
               appsE [conE 'Closure, thkQ, appsE [funQ, varE loc], envQ]
  case thk of
    AppE (VarE clo_abs_name) free_vars
      | isGlobalVarName clo_abs_name && isLocalVars free_vars -> cloQ
    AppE (ConE clo_abs_name) free_vars
      | isGlobalDataName clo_abs_name && isLocalVars free_vars -> cloQ
    VarE clo_name
      | isGlobalVarName clo_name -> cloQ
    ConE clo_name
      | isGlobalDataName clo_name -> cloQ
    _ -> fail $ "Control.Parallel.HdpH.Closure.mkClosureLoc: " ++
                "argument not of the form 'globalVarOrCon' or " ++
                "'globalVarOrCon (localVar_1,...,localVar_n)'"


-----------------------------------------------------------------------------
-- Static deserializers

-- | Template Haskell transformation converting a toplevel closure abstraction
-- (given by its name) into a @'Static'@ deserialiser.
-- See the tutorial below for how to use @static@.
static :: Name -> ExpQ
static name =
  case tryLabelClosureAbs name of
    Just (clo_abs, label) -> appsE [global 'mkStatic, clo_abs, litE label]
    Nothing -> fail $ "Control.Parallel.HdpH.Closure.static: " ++
                      show name ++ " not a global variable or constructor name"

-- Called by 'static'.
{-# INLINE mkStatic #-}
mkStatic :: (Serialize a)
         => (a -> b) -> String -> Static (Env -> b)
mkStatic clo_abs label =
  staticAs (clo_abs . decodeEnv) label


-- | Template Haskell transformation converting a static toplevel closure
-- (given by its name) into a @'Static'@ deserialiser.
-- Note that a static closure ignores its empty environment (which is
-- what the suffix @_@ is meant to signify).
-- See the tutorial below for how to use @static_@.
static_ :: Name -> ExpQ
static_ name =
  case tryLabelClosureAbs name of
    Just (clo, label) -> appsE [global 'mkStatic_, clo, litE label]
    Nothing -> fail $ "Control.Parallel.HdpH.Closure.static_: " ++
                      show name ++ " not a global variable or constructor name"

-- Called by 'static_'.
{-# INLINE mkStatic_ #-}
mkStatic_ :: b -> String -> Static (Env -> b)
mkStatic_ clo label =
  staticAs (const clo) $ "_/" ++ label


-- | Template Haskell transformation converting a toplevel closure abstraction
-- (given by its name) into a family of @'Static'@ deserialisers indexed by
-- location (that's what the suffix @Loc@ stands for).
-- See the tutorial below for how to use @staticLoc@.
staticLoc :: Name -> ExpQ
staticLoc name =
  case tryLabelClosureAbs name of
    Just (clo_abs, label) -> appsE [global 'mkStaticLoc, clo_abs, litE label]
    Nothing -> fail $ "Control.Parallel.HdpH.Closure.staticLoc: " ++
                      show name ++ " not a global variable or constructor name"

-- Called by 'staticLoc'.
{-# INLINE mkStaticLoc #-}
mkStaticLoc :: (Serialize a)
            => (a -> b) -> String -> (LocT b -> Static (Env -> b))
mkStaticLoc clo_abs label =
  \ loc -> staticAs (clo_abs . decodeEnv) $
             label ++ "{loc=" ++ show loc ++ "}"


-- | Template Haskell transformation converting a static toplevel closure
-- (given by its name) into a family of @'Static'@ deserialisers indexed by
-- location (that's what the suffix @Loc@ stands for).
-- Note that a static closure ignores its empty environment (which is
-- what the suffix @_@ is meant to signify).
-- See the tutorial below for how to use @staticLoc_@.
staticLoc_ :: Name -> ExpQ
staticLoc_ name =
  case tryLabelClosureAbs name of
    Just (clo, label) -> appsE [global 'mkStaticLoc_, clo, litE label]
    Nothing -> fail $ "Control.Parallel.HdpH.Closure.staticLoc_: " ++
                      show name ++ " not a global variable or constructor name"

-- Called by 'staticLoc_'.
{-# INLINE mkStaticLoc_ #-}
mkStaticLoc_ :: b -> String -> (LocT b -> Static (Env -> b))
mkStaticLoc_ clo label =
  \ loc -> staticAs (const clo) $
             "_/" ++ label ++ "{loc=" ++ show loc ++ "}"


-----------------------------------------------------------------------------
-- auxiliary stuff: operations involving variable names

-- Expects the argument to be a global variable or constructor name.
-- If so, returns a pair consisting of an expression (the closure abstraction
-- or static closure named by the argument) and a label representing the name;
-- otherwise returns Nothing.
{-# INLINE tryLabelClosureAbs #-}
tryLabelClosureAbs :: Name -> Maybe (ExpQ, Lit)
tryLabelClosureAbs name =
  let mkLabel pkg' mod' occ' = stringL $ pkgString pkg' ++ "/" ++
                                         modString mod' ++ "." ++
                                         occString occ'
    in case name of
         Name occ' (NameG VarName pkg' mod') ->
           Just (varE name, mkLabel pkg' mod' occ')
         Name occ' (NameG DataName pkg' mod') ->
           Just (conE name, mkLabel pkg' mod' occ')
         _ -> Nothing


-- True iff the argument is a global variable name.
{-# INLINE isGlobalVarName #-}
isGlobalVarName :: Name -> Bool
isGlobalVarName (Name _ (NameG VarName _ _)) = True
isGlobalVarName _                            = False

-- True iff the argument is a data constructor name.
{-# INLINE isGlobalDataName #-}
isGlobalDataName :: Name -> Bool
isGlobalDataName (Name _ (NameG DataName _ _)) = True
isGlobalDataName _                             = False


-- True iff the expression is a local variable.
{-# INLINE isLocalVar #-}
isLocalVar :: Exp -> Bool
isLocalVar (VarE (Name _ (NameL _))) = True
isLocalVar _                         = False

-- True iff the expression is a (possibly empty) tuple of local variables.
-- NOTE: Empty tuples are only properly recognized from GHC 7.4 onwards
--       (prior to 7.4 empty tuples were represented as unit values).
{-# INLINE isLocalVarTuple #-}
isLocalVarTuple :: Exp -> Bool
isLocalVarTuple (TupE exps) = all isLocalVar exps      -- non-empty tuples
isLocalVarTuple (ConE name) = name == tupleDataName 0  -- empty tuple
isLocalVarTuple _           = False

-- True iff the expression is a local variable or a tuple of local variables.
{-# INLINE isLocalVars #-}
isLocalVars :: Exp -> Bool
isLocalVars expr = isLocalVar expr || isLocalVarTuple expr


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
