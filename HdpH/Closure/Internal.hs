-- Closure representation, inspired by [1] and refined by [2].
--   [1] Epstein et al. "Haskell for the cloud". Haskell Symposium 2011.
--   [2] Maier, Trinder. "Implementing a High-level Distributed-Memory
--       Parallel Haskell in Haskell". IFL 2011.
-- Internal module necessary to satisfy Template Haskell stage restrictions.
--
-- Visibility: HdpH.Closure
-- Author: Patrick Maier <P.Maier@hw.ac.uk>
-- Created: 12 May 2011
--
-----------------------------------------------------------------------------

-- The module 'HdpH.Closure' implements /explicit closures/ as described in [2].
-- Due to Template Haskell stage restrictions, the module has to be split into
-- two halves. This is the half that that defines internals like the actual
-- closure representation and Template Haskell macros on it. The higher
-- level stuff (including a tutorial on expclicit closures) is in
-- module 'HdpH.Closure'.

{-# LANGUAGE TemplateHaskell #-}

module HdpH.Closure.Internal
  ( -- * source locations with phantom type attached
    LocT,             -- instances: Eq, Ord, Show
    here,             -- :: ExpQ

    -- * serialised environment
    Env,              -- synonym: Data.ByteString.Lazy.ByteString
    encodeEnv,        -- :: (Serialize a) => a -> Env
    decodeEnv,        -- :: (Serialize a) => Env -> a

    -- * Closure type constructor
    Closure,          -- instances: Show, NFData, Serialize

    -- * introducing and eliminating Closures
    unsafeMkClosure,  -- :: a -> Static (Env -> a) -> Env -> Closure a
    unClosure,        -- :: Closure a -> a

    -- * safe Closure construction
    mkClosure,        -- :: ExpQ -> ExpQ
    mkClosureLoc,     -- :: ExpQ -> ExpQ

    -- * Static deserializers
    static,           -- :: Name -> ExpQ
    staticLoc,        -- :: Name -> ExpQ
    static_,          -- :: Name -> ExpQ
    staticLoc_        -- :: Name -> ExpQ
  ) where

import Prelude
import Control.DeepSeq (NFData(rnf))
import Data.ByteString.Lazy (ByteString, unpack, foldl')
import Data.Serialize (Serialize)
import qualified Data.Serialize (put, get, encodeLazy, decodeLazy)
import Language.Haskell.TH
       (Exp(AppE, VarE, TupE, ConE), ExpQ,
        appsE, lam1E, conE, varE, global, litE, varP, stringL, tupleDataName)
import qualified Language.Haskell.TH as TH (Loc(..), location)
import Language.Haskell.TH.Syntax
       (Name(Name), NameFlavour(NameG, NameL), NameSpace(VarName), newName,
       pkgString, modString, occString)

import HdpH.Closure.Static (Static, unstatic, staticAs)


-----------------------------------------------------------------------------
-- source locations with phantom type attached

-- A value of type 'LocT a' is String representation of Haskell source location
-- (more precisely, the location of a Template Haskell slice). Additionally,
-- this location is annotated with a phantom type 'a', which will be used for
-- implementing a type discrimination scheme for overloaded Static terms.
newtype LocT a = LocT { unLocT :: String } deriving (Eq, Ord)

instance Show (LocT a) where
  show = unLocT

-- Template Haskell construct returning its own location when spliced.
here :: ExpQ
here = do
  loc <- TH.location
  appsE [conE 'LocT, litE $ stringL $ showLoc loc]


-----------------------------------------------------------------------------
-- serialised environments

-- A serialised environment is just a lazy bytestring.
type Env = ByteString

-- introducing an environment
encodeEnv :: (Serialize a) => a -> Env
encodeEnv = Data.Serialize.encodeLazy

-- eliminating an environment
decodeEnv :: (Serialize a) => Env -> a
decodeEnv env =
  case Data.Serialize.decodeLazy env of
    Right x  -> x
    Left msg -> error $ "HdpH.Internal.Closure.decodeEnv " ++
                        showEnvPrefix 20 env ++ ": " ++ msg


-----------------------------------------------------------------------------
-- 'Closure' type constructor

-- An explicit Closure (i.e. a term of type 'Closure a') maintains a dual
-- representation of an actual closure of type 'a'.
-- * The first argument is the actual closure value, the /thunk/.
-- * The second and third arguments comprise a serialisable representation
--   of the thunk, consisting of a (Static) function expecting a
--   (serialised) environment, plus such a (serialised) environment.
--
-- The former rep is used for computing with closures while the latter
-- is used for serialising and communicating closures across the network.
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

-- Eliminating a Closure (aka. un-Closure conversion) by returning its thunk
{-# INLINE unClosure #-}
unClosure :: Closure a -> a
unClosure (Closure thk _ _) = thk


-- Introducing a Closure (aka. Closure conversion)
-- NOTE: unsafe version; it is the programmer's job to ensure that
--       '(unstatic fun) env' and 'thk' evaluate to the same term.
{-# INLINE unsafeMkClosure #-}
unsafeMkClosure :: a -> Static (Env -> a) -> Env -> Closure a
unsafeMkClosure thk fun env = Closure thk fun env


-----------------------------------------------------------------------------
-- safely constructing Closures from closure abstractions

-- Template Haskell transformation converting a thunk into a Closure.
-- The thunk must either be a single toplevel closure (in which case the result
-- is a /static/ Closure), or an application of a toplevel closure
-- abstraction to a tuple of local variables.
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


-- Template Haskell transformation converting a thunk into a family of Closures
-- indexed by location (that's what the suffix 'Loc' stands for).
-- The thunk must either be a single toplevel closure (in which case the result
-- is a family of /static/ Closures), or an application of a toplevel closure
-- abstraction to a tuple of local variables.
mkClosureLoc :: ExpQ -> ExpQ
mkClosureLoc thkQ = do
  loc <- newName "loc"
  thk <- thkQ
  case thk of
    AppE (VarE clo_abs) free_vars
      | isGlobalVarName clo_abs && isLocalVars free_vars -> do
          let envQ = appsE [global 'encodeEnv, return free_vars]
          let funQ = staticLoc clo_abs
          lam1E (varP loc) $
            appsE [conE 'Closure, thkQ, appsE [funQ, varE loc], envQ]
    VarE clo
      | isGlobalVarName clo -> do
          let envQ = [| encodeEnv () |]
          let funQ = staticLoc_ clo
          lam1E (varP loc) $
            appsE [conE 'Closure, thkQ, appsE [funQ, varE loc], envQ]
    _ -> fail $ "HdpH.Internal.Closure.mkClosureLoc: argument not " ++
                "'globalVar' or 'globalVar (localVar_1,...,localVar_n)'"


-----------------------------------------------------------------------------
-- Static deserializers

-- Template Haskell transformation converting a toplevel closure abstraction
-- (given by its name) into a Static deserialiser.
static :: Name -> ExpQ
static name =
  case tryShowClosureAbs name of
    Just label -> appsE [global 'mkStatic, varE name, litE $ stringL label]
    Nothing    -> fail $ "HdpH.Internal.Closure.static: " ++
                         show name ++ " not a global variable name"

-- Called by 'static'.
{-# INLINE mkStatic #-}
mkStatic :: (Serialize a)
         => (a -> b) -> String -> Static (Env -> b)
mkStatic clo_abs label =
  staticAs (clo_abs . decodeEnv) label


-- Template Haskell transformation converting a static toplevel closure
-- (given by its name) into a Static deserialiser.
-- Note that a static closure ignores its empty environment (which is
-- what the suffix '_' is meant to signify).
static_ :: Name -> ExpQ
static_ name =
  case tryShowClosureAbs name of
    Just label -> appsE [global 'mkStatic_, varE name, litE $ stringL label]
    Nothing    -> fail $ "HdpH.Internal.Closure.static_: " ++
                         show name ++ " not a global variable name"

-- Called by 'static_'.
{-# INLINE mkStatic_ #-}
mkStatic_ :: b -> String -> Static (Env -> b)
mkStatic_ clo label =
  staticAs (const clo) $ "_/" ++ label


-- Template Haskell transformation converting a toplevel closure abstraction
-- (given by its name) into a family of Static deserialisers indexed by
-- location (that's what the suffix 'Loc' stands for).
staticLoc :: Name -> ExpQ
staticLoc name =
  case tryShowClosureAbs name of
    Just label -> appsE [global 'mkStaticLoc, varE name, litE $ stringL label]
    Nothing    -> fail $ "HdpH.Internal.Closure.staticLoc: " ++
                         show name ++ " not a global variable name"

-- Called by 'staticLoc'.
{-# INLINE mkStaticLoc #-}
mkStaticLoc :: (Serialize a)
            => (a -> b) -> String -> (LocT b -> Static (Env -> b))
mkStaticLoc clo_abs label =
  \ loc -> staticAs (clo_abs . decodeEnv) $
             label ++ "{loc=" ++ show loc ++ "}"


-- Template Haskell transformation converting a static toplevel closure
-- (given by its name) into a family of Static deserialisers indexed by
-- location (that's what the suffix 'Loc' stands for).
-- Note that a static closure ignores its empty environment (which is
-- what the suffix '_' is meant to signify).
staticLoc_ :: Name -> ExpQ
staticLoc_ name =
  case tryShowClosureAbs name of
    Just label -> appsE [global 'mkStaticLoc_, varE name, litE $ stringL label]
    Nothing    -> fail $ "HdpH.Internal.Closure.staticLoc_: " ++
                         show name ++ " not a global variable name"

-- Called by 'staticLoc_'.
{-# INLINE mkStaticLoc_ #-}
mkStaticLoc_ :: b -> String -> (LocT b -> Static (Env -> b))
mkStaticLoc_ clo label =
  \ loc -> staticAs (const clo) $
             "_/" ++ label ++ "{loc=" ++ show loc ++ "}"


-----------------------------------------------------------------------------
-- auxiliary stuff: operations involving variable names

-- Returns a String representation of the argument if it is a global variable,
-- Nothing otherwise.
{-# INLINE tryShowClosureAbs #-}
tryShowClosureAbs :: Name -> Maybe String
tryShowClosureAbs name =
  case name of
    Name occ (NameG VarName pkg mod) ->
      Just $ pkgString pkg ++ "/" ++ modString mod ++ "." ++ occString occ
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
isLocalVars exp = isLocalVar exp || isLocalVarTuple exp


-----------------------------------------------------------------------------
-- auxiliary stuff: various show operations

-- Show Template Haskell location.
-- Should be defined in module Language.Haskell.TH.
{-# INLINE showLoc #-}
showLoc :: TH.Loc -> String
showLoc loc = showsLoc loc ""
  where
    showsLoc loc = showString (TH.loc_package loc)  . showString "/" .
                   showString (TH.loc_module loc)   . showString ":" .
                   showString (TH.loc_filename loc) . showString "@" .
                   shows (TH.loc_start loc)         . showString "-" .
                   shows (TH.loc_end loc)


-- Show first 'n' bytes of 'env'.
showEnvPrefix :: Int -> Env -> String
showEnvPrefix n env = showListUpto n (unpack env) ""


-- Show first 'n' list elements
showListUpto :: (Show a) => Int -> [a] -> String -> String
showListUpto n []     = showString "[]"
showListUpto n (x:xs) = showString "[" . shows x . go (n - 1) xs
  where
    go _ [] = showString "]"
    go n (x:xs) | n > 0     = showString "," . shows x . go (n - 1) xs
                | otherwise = showString ",...]"


-----------------------------------------------------------------------------
-- misc auxiliary stuff

-- This instance should be part of module 'Data.ByteString.Lazy'.
-- It is required by the 'NFData' instance for Closures.
instance NFData ByteString where
  rnf = foldl' (\ _ -> rnf) ()
