-- Closure representation, inspired by
--   [1] Epstein et al. "Haskell for the cloud". Haskell Symposium 2011.
--
-- Visibility: public
-- Author: Patrick Maier <P.Maier@hw.ac.uk>
-- Created: 12 May 2011
--
-----------------------------------------------------------------------------

{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE ScopedTypeVariables #-}  -- req'd for phantom type annotations

module HdpH.Closure
  ( -- * serialised environment
    Env,       -- synonym: Data.ByteString.Lazy.ByteString
    encodeEnv, -- :: (Serialize a) => a -> Env
    decodeEnv, -- :: (Serialize a) => Env -> a

    -- * Closure type constructor
    Closure,   -- instances: Eq, Ord, Show, NFData, Serialize, Typeable

    -- * introducing and eliminating closures
    toClosure,        -- :: (DecodeStatic a) => a -> Closure a
    unsafeMkClosure,  -- :: a -> Static (Env -> a) -> Env -> Closure a
    unsafeMkClosure0, -- :: a -> Static (Env -> a) -> Closure a
    unClosure,        -- :: Closure a -> a

    -- * forcing closures
    forceClosure,  -- :: (NFData a, DecodeStatic a) => Closure a -> Closure a

    -- * categorical operations on function closures
    idClosure,   -- :: Closure (a -> a)
    compClosure, -- :: Closure (b -> c) -> Closure (a -> b) -> Closure (a -> c)
    mapClosure,  -- :: Closure (a -> b) -> Closure a -> Closure b

    -- * static deserialisers
    DecodeStatic(   -- context: Serialize, Typeable
      decodeStatic  -- :: Static (Env -> a)
    ),

    -- * Static declaration and registration;
    --   exported to top-level module 'HdpH' but not re-exported by 'HdpH'
    registerStatic  -- :: IO ()
  ) where

import Prelude hiding (error)
import Control.DeepSeq (NFData(rnf))
import Data.ByteString.Lazy as Lazy (ByteString)
import Data.Monoid (mconcat)
import Data.Serialize (Serialize)
import qualified Data.Serialize (put, get)
import Data.Typeable (Typeable, Typeable1, typeOf, typeOfDefault)

import HdpH.Internal.Misc (Void, encodeLazy, decodeLazy)
import HdpH.Internal.Static
       (Static, unstatic, staticAs, staticAsTD, declare, register)


-----------------------------------------------------------------------------
-- 'Static' declaration and registration

-- Req'd for calls to 'idClosure', 'mapClosure', 'compClosure', as well as
-- the static deserializer (of class 'DecodeStatic') for 'Closure'.
registerStatic :: IO ()
registerStatic =
  register $ mconcat
    [declare idClosureStatic,
     declare compClosureStatic,
     declare mapClosureStatic,
     declare (decodeStatic :: Static (Env -> Closure a))]


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
-- * The first constructor argument id the actual closure value.
-- * The second and third argument comprise a serialisable representation
--   of the closure value, consisting of a (static) function expecting a
--   (serialised) environment, plus such a serialised environment.
--
-- The former rep is used for computing with closures while the latter
-- is used for serialising and communicating closures across the network.
--
data Closure a = Closure
                   a                    -- actual closure value
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
--       (which is important for the semantics of 'HdpH.rput')
-- * A type is an instance of NFData iff it is an instance of Serialize.
-- * A value is forced by 'rnf' to the same extent it is forced by
--   'rnf . encode'.

-- NOTE: Phil is unhappy with this contract. He argues that 'rnf' should
--       fully force the closure (and thus evaluate further than 'rnf . encode'
--       would). His argument is essentially that 'rnf' should act on
--       explicit closures as it acts on actual closures. Which would mean
--       that 'NFData (Closure a)' can only be defined given a '(Serialize a)'
--       context.
--
--       Here is the killer argument against Phil's proposal: Because of the
--       type of 'rnf', it can only evaluate its argument as a side effect;
--       it cannot actually change its representation. However, forcing an
--       explicit closure must change its representation (at least if we
--       want to preserve the evaluation status across serialisation).

instance NFData (Closure a) where
  -- force all fields apart from the actual closure value
  rnf (Closure _ fun env) = rnf fun `seq` rnf env


instance Serialize (Closure a) where
  -- serialise all fields apart from the actual closure value
  put (Closure _ fun env) = Data.Serialize.put fun >>
                            Data.Serialize.put env

  -- deserialise all fields and lazily re-instate the actual closure value
  get = do fun <- Data.Serialize.get
           env <- Data.Serialize.get
           let val = (unstatic fun) env
           return $ Closure val fun env


instance Show (Closure a) where  -- for debugging only; show serialisable rep
  showsPrec _ (Closure _ fun env) = showString "Closure(" . shows fun .
                                    showString "," . shows env . showString ")"


-----------------------------------------------------------------------------
-- introducing and eliminating closures

-- Eliminating a closure (aka. un-closure conversion) by returning its value
unClosure :: Closure a -> a
unClosure (Closure val _ _) = val


-- introducing a closure (aka. closure conversion)
-- NOTE: unsafe version; '(unstatic fun) env' and 'val' must evaluate to same.
unsafeMkClosure :: a -> Static (Env -> a) -> Env -> Closure a
unsafeMkClosure val fun env = Closure val fun env

-- introducing a closure; variant of 'unsafeMkClosure' for empty environments
unsafeMkClosure0 :: a -> Static (Env -> a) -> Closure a
unsafeMkClosure0 val fun = Closure val fun env
  where env = encodeLazy ()  -- empty dummy environment


-- converting a value into a closure (forcing the value on serialisation)
toClosure :: (DecodeStatic a) => a -> Closure a
toClosure val = Closure val fun env
  where env = encodeLazy val
        fun = decodeStatic

-- Identities:
-- 'forall x . unClosure $ toClosure x == x'
-- 'forall x fun env . unClosure $ unsafeMkClosure x fun env == x'
-- 'forall x fun . unClosure $ unsafeMkClosure0 x fun == x'


-----------------------------------------------------------------------------
-- forcing closures

-- forcing a closure (ie. fully normalising the actual closure value);
-- note that 'forceClosure clo' does not have the same effect as
-- * 'rnf clo' (because 'forceClosure' changes the closure representation), or
-- * 'rnf clo1 where clo1 = toClosure $ unClosure clo' (because 'forceClosure'
--   does not force the serialised environment of its result).
forceClosure :: (NFData a, DecodeStatic a) => Closure a -> Closure a
forceClosure clo = rnf val' `seq` clo' where
  clo'@(Closure val' _ _) = toClosure $ unClosure clo
  -- NOTE: Order of eval of args of 'seq' already determined by dependencies;
  --       'pseq' would make no difference.


------------------------------------------------------------------------------
-- categorical operations on closures

-- identity wrapped in a closure
idClosure :: Closure (a -> a)
idClosure = unsafeMkClosure0 val fun
  where val = id
        fun = idClosureStatic

idClosureStatic :: Static (Env -> (a -> a))
idClosureStatic = staticAs (const id) "HdpH.Closure.idClosure"


-- composition of function closures
compClosure :: Closure (b -> c) -> Closure (a -> b) -> Closure (a -> c)
compClosure clo_g clo_f = unsafeMkClosure val fun env
  where val = unClosure clo_g . unClosure clo_f
        env = encodeLazy (clo_g, clo_f)
        fun = compClosureStatic

compClosureStatic :: Static (Env -> (a -> c))
compClosureStatic = staticAs
  (\ env -> let (clo_g, clo_f) = decodeLazy env
              in unClosure clo_g . unClosure clo_f)
  "HdpH.Closure.compClosure"


-- map a closure by applying a function closure (without forcing anything)
mapClosure :: Closure (a -> b) -> Closure a -> Closure b
mapClosure clo_f clo_x = unsafeMkClosure val fun env
  where val = unClosure clo_f $ unClosure clo_x
        env = encodeLazy (clo_f, clo_x)
        fun = mapClosureStatic

mapClosureStatic :: Static (Env -> b)
mapClosureStatic = staticAs
  (\ env -> let (clo_f, clo_x) = decodeLazy env
              in unClosure clo_f $ unClosure clo_x)
  "HdpH.Closure.mapClosure"


-----------------------------------------------------------------------------
-- static deserialisers

-- class of static deserialisers
class (Serialize a, Typeable a) => DecodeStatic a where
  decodeStatic :: Static (Env -> a)
  decodeStatic = staticAsTD decodeLazy "HdpH.Closure.decode" (undefined :: a)


-- 'DecodeStatic' instance for closures
instance DecodeStatic (Closure a)

-- NOTE: Every 'DecodeStatic' instance also needs to be registered.
--       In the case of the above instance for closures, 'registerStatic'
--       must include the following declaration:
--         'declare (decodeStatic :: Static (Env -> Closure a))'
