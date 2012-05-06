-- 'Static' support, borrowing from
--   [1] Epstein et al. "Haskell for the cloud". Haskell Symposium 2011.
--
-- Visibility: HdpH.Internal
-- Author: Patrick Maier <P.Maier@hw.ac.uk>
-- Created: 13 May 2011
--
-----------------------------------------------------------------------------

-- That 'Static' type constructor as proposed in [1] is not yet supported
-- by GHC (as of version 7.2.1). This module provides a workaround that
-- necessarily differs in some aspects. However, the type constructor 'Static'
-- itself and its eliminator 'unstatic' work exactly as described in [1].
--
-- Differences between 'HdpH.Internal.Static' and a builtin 'Static' are:
-- * 'Static' terms must be explicitly named (via 'staticAs' or 'staticAsTD')
-- * 'Static' terms must be explicitly declared and registered before use
-- * Static values must either be parametrically polymorphic (those introduced
--   by 'staticAs'), or use a sufficiently monomorphic and 'Typeable' type
--   discriminator (those introduced by 'staticAsTD'); not all combinations
--   of parametric and ad-hoc polymorphism may be admissible.


{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE ScopedTypeVariables #-}  -- req'd for type annot in 'staticAsTD'

module HdpH.Internal.Static
  ( -- * 'Static' type constructor
    Static,      -- instances: Eq, Ord, Show, NFData, Serialize, Typeable1

    -- * introducing 'Static' terms
    staticAs,    -- :: a -> String -> Static a
    staticAsTD,  -- :: (Typeable t) => a -> String -> t -> Static a

    -- * eliminating 'Static' terms
    unstatic,    -- :: Static a -> a

    -- * globally registering 'Static' terms
    register     -- :: Static a -> IO ()
  ) where

import Prelude hiding (error)
import Control.DeepSeq (NFData, deepseq)
import Control.Monad (unless)
import Data.Bits (shiftL, xor)
import Data.Functor ((<$>))
import Data.Int (Int32)
import Data.IORef (readIORef, atomicModifyIORef)
import Data.List (foldl')
import qualified Data.Map as Map (member, lookup, insert)
import Data.Serialize (Serialize)
import qualified Data.Serialize (put, get)
import Data.Typeable (Typeable, Typeable1, typeOf)
import System.IO.Unsafe (unsafePerformIO)
import Unsafe.Coerce (unsafeCoerce)

import HdpH.Internal.Location (error)
import HdpH.Internal.Misc (AnyType(Any))
import HdpH.Internal.State.Static (sdRef)
import HdpH.Internal.Type.Static
       (StaticLabel(StaticLabel), hash, name, typerep,
        Static(Static), label, unstatic,
        StaticDecl)


-----------------------------------------------------------------------------
-- Key facts about 'Static'
--
-- * We say that a term 't' /captures/ a variable 'x' if 'x' occurs free
--   in 't' and 'x' is not declared at the top-level. (That is, this notion
--   of capture is sensitive to the context.)
--
-- * We call a term 't' /static/ if it could be declared at the top-level,
--   that is if does not capture any variables.
--
-- * In principle, a term 'stat' of type 'Static t' acts as serialisable
--   reference to a static term of type 't'. However, here we realise
--   terms of type 'Static t' as a pair consisting of such a serialisable
--   reference (a unique label) and the refered-to static term itself.
--
-- * Only the serialisable reference is actually serialised when a term 'stat'
--   of type 'Static t' is serialised. The link between that reference and
--   the associated 'Static' term 'stat' is established by a registry mapping
--   labels to /registered/ 'Static' terms, similar to the registry for
--   global references.
--
-- * There are two ways of obtaining a term 'stat :: Stat t': By introducing
--   it with one of the smart constructors 'staticAs' or 'staticAsTD',
--   or by deserialising it. The latter does require a lookup in the registry,
--   hence it will fail (by producing a special 'Static' /error/ term) if
--   the serialised reference is not registered.
--
-- * Unlike is the case for global references, the registry for 'Static' terms
--   allows only very limited updates; in particular, no entries can ever be
--   removed or over-written. The only updates allowed extend the registry
--   by registering new 'Static' terms.
--
-- * Another difference to global references is that the registry for 'Static'
--   terms must be uniform on all nodes. That is, all nodes must associate
--   any given registered reference to the same 'Static' term.
--
-- * Applications using this module (even indirectly) must go through two
--   distinct phases: The first phase introduces and registers all 'Static'
--   terms (of which, by their nature, there can only be a statically bounded
--   number) but does not deserialise any 'Static' terms. The second phase
--   may deserialise 'Static' terms but does not register any new ones.


-----------------------------------------------------------------------------
-- How to register 'Static' terms
--
-- Every module which introduces 'Static' terms via 'staticAs' or 'staticAsTD'
-- must export an IO action 'registerStatic' for registering these terms.
--
-- To demonstrate the simplest example, suppose module 'M1' introduces two
-- 'Static' terms (as top-level variables, which is recommended but not
-- necessary).
-- 
-- > x_Static :: Static Int
-- > x_Static = staticAs 42 "Fully_Qualified_Name_Of_M1.x"
-- >
-- > f_Static :: Static ([a] -> Int)
-- > f_Static = staticAs f "Fully_Qualified_Name_Of_M1.f"
-- >   where
-- >     f []     = 0
-- >     f (_:xs) = 1 + f xs
--
-- Then 'M1' ought to export
--
-- > registerStatic :: IO ()
-- > registerStatic = do register x_Static
-- >                     register f_Static
--
-- Now suppose a module 'M3' imports 'M1'. It doesn't matter whether 'M3'
-- imports the 'Static' terms of 'M1' directly; any function exported by
-- 'M1' may depend on its 'Static' terms being registered, so 'M3' must
-- ensure that registration happens. Suppose further that 'M3' also imports
-- a module 'M2' which requires some other 'Static' terms to be registered,
-- and that 'M3' itself introduces a term 'x_Static :: Static Bool'.
-- In this case, 'M3' ought to export the following 'registerStatic' action
--
-- > registerStatic :: IO ()
-- > registerStatic = do M1.registerStatic
-- >                     M2.registerStatic
-- >                     register x_Static
--
-- Note that 'M2' may import 'M1' and so 'M2.registerStatic' may call
-- 'M1.registerStatic' again, after it was just called by 'M3.registerStatic'.
-- This is of no concern as only the first call to 'M1.registerStatic' has
-- an effect on the registry, later calls have no effect. Even concurrent
-- calls to 'registerStatic' are permissible.
--
-- The main module must declare an action 'registerStatic', similar to
-- 'M3.registerStatic' above. This action 'registerStatic' must be called
-- at the beginning of the 'main' function, before calling any function
-- that might deserialise a 'Static' term.
--
-- The module 'HdpH.Closure' will offer more convenient support for
-- introducing and registering 'Static' terms linked to explicit closures.


-----------------------------------------------------------------------------
-- 'Static' terms (abstract outwith this module)
-- NOTE: Static labels are hyperstrict, and 'Static' terms are hyperstrict
--       in their label part.

-- Constructs a 'StaticLabel' out of a fully qualified variable name 'nm'
-- and a String rep 'ty' of some (monomorphic) type (usually produced by
-- 'show . typeOf', or else the empty string). The type rep 'ty' is used
-- to discriminate between instances of polymorphic 'Static' values.
-- This constructor ensures that the resulting 'StaticLabel' is hyperstrict;
-- this constructor is not to be exported. 
-- NOTE: Hyperstrictness is guaranteed by the strictness flags in the data
--       declaration of 'StaticLabel'. For the 'hash' field this already
--       implies full normalisation, computing the hash also fully normalises
--       the two remaining fields.
mkStaticLabel :: String -> String -> StaticLabel
mkStaticLabel nm ty = StaticLabel { hash = h, name = nm, typerep = ty }
                        where
                          h = hashString nm `hashStringWithSalt` ty


-- Constructs a 'Static' term associated with the given static value 'val'
-- and identified by the given name 'nm' and given discriminating type rep
-- (passed as dummy arg of type 't').
-- NOTE: Strictness flag on 'label' forces that field to WHNF (which conincides
--       with NF due to hyperstrictness of StaticLabel).
staticAsTD :: forall a t . (Typeable t) => a -> String -> t -> Static a
staticAsTD val nm _ =
  Static { label = lbl, unstatic = val }
    where lbl = mkStaticLabel nm (show $ typeOf (undefined :: t))


-- Constructs a 'Static' term associated with the given static value 'val'
-- and identified by the given name 'nm' only.
-- NOTE: Strictness as with 'staticAsTD'.
staticAs :: a -> String -> Static a
staticAs val nm =
  Static { label = lbl, unstatic = val }
    where lbl = mkStaticLabel nm ""


-----------------------------------------------------------------------------
-- Eq/Ord/Show/NFData instances for 'Static';
-- all these instances care only about the 'label' part of a 'Static' term

deriving instance Typeable1 Static


instance Eq (Static a) where
  stat1 == stat2 = label stat1 == label stat2

instance Eq StaticLabel where
  lbl1 == lbl2 = hash lbl1    == hash lbl2 &&  -- compare 'hash' first
                 name lbl1    == name lbl2 &&
                 typerep lbl1 == typerep lbl2


instance Ord (Static a) where
  stat1 <= stat2 = label stat1 <= label stat2

instance Ord StaticLabel where
  compare lbl1 lbl2 = case compare (hash lbl1) (hash lbl2) of -- cmp 'hash' 1st
                        LT -> LT
                        GT -> GT
                        EQ -> case compare (name lbl1) (name lbl2) of
                          LT -> LT
                          GT -> GT
                          EQ  -> compare (typerep lbl1) (typerep lbl2)


-- Show instance mainly for debugging
instance Show (Static a) where
  showsPrec _ stat = showString "Static:" . shows (label stat)

instance Show StaticLabel where
  showsPrec _ lbl =
    showString (name lbl) .
    if null (typerep lbl)
      then showString ""
      else showString "(" . showString (typerep lbl)  . showString ")"


instance NFData (Static a)  -- default inst suffices ('label' part hypestrict,
                            -- associated static value not to be forced)
instance NFData StaticLabel -- default inst suffices (due to hyperstrictness)


-----------------------------------------------------------------------------
-- Serialize instance for 'Static'

instance Serialize (Static a) where
  put = Data.Serialize.put . label
  get = resolve <$> Data.Serialize.get  
        -- NOTES: 
        -- * Hyperstrictness is enforced by 'resolve'.
        -- * Deserialisation always succeeds in returning a 'Static' value,
        --   however, that value may be 'staticError'.

instance Serialize StaticLabel where
  put lbl = Data.Serialize.put (hash lbl) >>
            Data.Serialize.put (name lbl) >>
            Data.Serialize.put (typerep lbl)
  get = do h  <- Data.Serialize.get
           nm <- Data.Serialize.get
           ty <- Data.Serialize.get
           return $ StaticLabel {hash = h, name = nm, typerep = ty}
           -- NOTE: Hyperstrictness not enforced here, because return value 
           --       is discarded after passing through 'resolve' above.


-- Resolves a label obtained from deserialisation into a 'Static' value
-- by looking up the label in the global 'Static' registry. Returns either
-- the registered 'Static', or the special value 'staticError'.
resolve :: StaticLabel -> Static a
resolve lbl =
  case Map.lookup lbl (unsafePerformIO $ readIORef sdRef) of
    Just (Any stat) -> unsafeCoerce stat
                       -- see below for arguments why unsafeCoerce is safe here
    Nothing         -> staticError lbl


-- Special 'Static' value (which cannot be produced by the exported
-- constructors) returned if 'resolve' encounters an unregistered label.
-- Eliminating this 'Static' value will result in an error.
staticError :: StaticLabel -> Static a
staticError lbl =
  Static { label    = StaticLabel { hash = 0, name = "", typerep = "" },
           unstatic = error msg }
             where
               msg = "HdpH.Internal.Static: " ++ show lbl ++ " not registered"


-- Why 'unsafeCoerce' in safe in 'resolve':
--
-- * 'Static' terms can only be introduced by the smart constructors
--   'staticAs' and 'staticAsTD'.
--
-- * We assume that the label part of 'Static' terms (as constructed by the
--   smart constructors) is a /key/.  That is, whenever terms
--   'stat1 :: Static t1' and 'stat2 :: Static t2' are introduced by
--   'staticAs' or 'staticAsTD' such that 'label stat1 == label stat2' then
--   'unstatic stat1 :: t1' is identical to 'unstatic stat2 :: t2' (which
--   also implies that the type 't1' is identical to 't2'). This assumption
--   should hold if labels are fully qualified top-level names.
--
-- * Thus, we can safely super-impose (using 'unsafeCoerce') the return type
--   'Static a' of 'resolve' on the returned 'Static' term. This type will
--   have been dictated by the calling context of 'resolve', ie. by the
--   calling context of deserialisation.


-----------------------------------------------------------------------------
-- globally registering 'Static' terms

-- Registers the given 'Static' term as part of the implicit global 'Static'
-- declaration. Must be called becore any attempts to deserialise 'Static'
-- terms. Guarantees never to overwrite an already registered'Static' term
-- (ie. only first try at registering a 'Static' term succeeds).
register :: Static a -> IO ()
register stat = do
  sd0 <- readIORef sdRef
  unless (Map.member (label stat) sd0) $
    atomicModifyIORef sdRef $ \ sd ->
      (Map.insert (label stat) (Any stat) sd, ())


-----------------------------------------------------------------------------
-- auxiliary stuff: hashing strings; adapted from Data.Hashable.
-- NOTE: Hashing fully evaluates the String argument as a side effect.

hashStringWithSalt :: Int32 -> String -> Int32
hashStringWithSalt = foldl' (\ h ch -> h `combine` toEnum (fromEnum ch))
                       where
                         combine :: Int32 -> Int32 -> Int32
                         combine h1 h2 = (h1 + h1 `shiftL` 5) `xor` h2

hashString :: String -> Int32
hashString = hashStringWithSalt defaultSalt
               where
                 defaultSalt = 5381
