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
{-# LANGUAGE ScopedTypeVariables #-}  -- req'd for phantom type annotations

module HdpH.Internal.Static
  ( -- * 'Static' type constructor
    Static,      -- instances: Eq, Ord, Show, NFData, Serialize, Typeable1

    -- * introducing 'Static' terms
    staticAs,    -- :: a -> String -> Static a
    staticAsTD,  -- :: (Typeable t) => a -> String -> t -> Static a

    -- * declaring 'Static' terms
    StaticDecl,  -- instances: Monoid, Show
    declare,     -- :: Static a -> StaticDecl

    -- * registering global 'Static' declaration
    register,    -- :: StaticDecl -> IO ()

    -- * eliminating 'Static' terms (via registered global 'Static' decl)
    unstatic     -- :: Static a -> a
  ) where

import Prelude hiding (error)
import Control.DeepSeq (NFData, rnf)
import Control.Monad (unless)
import Data.Functor ((<$>))
import Data.IORef (readIORef, atomicModifyIORef)
import qualified Data.Map as Map
       (lookup, empty, singleton, union, unions, toList, difference, null)
import Data.Monoid (Monoid(mempty, mappend, mconcat))
import Data.Serialize (Serialize)
import qualified Data.Serialize (put, get)
import Data.Typeable (Typeable, Typeable1, typeOf)
import System.IO.Unsafe (unsafePerformIO)
import Unsafe.Coerce (unsafeCoerce)

import HdpH.Internal.Location (error)
import HdpH.Internal.Misc (Void, AnyType(Any))
import HdpH.Internal.State.Static (sdRef)
import HdpH.Internal.Type.Static
       (StaticLabel(StaticLabel), name, typerep,
        Static(Static), label, value,
        StaticDecl(StaticDecl), unStaticDecl)


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
-- * A term 'stat' of type 'Static t' acts as serialisable reference to
--   a static term of type 't'.
--
-- * There are two ways of obtaining a term 'stat :: Stat t': By introducing
--   it with one of the smart constructors 'staticAs' or 'staticAsTD',
--   or by deserialising it. In the former case, 'stat' will have a /fat/
--   representation pairing a unique label and with the associated static term.
--   In the latter case, 'stat' will have a /thin/ representation consisting
--   solely of the unique label.
--
-- * The link between 'stat :: Static t' and its associated static term
--   'x :: t' is established by a registry mapping /registered/ 'Static' terms
--   to static terms, similar to the registry for global references.
--
-- * Unlike is the case for global references, the registry for 'Static' terms
--   allows only very limited updates; in particular, no entries can ever be
--   removed or over-written. The only updates allowed extend the registry
--   by registering new 'Static' terms.
--
-- * Another difference to global references is that the registry for 'Static'
--   terms must be uniform on all nodes. That is, all nodes must associate
--   any given registered 'Static' term to the same static term.
--
-- * Applications using this module (even indirectly) must go through two
--   distinct phases: The first phase introduces and registers all 'Static'
--   terms (of which, by their nature, there can only be a statically bounded
--   number) but does not eliminate any. The second phase may eliminate
--   'Static' terms (via 'unstatic') but does not register any new ones.


-----------------------------------------------------------------------------
-- How to register 'Static' terms
--
-- Every module which introduces 'Static' terms via 'staticAs' or 'staticAsTD'
-- must export an IO action 'registerStatic' for registering these terms.
--
-- To demonstrate the simplest example, suppose module 'M1' introduces two
-- 'Static' terms (as top-level variables, which is recommended but not
-- necessary)
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
-- > registerStatic = do register $ declare x_Static
-- >                     register $ declare f_Static
--
-- Or, the 'registerStatic' action can be coded more efficiently as follows
--
-- > registerStatic = register $ mconcat [declare x_Static, declare f_Static]
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
-- >                     register $ declare x_Static
--
-- Note that 'M2' may import 'M1' and so 'M2.registerStatic' may call
-- 'M1.registerStatic' again, after it was just called by 'M3.registerStatic'.
-- This is of no concern as only the first call to 'M1.registerStatic' has
-- an effect on the registry, later calls have no effect. Even concurrent
-- calls to 'registerStatic' are permissible.
--
-- The main module must declare an action 'registerStatic', similar to
-- 'M3.registerStatic' above. This action 'registerStatic' must be called
-- at the beginning of the 'main' function, before any call to 'unstatic'.
--
-- For concrete examples on how to register 'Static' terms see the
-- demonstrators in directory TEST/HdpH.


-----------------------------------------------------------------------------
-- 'Static' terms (abstract outwith this module)
-- NOTE: Static labels are hyperstrict, and 'Static' terms are hyperstrict
--       in their label part.

-- Constructs a 'StaticLabel' out of a fully qualified variable name 'nm'
-- and a String rep 'ty' of some (monomorphic) type (usually produced by
-- 'show . typeOf'). The type rep 'ty' is used to discriminate between
-- instances of polymorphic 'Static' values.
-- This constructor ensures that the resulting 'StaticLabel' is hyperstrict;
-- this constructor is not to be exported.
mkStaticLabel :: String -> String -> StaticLabel
mkStaticLabel nm ty =
  rnf nm `seq` rnf ty `seq` StaticLabel { name = nm, typerep = ty }


-- Constructs a 'Static' term from a given static label (without
-- associating the resulting term to a static value).
-- Ensures that the resulting 'Static' term is hyperstrict in the label part;
-- this constructor is not to be exported.
mkStatic :: StaticLabel -> Static a
mkStatic lbl =
  lbl `seq` Static { label = lbl, value = Nothing }


-- Constructs a 'Static' term associated with the given static value 'val'
-- and identified by the given name 'nm' and given discriminating type rep
-- (passed as dummy arg of type 't').
staticAsTD :: forall a t . (Typeable t) => a -> String -> t -> Static a
staticAsTD val nm _ =
  lbl `seq` Static { label = lbl, value = Just val }
    where lbl = mkStaticLabel nm (show $ typeOf (undefined :: t))


-- Constructs a 'Static' term associated with the given static value 'val'
-- and identified by the given name 'nm' only.
staticAs :: a -> String -> Static a
staticAs val nm = staticAsTD val nm (undefined :: Void)


-----------------------------------------------------------------------------
-- Eq/Ord/Show/NFData/Serialize instances for 'Static';
-- all these instances care only about the 'label' part of a 'Static' term

deriving instance Typeable1 Static


instance Eq (Static a) where
  stat1 == stat2 = label stat1 == label stat2

deriving instance Eq StaticLabel


instance Ord (Static a) where
  stat1 <= stat2 = label stat1 <= label stat2

deriving instance Ord StaticLabel


-- Show instance mainly for debugging
instance Show (Static a) where
  showsPrec _ stat = showString "Static:" . shows (label stat)

instance Show StaticLabel where
  showsPrec _ lbl =
    showString (name lbl) .
    showString "(" . showString (typerep lbl)  . showString ")"


instance NFData (Static a)  -- default inst suffices ('label' part hypestrict,
                            -- 'value' not to be forced)
instance NFData StaticLabel -- default inst suffices (due to hyperstrictness)


instance Serialize (Static a) where
  put = Data.Serialize.put . label
  get = mkStatic <$> Data.Serialize.get  -- 'mkStatic' ensures hyperstrictness

instance Serialize StaticLabel where
  put lbl = Data.Serialize.put (name lbl) >>
            Data.Serialize.put (typerep lbl)
  get = do nm <- Data.Serialize.get
           ty <- Data.Serialize.get
           return (mkStaticLabel nm ty)  -- 'mkStaticLabel' for hyperstrictness


-----------------------------------------------------------------------------
-- 'Static' declarations (abstract outwith this module)

-- Promotes the given 'Static' term (which must be in fat representation)
-- to a static declaration (by mapping the label to the associated static
-- value, existentially wrapped).
declare :: Static a -> StaticDecl
declare stat =
  case value stat of
    Just x  -> StaticDecl $ Map.singleton (label stat) (Any x)
    Nothing -> error $ "HdpH.Internal.Static.declare: no value for " ++
                       show stat


-- Static declarations form a monoid, at least if we assume that for each
-- 'Static' term 'stat' there is a unique associated static value 'x'.
-- (This assumption is fair if the labels of 'Static' terms are discriminated
-- either by their fully qualified names, or by their type reps (for
-- monomorphic types), and we assume that 'Static' terms within a module do 
-- not have conflicting declarations.) If there are conflicting declarations,
-- they will be silently accepted, but only one declaration will prevail
-- (which one depends on implementation details).
instance Monoid StaticDecl where
  mempty = StaticDecl Map.empty
  sd1 `mappend` sd2 =
    StaticDecl (unStaticDecl sd1 `Map.union` unStaticDecl sd2)
  mconcat = StaticDecl . Map.unions . map unStaticDecl


-- Show instance only for debugging; only shows domain of 'StaticDecl'
instance Show StaticDecl where
  showsPrec _ sd = showString "StaticDecl " .
                   shows (map fst $ Map.toList $ unStaticDecl sd)


-----------------------------------------------------------------------------
-- registering global 'Static' declaration

-- Registers the given 'Static' declaration as part of the implicit global
-- 'Static' declaration. Must be called becore any call to 'unstatic'.
-- Guarantees never to overwrite an already registered 'Static' term
-- (ie. only the first effort at registering any 'Static' term succeeds).
register :: StaticDecl -> IO ()
register sd = do
  sd0 <- readIORef sdRef
  let new_sd = StaticDecl (unStaticDecl sd `Map.difference` unStaticDecl sd0)
  unless (Map.null $ unStaticDecl new_sd) $
    atomicModifyIORef sdRef $ \ sd0 -> (sd0 `mappend` new_sd, ())


-----------------------------------------------------------------------------
-- eliminating 'Static' terms (via global 'Static' declaration)

-- Unwraps a 'Static' term 'stat', yielding the associated static value;
-- aborts with a runtime error if 'stat' has not been registered.
-- NOTE: This function depends on implicit state yet has pure type signature.
--       This is justified provided there is a clear phase distinction
--       between 'Static' registration and 'Static' elimination. That is,
--       all calls to 'register' must precede any calls to 'unstatic'.
unstatic :: Static a -> a
unstatic stat = unsafePerformIO $ do
  sd <- readIORef sdRef
  case Map.lookup (label stat) (unStaticDecl sd) of
    Just (Any x) -> return $ unsafeCoerce x
                    -- see below for an argument why unsafeCoerce is safe here
    Nothing      -> error $ "HdpH.Internal.Static.unstatic: " ++
                            show stat ++ " not registered"


-------------------------------------------------------------------------------
-- Why 'unsafeCoerce' in safe in 'unstatic'
--
-- * 'Static' terms can only be introduced by the smart constructors
--   'staticAs' and 'staticAsTD'.
--
-- * We assume that the label part of the fat representation of 'Static'
--   terms (which is the result of the smart constructors) is a /key/.
--   That is, whenever terms 'stat1 :: Static t1' and 'stat2 :: Static t2'
--   are introduced by 'staticAs' or 'staticAsTD' such that
--   'label stat1 == label stat2' (and hence 'stat1 == stat2') then
--   'value stat1 :: t1' is identical to 'value stat2 :: t2' (which
--   also implies that the type 't1' is identical to 't2'). This assumption
--   should hold if labels are fully qualified top-level names.
--
-- * Thus, we can safely super-impose (using 'unsafeCoerce') the phantom type
--   of a 'Static' term on to its associated static term.
