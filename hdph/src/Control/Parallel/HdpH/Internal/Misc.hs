-- Misc auxiliary types and functions that should probably be in other modules.
--
-- Author: Patrick Maier
-------------------------------------------------------------------------------

{-# LANGUAGE GADTs #-}               -- for existential types
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE FlexibleContexts #-}    -- for Forkable instances
{-# LANGUAGE FlexibleInstances #-}

module Control.Parallel.HdpH.Internal.Misc
  ( -- * existential wrapper type
    AnyType(..),   -- no instances

    -- * monads supporting forking of threads
    Forkable(      -- context: (Monad m) => Forkable m
      fork,        -- ::        m () -> m Control.Concurrent.ThreadId
      forkOn       -- :: Int -> m () -> m Control.Concurrent.ThreadId
    ),

    -- * continuation monad with stricter bind
    Cont(..),      -- instances: Functor, Monad

    -- * rotate a list (to the left)
    rotate,        --  :: Int -> [a] -> [a]

    -- * return prefix of a list until predicate is satisfied.
    takeWeakUntil, --  :: (a -> Bool) -> [a] -> [a]

    -- * force spine of given list
    forceSpine,    --  :: [a] -> [a]

    -- * random permutation of given list
    shuffle,       --  :: [a] -> IO [a]

    -- * decode ByteStrings (without error reporting)
    decode,        -- :: Serialize a => Strict.ByteString -> a
    decodeLazy,    -- :: Serialize a => Lazy.ByteString -> a

    -- * encode ByteStrings (companions to the decoders above)
    encode,        -- :: Serialize a => a -> Strict.ByteString
    encodeLazy,    -- :: Serialize a => a -> Lazy.ByteString

    -- * encode/decode lists of bytes
    encodeBytes,   -- :: Serialize a => a -> [Word8]
    decodeBytes,   -- :: Serialize a => [Word8] -> a

    -- * destructors of Either values
    fromLeft,      -- :: Either a b -> a
    fromRight,     -- :: Either a b -> b

    -- * splitting a list
    splitAtFirst,  -- :: (a -> Bool) -> [a] -> Maybe ([a], a, [a])

    -- * To remove an Eq element from a list
    rmElems,       -- :: Eq a => a -> [a] -> [a]

    -- * action servers
    Action,        -- synonym: IO ()
    ActionServer,  -- abstract, no instances
    newServer,     -- :: IO ActionServer
    killServer,    -- :: ActionServer -> IO ()
    reqAction,     -- :: ActionServer -> Action -> IO ()

    -- * timing IO actions
    timeIO         -- :: IO a -> IO (a, NominalDiffTime)
  ) where

import Prelude hiding (error)
import Control.Applicative (Applicative, pure, (<*>))
import Control.Concurrent (ThreadId)
import qualified Control.Concurrent (forkIO, forkOn, killThread)
import Control.Concurrent.Chan (Chan, newChan, writeChan, readChan)
import Control.Monad (join, ap)
import Control.Monad.Reader (ReaderT, runReaderT, ask)
import Control.Monad.Trans (lift)
import qualified Data.ByteString 
       as Strict (ByteString, unpack)
import qualified Data.ByteString.Lazy
       as Lazy (ByteString, pack, unpack)
import Data.Functor ((<$>))
import Data.Serialize (Serialize)
import qualified Data.Serialize (encode, decode, encodeLazy, decodeLazy)
import Data.Time.Clock (NominalDiffTime, diffUTCTime, getCurrentTime)
import Data.Word (Word8)
import System.Random (randomRIO)

import Control.Parallel.HdpH.Internal.Location (error)


-------------------------------------------------------------------------------
-- Functionality missing in Data.List

rotate :: Int -> [a] -> [a]
rotate _ [] = []
rotate n xs = zipWith const (drop n $ cycle xs) xs

takeWeakUntil :: (a -> Bool) -> [a] -> [a]
takeWeakUntil _ []                 =  []
takeWeakUntil p (x:xs) | p x       =  [x]
                       | otherwise =  x : takeWeakUntil p xs

-- Forces the spine of a list.
forceSpine :: [a] -> [a]
forceSpine xs = walk xs `seq` xs
  where
    walk []     = ()
    walk (_:zs) = walk zs


-------------------------------------------------------------------------------
-- Random shuffle of a list (inspired by blog post from apfelmus, 2009)

-- Fair merge of random lists.
merge :: IO [a] -> IO [a] -> IO [a]
merge rxs rys = do
  xs <- rxs
  ys <- rys
  merge' (length xs, xs) (length ys, ys)
    where
      merge' (_,   [])         (_,   ys)         = return ys
      merge' (_,   xs)         (_,   [])         = return xs
      merge' (nxs, xs@(x:xs')) (nys, ys@(y:ys')) = do
        k <- randomRIO (1, nxs + nys)  -- selection weighted by size
        if k <= nxs
          then (x:) <$> merge' (nxs - 1, xs') (nys,     ys)
          else (y:) <$> merge' (nxs,     xs)  (nys - 1, ys')

-- Generates a random permutation in O(n log n).
shuffle :: [a] -> IO [a]
shuffle xs | n < 2     = return xs
           | otherwise = merge (shuffle l) (shuffle r)
                           where
                             n = length xs
                             (l,r) = splitAt (n `div` 2) xs


-------------------------------------------------------------------------------
-- Functionality missing in Data.Serialize

encode :: Serialize a => a -> Strict.ByteString
encode = Data.Serialize.encode

decode :: Serialize a => Strict.ByteString -> a
decode bs =
  case Data.Serialize.decode bs of
    Right x  -> x
    Left msg -> error $ "HdpH.Internal.Misc.decode " ++
                         showPrefix 10 bs ++ ": " ++ msg

encodeLazy :: Serialize a => a -> Lazy.ByteString
encodeLazy = Data.Serialize.encodeLazy

decodeLazy :: Serialize a => Lazy.ByteString -> a
decodeLazy bs =
  case Data.Serialize.decodeLazy bs of
    Right x  -> x
    Left msg -> error $ "HdpH.Internal.Misc.decodeLazy " ++
                        showPrefixLazy 10 bs ++ ": " ++ msg

decodeBytes :: Serialize a => [Word8] -> a
decodeBytes = decodeLazy . Lazy.pack

encodeBytes :: Serialize a => a -> [Word8]
encodeBytes = Lazy.unpack . Data.Serialize.encodeLazy


showPrefix :: Int -> Strict.ByteString -> String
showPrefix n bs = showListUpto n (Strict.unpack bs) ""

showPrefixLazy :: Int -> Lazy.ByteString -> String
showPrefixLazy n bs = showListUpto n (Lazy.unpack bs) ""

showListUpto :: (Show a) => Int -> [a] -> String -> String
showListUpto _ []     = showString "[]"
showListUpto n (x:xs) = showString "[" . shows x . go (n - 1) xs
  where
    go _ [] = showString "]"
    go k (z:zs) | k > 0     = showString "," . shows z . go (k - 1) zs
                | otherwise = showString ",...]"


-------------------------------------------------------------------------------
-- Existential type (serves as wrapper for values in heterogenous Maps)

data AnyType :: * where
  Any :: a -> AnyType


-------------------------------------------------------------------------------
-- Split a list at the first occurence of the given predicate;
-- the witness to the splitting occurence is stored as the middle element.

splitAtFirst :: (a -> Bool) -> [a] -> Maybe ([a], a, [a])
splitAtFirst p xs = let (left, rest) = break p xs in
                    case rest of
                      []           -> Nothing
                      middle:right -> Just (left, middle, right)


-------------------------------------------------------------------------------
-- Destructors for Either values

fromLeft :: Either a b -> a
fromLeft (Left x) = x
fromLeft _        = error "HdpH.Internal.Misc.fromLeft: wrong constructor"

fromRight :: Either a b -> b
fromRight (Right y) = y
fromRight _         = error "HdpH.Internal.Misc.fromRight: wrong constructor"


-------------------------------------------------------------------------------
-- Remove an Eq element from a list

rmElems' :: Eq a => a -> [a] -> [a]
rmElems' deleted xs = [ x | x <- xs, x /= deleted ]
rmElems :: Eq a => [a] -> [a] -> [a]
rmElems [] xs = xs
rmElems [y] xs = rmElems' y xs
rmElems (y:ys) xs = rmElems ys (rmElems' y xs)


-----------------------------------------------------------------------------
-- Forkable class and instances; adapted from Control.Concurrent.MState

class (Monad m) => Forkable m where
  fork   ::        m () -> m ThreadId
  forkOn :: Int -> m () -> m ThreadId

instance Forkable IO where
  fork   = Control.Concurrent.forkIO
  forkOn = Control.Concurrent.forkOn
  -- NOTE: 'forkOn' may cause massive variations in performance.

instance (Forkable m) => Forkable (ReaderT i m) where
  fork       action = do state <- ask
                         lift $ fork $ runReaderT action state
  forkOn cpu action = do state <- ask
                         lift $ forkOn cpu $ runReaderT action state


-----------------------------------------------------------------------------
-- Continuation monad with stricter bind; adapted from Control.Monad.Cont

newtype Cont r a = Cont { runCont :: (a -> r) -> r }

instance Functor (Cont r) where
    fmap f m = Cont $ \c -> runCont m (c . f)

-- The Monad instance is where we differ from Control.Monad.Cont,
-- the difference being the use of strict application ($!).
instance Monad (Cont r) where
    return a = Cont $ \ c -> c $! a
    m >>= k  = Cont $ \ c -> runCont m $ \ a -> runCont (k $! a) c

instance Applicative (Cont r) where
    pure  = return
    (<*>) = ap


-----------------------------------------------------------------------------
-- Action server

-- Actions are computations of type 'IO ()', and an action server is nothing
-- but a thread receiving actions over a channel and executing them one after
-- the other. Note that actions may block for a long time (eg. delay for 
-- several seconds), in which case the server itself is blocked (which is
-- intended).

type Action = IO ()
data ActionServer = ActionServer (Chan Action) ThreadId

newServer :: IO ActionServer
newServer = do trigger <- newChan
               tid <- Control.Concurrent.forkIO $ server trigger
               return (ActionServer trigger tid)

killServer :: ActionServer -> IO ()
killServer (ActionServer _ tid) = Control.Concurrent.killThread tid

reqAction :: ActionServer -> Action -> IO ()
reqAction (ActionServer trigger _) = writeChan trigger

server :: Chan Action -> IO ()
server trigger = do join (readChan trigger)
                    server trigger


-----------------------------------------------------------------------------
-- Timing an IO action

timeIO :: IO a -> IO (a, NominalDiffTime)
timeIO action = do t0 <- getCurrentTime
                   x <- action
                   t1 <- getCurrentTime
                   return (x, diffUTCTime t1 t0)
