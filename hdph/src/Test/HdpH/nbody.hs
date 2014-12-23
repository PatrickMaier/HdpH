-- N-body simulation; simple parallelisation of all pairs algorithm
--
-- Author: Patrick Maier
-----------------------------------------------------------------------------

{-# LANGUAGE FlexibleInstances #-}  -- req'd for some ToClosure instances
{-# LANGUAGE TemplateHaskell #-}    -- req'd for mkClosure, etc

module Main where

import Prelude
import Control.Applicative ((<$>), (<*>))
import Control.Exception (evaluate)
import Control.Monad (replicateM, when, (>=>))
import Control.DeepSeq (NFData(..), deepseq)
import Data.List (stripPrefix)
import Data.List (delete, foldl', tails, transpose)
import Data.Monoid (mconcat)
import Data.Serialize (Serialize)
import qualified Data.Serialize (put, get)
import Data.Time.Clock (NominalDiffTime, diffUTCTime, getCurrentTime)
import System.Environment (getArgs)
import System.IO (stdout, stderr, hSetBuffering, BufferMode(..))
import System.Random (mkStdGen, setStdGen, randomIO)

import Control.Parallel.HdpH 
       (RTSConf(..), defaultRTSConf, updateConf,
        Par, runParIO,
        Node,
        myNode, allNodes, force, fork, new, get, put,
        Thunk(Thunk), mkClosure,
        ToClosure(locToClosure),
        static, StaticToClosure, staticToClosure,
        StaticDecl, declare, register, here)
import qualified Control.Parallel.HdpH as HdpH (declareStatic)
import Control.Parallel.HdpH.Strategies 
       (ForceCC(locForceCC), parMapNF, pushMapNF,
        StaticForceCC, staticForceCC)
import qualified Control.Parallel.HdpH.Strategies as Strategies (declareStatic)


-----------------------------------------------------------------------------
-- 3D vectors and scalars

type Scalar = Double

data Vector3 = V3 {-# UNPACK #-} !Double
                  {-# UNPACK #-} !Double
                  {-# UNPACK #-} !Double
             deriving (Eq, Show)

instance NFData Vector3 where
  rnf (V3 x y z) = rnf x `seq` rnf y `seq` rnf z

instance Serialize Vector3 where
  put (V3 x y z) = Data.Serialize.put x >>
                   Data.Serialize.put y >>
                   Data.Serialize.put z
  get = do x <- Data.Serialize.get
           y <- Data.Serialize.get
           z <- Data.Serialize.get
           return $ V3 x y z


randomVector3 :: IO Vector3
randomVector3 = do x <- randomIO
                   y <- randomIO
                   z <- randomIO
                   return $ V3 x y z


{-# INLINE vzero #-}
vzero :: Vector3
vzero = V3 0.0 0.0 0.0


-- vector sum and difference
{-# INLINE (.+.) #-}
{-# INLINE (.-.) #-}
(.+.), (.-.) :: Vector3 -> Vector3 -> Vector3
(V3 x1 x2 x3) .+. (V3 y1 y2 y3) = V3 (x1 + y1) (x2 + y2) (x3 + y3)
(V3 x1 x2 x3) .-. (V3 y1 y2 y3) = V3 (x1 - y1) (x2 - y2) (x3 - y3)

{-# INLINE vsum #-}
vsum :: [Vector3] -> Vector3
vsum vs = foldl' (.+.) vzero vs


-- scalar multiplication
{-# INLINE (*.) #-}
(*.) :: Scalar -> Vector3 -> Vector3
k *. (V3 x1 x2 x3) = V3 (k * x1) (k * x2) (k * x3)


-- inner product
{-# INLINE (.*.) #-}
(.*.) :: Vector3 -> Vector3 -> Scalar
(V3 x1 x2 x3) .*. (V3 y1 y2 y3) = (x1 * y1) + (x2 * y2) + (x3 * y3)


-----------------------------------------------------------------------------
-- N-body data representation

-- a body consists of position and mass (and position is key)
data Body = Body {
              pos  :: {-# UNPACK #-} !Vector3,  -- position [m]
              mass :: {-# UNPACK #-} !Scalar }  -- mass [kg]
            deriving (Show)

instance Eq Body where
  body1 == body2 = pos body1 == pos body2

instance NFData Body where
  rnf body = rnf (pos body) `seq` rnf (mass body)

instance Serialize Body where
  put body = Data.Serialize.put (pos body) >>
             Data.Serialize.put (mass body)
  get = do x <- Data.Serialize.get
           m <- Data.Serialize.get
           return $ Body { pos = x, mass = m }


-- velocity [m/s]
type Vel = Vector3

-- differential velocity (aka acceleration) [m/s]
type DeltaVel = Vector3


-- The N-body problem is represented by two vectors (lists of the same length)
-- of bodies and velocities
type NBody = [Body]
type NVel  = [Vel]
type NBodyConf = (NBody, NVel)

-- Differential velocities for a N-body problems (again a vector)
type NDeltaVel = [DeltaVel]


-- Generate a random N-body instance (fully forced)
randomNBodyConf :: Int -> IO NBodyConf
randomNBodyConf n = do
  bodies <- replicateM n (Body <$> randomVector3 <*> randomIO)
  vs     <- replicateM n randomVector3
  return $ forceNF $ (bodies, vs)


-----------------------------------------------------------------------------
-- energy innate to a N-body configuration

-- Newton's gravitational constant 
g :: Scalar
g = 6.674/100000000000  -- 6.674 * 10^(-11) N m^2 / kg^2


energy :: NBodyConf -> Scalar
energy (bodies, vs) =
  sum (zipWith eKin bodies vs) + sum (map ePots (tails bodies))
    where
      ePots []            = 0
      ePots (body:bodys) = sum (map (ePot body) bodys)

eKin :: Body -> Vel -> Scalar
eKin body v = 0.5 * mass body * (v .*. v)

ePot :: Body -> Body -> Scalar
ePot body1 body2 = g * mass body1 * mass body2 / dist
                     where
                       dist = sqrt (d .*. d)
                       d = pos body1 .-. pos body2


-----------------------------------------------------------------------------
-- sequential N-body computation

-- time step ("delta t") of 1 ms
dt :: Scalar
dt = 0.001


-- softening factor (avoiding distances becoming too small)
eps :: Scalar
eps = 0.01


-- advance all bodies by 1 time step; input and output are chunks of the
-- given N-body problem (not lists of independent N-body problems);
-- outermost 'map' may be parallelised
advance :: [NBodyConf] -> [NBodyConf]
advance confs = forceNF confs'
  where
    bss = map fst confs
    confs' = map (\ (bodies, vs) ->
                    unzip $ zipWith3 update bodies vs $ dvs bodies bss) confs

-- update velocity and position of bodies
update :: Body -> Vel -> DeltaVel -> (Body, Vel)
update body v delta_v = (body', v')
                          where
                            body' = body { pos = pos body .+. (dt *. v') }
                            v' = v .+. delta_v


-- differential velocities of bodies due to gravitational influence by sources;
-- the length of the output vector matches the length of the first argument;
-- innermost 'map' may be parallelised
dvs :: NBody -> [[Body]] -> NDeltaVel
dvs bodies source_chunks =
  map vsum $ transpose $ map (part_dvs bodies) source_chunks

-- differential velocities of bodies due to gravitational influence by a
-- chunk of sources; length of output vector matches length of first argument;
-- outermost 'map' may be parallelised
part_dvs :: NBody -> [Body] -> NDeltaVel
part_dvs bodies source_chunk =
  map (\ body -> vsum (map (dv body) source_chunk)) bodies

-- differential velocity of body due to gravitational influence by source
dv :: Body -> Body -> DeltaVel
dv body source
  | body == source = vzero
  | otherwise      = (dt * g * mass source / (distSquared * dist)) *. d
                       where
                         d = pos body .-. pos source
                         distSquared = d .*. d + eps
                         dist = sqrt distSquared


-----------------------------------------------------------------------------
-- parallel N-body computation, shared memory

-- advance all bodies by 1 time step; same API as 'advance' (except for
-- the 'Par') but uses coarse-grain data parallelism
par_advance :: [NBodyConf] -> Par [NBodyConf]
par_advance confs =
  forkMapNF compute_chunk confs
    where
      bss = map fst confs
      compute_chunk (bodies, vs) =
        return $ unzip $ zipWith3 update bodies vs $ dvs bodies bss


-----------------------------------------------------------------------------
-- parallel N-body computation, distributed memory (sparking)

-- advance all bodies by 1 time step; same API as 'advance' (except for
-- the 'Par') but uses dist-mem data parallelism (parMap)
dist_advance :: [NBodyConf] -> Par [NBodyConf]
dist_advance confs =
  forkMapNF compute_chunk confs
    where
      bss = map fst confs
      compute_chunk (bodies, vs) =
        unzip <$> zipWith3 update bodies vs <$> dist_dvs bodies bss

-- differential velocities of bodies due to gravitational influence by sources;
-- same API as 'dvs' (except for the 'Par') but uses dist-mem parMap
dist_dvs :: NBody -> [[Body]] -> Par NDeltaVel
dist_dvs bodies source_chunks =
  map vsum <$> transpose <$>
    parMapNF $(mkClosure [| part_dvs_abs bodies |]) source_chunks

part_dvs_abs :: NBody -> Thunk ([Body] -> NDeltaVel)
part_dvs_abs bodies = Thunk $ part_dvs bodies


-----------------------------------------------------------------------------
-- parallel N-body computation, distributed memory (pushing)

-- advance all bodies by 1 time step; same API as 'advance' (except for
-- the 'Par') but uses dist-mem data parallelism (pushMap)
dist_advance_push :: [NBodyConf] -> Par [NBodyConf]
dist_advance_push confs = do
  nodes <- allNodes
  me <- myNode
  let other_nodes = case delete me nodes of { [] -> nodes; others -> others }
  forkMapNF (compute_chunk $ other_nodes) confs
    where
      bss = map fst confs
      compute_chunk nodes (bodies, vs) =
        unzip <$> zipWith3 update bodies vs <$> dist_dvs_push nodes bodies bss

-- differential velocities of bodies due to gravitational influence by sources;
-- same API as 'dvs' (except for the 'Par') but uses dist-mem pushMap
dist_dvs_push :: [Node] -> NBody -> [[Body]] -> Par NDeltaVel
dist_dvs_push nodes bodies source_chunks =
  map vsum <$> transpose <$>
    pushMapNF nodes $(mkClosure [| part_dvs_abs bodies |]) source_chunks


-----------------------------------------------------------------------------
-- shared memory task farm

forkMapNF :: (NFData b) => (a -> Par b) -> [a] -> Par [b]
forkMapNF f = mapM spawn >=> mapM get
                where
                  spawn x = do v <- new
                               fork (f x >>= force >>= put v)
                               return v


-----------------------------------------------------------------------------
-- chunking up lists; inverse of 'chunk n' is 'concat'

chunk :: Int -> [a] -> [[a]]
chunk _ [] = []
chunk n xs = ys : chunk n zs where (ys,zs) = splitAt n xs


-----------------------------------------------------------------------------
-- auxiliary functions

-- time an IO action
timeIO :: IO a -> IO (a, NominalDiffTime)
timeIO action = do t0 <- getCurrentTime
                   x <- action
                   t1 <- getCurrentTime
                   return (x, diffUTCTime t1 t0)


-- strict iteration
iterate' :: Int -> (a -> a) -> a -> a
iterate' n f x | n <= 0    = x
               | otherwise = let fx = f x in iterate' (n-1) f $! fx


-- strict iteration in a monad
iterateM' :: (Monad m) => Int -> (a -> m a) -> a -> m a
iterateM' n f x | n <= 0    = return x
                | otherwise = do { fx <- f x; iterateM' (n-1) f $! fx }


-- "deep" evaluation
forceNF :: (NFData a) => a -> a
forceNF x = x `deepseq` x


-----------------------------------------------------------------------------
-- Static declaration (just before 'main')

-- Empty splice; TH hack to make all environment abstractions visible.
$(return [])

instance ToClosure [Body] where locToClosure = $(here)
instance ToClosure [Vector3] where locToClosure = $(here)
instance ForceCC [Vector3] where locForceCC = $(here)

declareStatic :: StaticDecl
declareStatic =
  mconcat
    [HdpH.declareStatic,
     Strategies.declareStatic,
     declare (staticToClosure :: StaticToClosure [Body]),
     declare (staticToClosure :: StaticToClosure [Vector3]),
     declare (staticForceCC :: StaticForceCC [Vector3]),
     declare $(static 'part_dvs_abs)]


-----------------------------------------------------------------------------
-- initialisation, argument processing and 'main'

-- initialize random number generator
initrand :: Int -> IO ()
initrand seed = do
  when (seed /= 0) $ do
    setStdGen (mkStdGen seed)


-- parse runtime system config options (+ seed for random number generator)
-- abort if there is an error
parseOpts :: [String] -> IO (RTSConf, Int, [String])
parseOpts args = do
  either_conf <- updateConf args defaultRTSConf
  case either_conf of
    Left err_msg             -> error $ "parseOpts: " ++ err_msg
    Right (conf, [])         -> return (conf, 0, [])
    Right (conf, arg':args') ->
      case stripPrefix "-rand=" arg' of
        Just s  -> return (conf, read s, args')
        Nothing -> return (conf, 0,      arg':args')


-- parse (optional) arguments in this order: 
-- * version to run
-- * #bodies (randomly generated)
-- * #simulation steps
-- * size of chunks (evaluated sequentially, default = #bodies)
parseArgs :: [String] -> (Int, Int, Int, Int)
parseArgs []     = (defVers, defBodies, defSteps, defBodies)
parseArgs (s:ss) =
  let go :: Int -> [String] -> (Int, Int, Int, Int)
      go v []           = (v, defBodies, defSteps, defBodies)
      go v [s1]         = (v, read s1,   defSteps, read s1)
      go v [s1,s2]      = (v, read s1,   read s2,  read s1)
      go v (s1:s2:s3:_) = (v, read s1,   read s2,  read s3)
  in case stripPrefix "v" s of
       Just s' -> go (read s') ss
       Nothing -> go defVers (s:ss)


defVers, defBodies, defSteps :: Int
defVers   =    0
defBodies = 1024
defSteps  =   20


main :: IO ()
main = do
  hSetBuffering stdout LineBuffering
  hSetBuffering stderr LineBuffering
  register declareStatic
  opts_args <- getArgs
  (conf, seed, args) <- parseOpts opts_args
  let (version, n, steps, chunk_size) = parseArgs args
  initrand seed
  -- on every node: generate random N-body instance, compute initial energy
  putStrLn $ "Generating random " ++ show n ++ "-body instance"
  conf0 <- randomNBodyConf n
  putStrLn $ "Computing initial energy E0"
  energy0 <- evaluate (energy conf0)
  putStrLn $ "E0 = " ++ show energy0 ++ "J"
  -- chunking up the initial config (lazily)
  let (bs0, vs0) = conf0
  let confs0 = zip (chunk chunk_size bs0) (chunk chunk_size vs0)
  case version of
      0 -> do (confs, t) <- timeIO $ evaluate
                              (iterate' steps advance confs0)
              let (bss, vss) = unzip confs
              let conf' = (concat bss, concat vss)
              putStrLn $ "Computing energy drift"
              deltaE <- evaluate (energy conf' - energy0)
              putStrLn $
                "{v0} nbody " ++ show n ++ " " ++ show steps ++
                " --> deltaE = " ++ show deltaE ++ "J {runtime=" ++
                show t ++ "}"
      1 -> do (output, t) <- timeIO $ evaluate =<< runParIO conf
                               (iterateM' steps par_advance confs0)
              case output of
                Just confs -> do let (bss, vss) = unzip confs
                                 let conf' = (concat bss, concat vss)
                                 putStrLn $ "Computing energy drift"
                                 deltaE <- evaluate (energy conf' - energy0)
                                 putStrLn $
                                   "{v1 chunksize=" ++ show chunk_size ++
                                   "} nbody " ++ show n ++ " " ++ show steps ++
                                   " --> deltaE = " ++ show deltaE ++
                                   "J {runtime=" ++ show t ++ "}"
                Nothing     -> return ()
      2 -> do (output, t) <- timeIO $ evaluate =<< runParIO conf
                               (iterateM' steps dist_advance confs0)
              case output of
                Just confs -> do let (bss, vss) = unzip confs
                                 let conf' = (concat bss, concat vss)
                                 putStrLn $ "Computing energy drift"
                                 deltaE <- evaluate (energy conf' - energy0)
                                 putStrLn $
                                   "{v2 chunksize=" ++ show chunk_size ++
                                   "} nbody " ++ show n ++ " " ++ show steps ++
                                   " --> deltaE = " ++ show deltaE ++
                                   "J {runtime=" ++ show t ++ "}"
                Nothing     -> return ()
      3 -> do (output, t) <- timeIO $ evaluate =<< runParIO conf
                               (iterateM' steps dist_advance_push confs0)
              case output of
                Just confs -> do let (bss, vss) = unzip confs
                                 let conf' = (concat bss, concat vss)
                                 putStrLn $ "Computing energy drift"
                                 deltaE <- evaluate (energy conf' - energy0)
                                 putStrLn $
                                   "{v3 chunksize=" ++ show chunk_size ++
                                   "} nbody " ++ show n ++ " " ++ show steps ++
                                   " --> deltaE = " ++ show deltaE ++
                                   "J {runtime=" ++ show t ++ "}"
                Nothing     -> return ()
      _ -> return ()
