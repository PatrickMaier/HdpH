-- HdpH programming interface
--
-- Author: Patrick Maier, Rob Stewart
-----------------------------------------------------------------------------

{-# LANGUAGE GeneralizedNewtypeDeriving #-}  -- for 'GIVar' and 'NodeId'
{-# LANGUAGE TemplateHaskell #-}             -- for 'mkClosure', etc.

module Control.Parallel.HdpH
  ( -- $Intro

    -- * Par monad
    -- $Par_monad
    Par,        -- kind * -> *; instances: Functor, Monad
    runParIO_,     -- :: RTSConf -> Par () -> IO ()
    runParIO,      -- :: RTSConf -> Par a -> IO (Maybe a)

    -- * Operations in the Par monad
    -- $Par_ops
    done,       -- :: Par a
    myNode,     -- :: Par NodeId
    allNodes,   -- :: Par [NodeId]
    io,         -- :: IO a -> Par a
    eval,       -- :: a -> Par a
    force,      -- :: (NFData a) => a -> Par a
    fork,       -- :: Par () -> Par ()
    spark,      -- :: Closure (Par ()) -> Par ()
    pushTo,     -- :: Closure (Par ()) -> NodeId -> Par ()
    new,        -- :: Par (IVar a)
    put,        -- :: IVar a -> a -> Par ()
    get,        -- :: IVar a -> Par a
    tryGet,     -- :: IVar a -> Par (Maybe a)
    probe,      -- :: IVar a -> Par Bool
    glob,       -- :: IVar (Closure a) -> Par (GIVar (Closure a))
    rput,       -- :: GIVar (Closure a) -> Closure a -> Par ()

    -- * Locations
    NodeId,

    -- * Local and global IVars
    IVar,
    GIVar,
    at,

    -- * Explicit Closures
    module Control.Parallel.HdpH.Closure,

    -- * Runtime system configuration
    module Control.Parallel.HdpH.Conf,

    -- * This module's Static declaration
    declareStatic
  ) where

import Prelude hiding (error)
import Control.Concurrent.MVar (newEmptyMVar, putMVar, takeMVar)
import Control.DeepSeq (NFData, deepseq)
import Control.Monad (when)
import Data.Functor ((<$>))
import Data.IORef (newIORef, readIORef, writeIORef)
import Data.Monoid (mconcat)
import Data.Serialize (Serialize)

import Control.Parallel.HdpH.Conf                            -- re-export whole module
import Control.Parallel.HdpH.Closure hiding (declareStatic)  -- re-export almost whole module
import qualified Control.Parallel.HdpH.Closure as Closure (declareStatic)
import qualified Control.Parallel.HdpH.Internal.Comm as Comm
       (myNode, allNodes, isMain, shutdown)
import qualified Control.Parallel.HdpH.Internal.IVar as IVar (IVar, GIVar)
import Control.Parallel.HdpH.Internal.IVar
       (hostGIVar, newIVar, putIVar, getIVar, pollIVar, probeIVar,
        globIVar, putGIVar)
import qualified Control.Parallel.HdpH.Internal.Location as Location
       (NodeId, dbgStaticTab)
import Control.Parallel.HdpH.Internal.Scheduler
       (RTS, liftThreadM, liftSparkM, liftCommM, liftIO,
        schedulerID, mkThread, execThread, sendPUSH)
import qualified Control.Parallel.HdpH.Internal.Scheduler as Scheduler (run_)
import Control.Parallel.HdpH.Internal.Sparkpool (putSpark)
import Control.Parallel.HdpH.Internal.Threadpool (putThread, putThreads)
import Control.Parallel.HdpH.Internal.Type.Par (ParM(Par), Thread(Atom))


-----------------------------------------------------------------------------
-- Static declaration

-- | Static declaration of Static deserialisers used in explicit Closures
-- created or imported by this module.
-- This Static declaration must be imported by every main module using HdpH.
-- The imported Static declaration must be combined with the main module's own
-- Static declaration and registered; failure to do so may abort the program
-- at runtime.
declareStatic :: StaticDecl
declareStatic = mconcat
  [Closure.declareStatic,
   declare $(static 'rput_abs)]


-----------------------------------------------------------------------------
-- $Intro
-- HdpH (/Haskell distributed parallel Haskell/) is a Haskell DSL for shared-
-- and distributed-memory parallelism, implemented entirely in Haskell
-- (as supported by the GHC). HdpH is described in the following paper:
--
-- P. Maier, P. W. Trinder.
-- /Implementing a High-level Distributed-Memory Parallel Haskell in Haskell/.
-- IFL 2011.
--
-- HdpH executes programs written in a monadic embedded DSL for shared-
-- and distributed-memory parallelism.  
-- HdpH operates a distributed runtime system, scheduling tasks
-- either explicitly (controled by the DSL) or implicitly (by work stealing).
-- The runtime system distinguishes between nodes and schedulers.
-- A /node/ is an OS process running HdpH (that is, a GHC-compiled executable
-- built on top of the HdpH library), whereas a /scheduler/ is a Haskell IO
-- thread executing HdpH expressions (that is, 'Par' monad computation).
-- As a rule of thumb, a node should correspond to a machine in a network,
-- and a scheduler should correspond to a core in a machine.
--
-- The semantics of HdpH was developed with fault tolerance in mind (though
-- this version of HdpH is not yet fault tolerant). In particular, HdpH
-- allows the replication computations, and the racing of computations
-- against each other. The price to pay for these features is that HdpH
-- cannot enforce determinism.


-----------------------------------------------------------------------------
-- abstract Locations

-- | A 'NodeId' identifies a node (that is, an OS process running HdpH).
-- A 'NodeId' should be thought of as an abstract identifier which
-- instantiates the classes 'Eq', 'Ord', 'Show', 'NFData' and 'Serialize'.
newtype NodeId = NodeId Location.NodeId
                 deriving (Eq, Ord, NFData, Serialize)

-- Show instance (mainly for debugging)
instance Show NodeId where
  showsPrec _ (NodeId n) = showString "Node:". shows n


-----------------------------------------------------------------------------
-- abstract IVars and GIVars

-- | An IVar is a write-once one place buffer.
-- IVars are abstract; they can be accessed and manipulated only by
-- the operations 'put', 'get', 'tryGet', 'probe' and 'glob'.
newtype IVar a = IVar (IVar.IVar RTS a)


-- | A GIVar (short for /global/ IVar) is a globally unique handle referring
-- to an IVar.
-- Unlike IVars, GIVars can be compared and serialised.
-- They can also be written to remotely by the operation 'rput'.
newtype GIVar a = GIVar (IVar.GIVar RTS a)
                  deriving (Eq, Ord, NFData, Serialize)

-- Show instance (mainly for debugging)
instance Show (GIVar a) where
  showsPrec _ (GIVar gv) = showString "GIVar:" . shows gv


-- | Returns the node hosting the IVar referred to by the given GIVar.
-- This function being pure implies that IVars cannot migrate between nodes.
at :: GIVar a -> NodeId
at (GIVar gv) = NodeId $ hostGIVar gv


-----------------------------------------------------------------------------
-- abstract runtime system (don't export)

-- | Eliminate the 'RTS' monad down to 'IO' by running the given 'action';
-- aspects of the RTS's behaviour are controlled by the respective parameters
-- in the 'conf' argument.
runRTS_ :: RTSConf -> RTS () -> IO ()
runRTS_ = Scheduler.run_

-- | Return True iff this node is the root node.
isMainRTS :: RTS Bool
isMainRTS = liftCommM Comm.isMain

-- | Initiate RTS shutdown.
shutdownRTS :: RTS ()
shutdownRTS = liftCommM Comm.shutdown

-- Print global Static table to stdout, one entry a line.
printStaticTable :: RTS ()
printStaticTable =
  liftIO $ mapM_ putStrLn $
    "Static Table:" : map ("  " ++) showStaticTable


-----------------------------------------------------------------------------
-- $Par_monad
-- 'Par' is the monad for parallel computations in HdpH.
-- It is a continuation monad, similar to the one described in paper
-- /A monad for deterministic parallelism/
-- by S. Marlow, R. Newton, S. Peyton Jones (Haskell 2011).

-- | 'Par' is type constructor of kind @*->*@ and an instance of classes
-- 'Functor' and 'Monad'.
-- 'Par' is defined in terms of a parametric continuation monad 'ParM'
-- by plugging in 'RTS', the state monad of the runtime system.
-- Since neither 'ParM' nor 'RTS' are exported, 'Par' can be considered
-- abstract.
type Par = ParM RTS
-- A newtype would be nicer than a type synonym but the resulting
-- wrapping and unwrapping destroys readability (if it is at all possible,
-- eg. inside Closures).


-- | Eliminate the 'Par' monad by converting the given 'Par' action 'p'
-- into an 'RTS' action (to be executed on any one node of the distributed
-- runtime system).
runPar :: Par a -> RTS a
runPar p = do -- create an empty MVar expecting the result of action 'p'
              res <- liftIO $ newEmptyMVar

              -- fork 'p', combined with a write to above MVar;
              -- note that the starter thread (ie the 'fork') runs outwith
              -- any scheduler (and terminates quickly); the forked action
              -- (ie. 'p >>= ...') runs in a scheduler, however.
              execThread $ mkThread $ fork (p >>= io . putMVar res)

              -- block waiting for result
              liftIO $ takeMVar res


-- | Eliminates the 'Par' monad by executing the given parallel computation 'p',
-- including setting up and initialising a distributed runtime system
-- according to the configuration parameter 'conf'.
-- This function lives in the IO monad because 'p' may be impure,
-- for instance, 'p' may exhibit non-determinism.
-- Caveat: Though the computation 'p' will only be started on a single root
-- node, 'runParIO_' must be executed on every node of the distributed runtime
-- system du to the SPMD nature of HdpH.
-- Note that the configuration parameter 'conf' applies to all nodes uniformly;
-- at present there is no support for heterogeneous configurations.
runParIO_ :: RTSConf -> Par () -> IO ()
runParIO_ conf p =
  runRTS_ conf $ do isMain <- isMainRTS
                    when isMain $ do
                      -- print Static table
                      when (Location.dbgStaticTab <= debugLvl conf)
                        printStaticTable
                      runPar p
                      shutdownRTS


-- | Convenience: variant of 'runParIO_' which does return a result.
-- Caveat: The result is only returned on the root node; all other nodes
-- return 'Nothing'.
runParIO :: RTSConf -> Par a -> IO (Maybe a)
runParIO conf p = do res <- newIORef Nothing
                     runParIO_ conf (p >>= io . writeIORef res . Just)
                     readIORef res


-----------------------------------------------------------------------------
-- $Par_ops
-- These operations form the HdpH DSL, a low-level API of for parallel
-- programming across shared- and distributed-memory architectures.
-- For a more high-level API see module "Control.Parallel.HdpH.Strategies".

-- | Terminates the current thread.
done :: Par a
done = Par $ \ _c -> Atom (return Nothing)

-- lifting RTS into the Par monad (really a monadic map); don't export
atom :: RTS a -> Par a
atom m = Par $ \ c -> Atom (return . Just . c =<< m)

-- | Returns the node this operation is currently executed on.
myNode :: Par NodeId
myNode = NodeId <$> (atom $ liftCommM $ Comm.myNode)

-- | Returns a list of all nodes currently forming the distributed
-- runtime system.
allNodes :: Par [NodeId]
allNodes = map NodeId <$> (atom $ liftCommM $ Comm.allNodes)

-- | Lifts an IO action into the Par monad.
io :: IO a -> Par a
io = atom . liftIO

-- | Evaluates its argument to weak head normal form.
eval :: a -> Par a
eval x = atom (x `seq` return x)

-- | Evaluates its argument to normal form (as defined by 'NFData' instance).
force :: (NFData a) => a -> Par a
force x = atom (x `deepseq` return x)

-- | Creates a new thread, to be executed on the current node.
fork :: Par () -> Par ()
fork = atom . liftThreadM . putThread . mkThread

-- | Creates a spark, to be available for work stealing.
-- The spark may be converted into a thread and executed locally, or it may
-- be stolen by another node and executed there.
spark :: Closure (Par ()) -> Par ()
spark clo = atom (schedulerID >>= \ i -> liftSparkM $ putSpark i clo)

-- | Pushes a computation to the given node, where it is eagerly converted
-- into a thread and executed.
pushTo :: Closure (Par ()) -> NodeId -> Par ()
pushTo clo (NodeId n) = atom $ sendPUSH clo n

-- | Creates a new empty IVar.
new :: Par (IVar a)
new = IVar <$> atom (liftIO $ newIVar)

-- | Writes to given IVar (without forcing the value written).
put :: IVar a -> a -> Par ()
put (IVar v) a = atom $ liftIO (putIVar v a) >>=
                        liftThreadM . putThreads

-- | Reads from given IVar; blocks if the IVar is empty.
get :: IVar a -> Par a
get (IVar v) = Par $ \ c -> Atom $ liftIO (getIVar v c) >>=
                                   maybe (return Nothing) (return . Just . c)

-- | Reads from given IVar; does not block but returns 'Nothing' if IVar empty.
tryGet :: IVar a -> Par (Maybe a)
tryGet (IVar v) = atom $ liftIO (pollIVar v)

-- | Tests whether given IVar is empty or full; does not block.
probe :: IVar a -> Par Bool
probe (IVar v) = atom $ liftIO (probeIVar v)

-- | Globalises given IVar, returning a globally unique handle;
-- this operation is restricted to IVars of 'Closure' type.
glob :: IVar (Closure a) -> Par (GIVar (Closure a))
glob (IVar v) = GIVar <$> atom (schedulerID >>= \ i -> liftIO $ globIVar i v)

-- | Writes to (possibly remote) IVar denoted by given global handle;
-- this operation is restricted to write valueso of 'Closure' type.
rput :: GIVar (Closure a) -> Closure a -> Par ()
rput gv clo = pushTo $(mkClosure [| rput_abs (gv, clo) |]) (at gv)

-- write to locally hosted global IVar; don't export
{-# INLINE rput_abs #-}
rput_abs :: (GIVar (Closure a), Closure a) -> Par ()
rput_abs (GIVar gv, clo) = atom $ schedulerID >>= \ i ->
                                  liftIO (putGIVar i gv clo) >>=
                                  liftThreadM . putThreads
