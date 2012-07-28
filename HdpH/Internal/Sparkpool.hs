-- Spark pool and fishing
--
-- Visibility: HdpH, HdpH.Internal
-- Author: Patrick Maier <P.Maier@hw.ac.uk>
-- Created: 02 Jul 2011
--
-----------------------------------------------------------------------------

{-# LANGUAGE ScopedTypeVariables #-}  -- req'd for type annotations

module HdpH.Internal.Sparkpool
  ( -- * spark pool monad
    SparkM,      -- synonym: SparkM m = ReaderT <State m> CommM
    run,         -- :: RTSConf -> ActionServer -> Sem -> SparkM m a -> CommM a
    liftCommM,   -- :: Comm a -> SparkM m a
    liftIO,      -- :: IO a -> SparkM m a

    -- * blocking and unblocking idle schedulers
    blockSched,  -- :: SparkM m ()
    wakeupSched, -- :: Int -> SparkM m ()

    -- * local (ie. scheduler) access to spark pool
    getSpark,    -- :: Int -> SparkM m (Maybe (Spark m))
    putSpark,    -- :: Int -> Spark m -> SparkM m ()

    -- * messages
    Msg(..),         -- instances: Show, NFData, Serialize

    -- * handle messages related to fishing
    dispatch,        -- :: Msg m -> SparkM m ()
    handleFISH,      -- :: Msg m -> SparkM m ()
    handleSCHEDULE,  -- :: Msg m -> SparkM m ()
    handleNOWORK,    -- :: Msg m -> SparkM m ()

    -- * access to stats data
    readPoolSize,      -- :: SparkM m Int
    readFishSentCtr,   -- :: SparkM m Int
    readSparkRcvdCtr,  -- :: SparkM m Int
    readMaxSparkCtr,   -- :: SparkM m Int
    readSparkGenCtr,   -- :: SparkM m Int
    readSparkConvCtr   -- :: SparkM m Int
  ) where

import Prelude hiding (error)
import Control.Concurrent (threadDelay)
import Control.DeepSeq (NFData, rnf)
import Control.Monad (unless, when, replicateM_)
import Control.Monad.Reader (ReaderT, runReaderT, ask)
import Control.Monad.Trans (lift)
import Data.Functor ((<$>))
import Data.IORef (IORef, newIORef, readIORef, writeIORef, atomicModifyIORef)
import Data.Serialize (Serialize)
import qualified Data.Serialize (put, get)
import Data.Set (Set)
import qualified Data.Set as Set (size, fromList, singleton, notMember)
import Data.Word (Word8)
import System.Random (randomRIO)

import HdpH.Conf
       (RTSConf(maxHops, maxFish, minSched, minFishDly, maxFishDly))
import HdpH.Internal.Comm (CommM)
import qualified HdpH.Internal.Comm as Comm
       (liftIO, send, nodes, myNode, allNodes)
import HdpH.Internal.Data.Deque
       (DequeIO, emptyIO, pushFrontIO, pushBackIO, popFrontIO, popBackIO,
        lengthIO, maxLengthIO)
import HdpH.Internal.Data.Sem (Sem)
import qualified HdpH.Internal.Data.Sem as Sem (wait, signal)
import HdpH.Internal.Location (NodeId, dbgMsgSend, dbgSpark, error)
import qualified HdpH.Internal.Location as Location (debug)
import HdpH.Internal.Misc (encodeLazy, ActionServer, reqAction)
import HdpH.Internal.Type.Par (Spark)


-----------------------------------------------------------------------------
-- SparkM monad

-- 'SparkM m' is a reader monad sitting on top of the 'CommM' monad;
-- the parameter 'm' abstracts a monad (cf. module HdpH.Internal.Type.Par).
type SparkM m = ReaderT (State m) CommM


-- spark pool state (mutable bits held in IORefs and the like)
data State m =
  State {
    s_conf       :: RTSConf,               -- config data
    s_pool       :: DequeIO (Spark m),     -- actual spark pool
    s_sparkOrig  :: IORef (Maybe NodeId),  -- origin of most recent spark recvd
    s_fishing    :: IORef Bool,            -- True iff FISH outstanding
    s_noWork     :: ActionServer,          -- for clearing "FISH outstndg" flag
    s_idleScheds :: Sem,                   -- semaphore for idle schedulers
    s_fishSent   :: IORef Int,             -- #FISH sent
    s_sparkRcvd  :: IORef Int,             -- #sparks received
    s_sparkGen   :: IORef Int,             -- #sparks generated
    s_sparkConv  :: IORef Int }            -- #sparks converted


-- Eliminates the 'SparkM' layer by executing the given 'SparkM' action on
-- an empty spark pool; expects a config data, an action server (for
-- clearing "FISH outstanding" flag) and a semaphore (for idle schedulers).
run :: RTSConf -> ActionServer -> Sem -> SparkM m a -> CommM a
run conf noWorkServer idleSem action = do
  -- set up spark pool state
  pool      <- Comm.liftIO $ emptyIO
  sparkOrig <- Comm.liftIO $ newIORef Nothing
  fishing   <- Comm.liftIO $ newIORef False
  fishSent  <- Comm.liftIO $ newIORef 0
  sparkRcvd <- Comm.liftIO $ newIORef 0
  sparkGen  <- Comm.liftIO $ newIORef 0
  sparkConv <- Comm.liftIO $ newIORef 0
  let s0 = State { s_conf       = conf,
                   s_pool       = pool,
                   s_sparkOrig  = sparkOrig,
                   s_fishing    = fishing,
                   s_noWork     = noWorkServer,
                   s_idleScheds = idleSem,
                   s_fishSent   = fishSent,
                   s_sparkRcvd  = sparkRcvd,
                   s_sparkGen   = sparkGen,
                   s_sparkConv  = sparkConv }
  -- run monad
  runReaderT action s0


-- Lifting lower layers.
liftCommM :: CommM a -> SparkM m a
liftCommM = lift

liftIO :: IO a -> SparkM m a
liftIO = liftCommM . Comm.liftIO


-----------------------------------------------------------------------------
-- access to state

getPool :: SparkM m (DequeIO (Spark m))
getPool = s_pool <$> ask

readPoolSize :: SparkM m Int
readPoolSize = getPool >>= liftIO . lengthIO

getSparkOrigHist :: SparkM m (IORef (Maybe NodeId))
getSparkOrigHist = s_sparkOrig <$> ask

readSparkOrigHist :: SparkM m (Maybe NodeId)
readSparkOrigHist = getSparkOrigHist >>= liftIO . readIORef

updateSparkOrigHist :: NodeId -> SparkM m ()
updateSparkOrigHist mostRecentOrigin = do
  sparkOrigHistRef <- getSparkOrigHist 
  liftIO $ writeIORef sparkOrigHistRef (Just mostRecentOrigin)

getFishingFlag :: SparkM m (IORef Bool)
getFishingFlag = s_fishing <$> ask

getNoWorkServer :: SparkM m ActionServer
getNoWorkServer = s_noWork <$> ask
  
getIdleSchedsSem :: SparkM m Sem
getIdleSchedsSem = s_idleScheds <$> ask

getFishSentCtr :: SparkM m (IORef Int)
getFishSentCtr = s_fishSent <$> ask

readFishSentCtr :: SparkM m Int
readFishSentCtr = getFishSentCtr >>= readCtr

getSparkRcvdCtr :: SparkM m (IORef Int)
getSparkRcvdCtr = s_sparkRcvd <$> ask

readSparkRcvdCtr :: SparkM m Int
readSparkRcvdCtr = getSparkRcvdCtr >>= readCtr

getSparkGenCtr :: SparkM m (IORef Int)
getSparkGenCtr = s_sparkGen <$> ask

readSparkGenCtr :: SparkM m Int
readSparkGenCtr = getSparkGenCtr >>= readCtr

getSparkConvCtr :: SparkM m (IORef Int)
getSparkConvCtr = s_sparkConv <$> ask

readSparkConvCtr :: SparkM m Int
readSparkConvCtr = getSparkConvCtr >>= readCtr

readMaxSparkCtr :: SparkM m Int
readMaxSparkCtr = getPool >>= liftIO . maxLengthIO

getMaxHops :: SparkM m Int
getMaxHops = maxHops <$> s_conf <$> ask

getMaxFish :: SparkM m Int
getMaxFish = maxFish <$> s_conf <$> ask

getMinSched :: SparkM m Int
getMinSched = minSched <$> s_conf <$> ask

getMinFishDly :: SparkM m Int
getMinFishDly = minFishDly <$> s_conf <$> ask

getMaxFishDly :: SparkM m Int
getMaxFishDly = maxFishDly <$> s_conf <$> ask


-----------------------------------------------------------------------------
-- blocking and unblocking idle schedulers

-- Put executing scheduler to sleep.
blockSched :: SparkM m ()
blockSched = getIdleSchedsSem >>= liftIO . Sem.wait


-- Wake up 'n' sleeping schedulers.
wakeupSched :: Int -> SparkM m ()
wakeupSched n = getIdleSchedsSem >>= liftIO . replicateM_ n . Sem.signal


-----------------------------------------------------------------------------
-- local access to spark pool

-- Get a spark from the front of the spark pool, if there is any;
-- possibly send a FISH message and update stats (ie. count sparks converted);
-- the scheduler ID argument may be used for logging.
getSpark :: Int -> SparkM m (Maybe (Spark m))
getSpark schedID = do
  pool <- getPool
  maybe_spark <- liftIO $ popFrontIO pool
  sendFISH
  case maybe_spark of
    Just _  -> do getSparkConvCtr >>= incCtr
                  sparks <- liftIO $ lengthIO pool
                  debug dbgSpark $
                    "#sparks=" ++ show sparks ++ " (spark converted)"
                  return maybe_spark
    Nothing -> do return maybe_spark


-- Put a new spark at the back of the spark pool, wake up 1 sleeping scheduler,
-- and update stats (ie. count sparks generated locally);
-- the scheduler ID argument may be used for logging.
putSpark :: Int -> Spark m -> SparkM m ()
putSpark schedID spark = do
  pool <- getPool
  liftIO $ pushBackIO pool spark
  wakeupSched 1
  getSparkGenCtr >>= incCtr
  sparks <- liftIO $ lengthIO pool
  debug dbgSpark $
    "#sparks=" ++ show sparks ++ " (spark created)"


-----------------------------------------------------------------------------
-- HdpH messages (peer to peer)

-- 4 different types of messages dealing with fishing and pushing sparks;
-- the parameter 's' abstracts the type of sparks
data Msg m = FISH        -- looking for work
               !NodeId     -- fishing node
               !NodeId     -- primary target (eg. where last spark came from)
               !Int        -- #hops FISH message may yet travel
           | NOWORK      -- reply to FISH sender (when there is no work)
           | SCHEDULE    -- reply to FISH sender (when there is work)
               (Spark m)   -- spark
               !NodeId     -- sender
           | PUSH        -- eagerly pushing work
               (Spark m)   -- spark


-- Show instance (mainly for debugging)
instance Show (Msg m) where
  showsPrec _ (FISH fisher target hops) = showString "FISH(" . shows fisher .
                                          showString "," . shows target .
                                          showString "," . shows hops .
                                          showString ")"
  showsPrec _ (NOWORK)                  = showString "NOWORK"
  showsPrec _ (SCHEDULE _spark sender)  = showString "SCHEDULE(_," .
                                          shows sender . showString ")"
  showsPrec _ (PUSH _spark)             = showString "PUSH(_)"


instance NFData (Msg m) where
  rnf (FISH fisher target hops) = rnf fisher `seq` rnf target `seq` rnf hops
  rnf (NOWORK)                  = ()
  rnf (SCHEDULE spark sender)   = rnf spark `seq` rnf sender
  rnf (PUSH spark)              = rnf spark


instance Serialize (Msg m) where
  put (FISH fisher target hops) = Data.Serialize.put (0 :: Word8) >>
                                  Data.Serialize.put fisher >>
                                  Data.Serialize.put target >>
                                  Data.Serialize.put hops
  put (NOWORK)                  = Data.Serialize.put (1 :: Word8)
  put (SCHEDULE spark sender)   = Data.Serialize.put (2 :: Word8) >>
                                  Data.Serialize.put spark >>
                                  Data.Serialize.put sender
  put (PUSH spark)              = Data.Serialize.put (3 :: Word8) >>
                                  Data.Serialize.put spark

  get = do tag <- Data.Serialize.get
           case tag :: Word8 of
             0 -> do fisher <- Data.Serialize.get
                     target <- Data.Serialize.get
                     hops   <- Data.Serialize.get
                     return $ FISH fisher target hops
             1 -> do return $ NOWORK
             2 -> do spark  <- Data.Serialize.get
                     sender <- Data.Serialize.get
                     return $ SCHEDULE spark sender
             3 -> do spark  <- Data.Serialize.get
                     return $ PUSH spark


-----------------------------------------------------------------------------
-- fishing and the like

-- Send a FISH message, but only if there is no FISH outstanding and the
-- number of sparks in the pool is less or equal to the 'maxFish' parameter;
-- the target is the sender of the most recent SCHEDULE message, if a
-- SCHEDULE message has yet been received, otherwise the target is random.
sendFISH :: SparkM m ()
sendFISH = do
  pool <- getPool
  fishingFlag <- getFishingFlag
  isFishing <- readFlag fishingFlag
  unless isFishing $ do
    -- no FISH currently outstanding
    nodes <- liftCommM $ Comm.nodes
    maxFish <- getMaxFish
    sparks <- liftIO $ lengthIO pool
    when (nodes > 1 && sparks <= maxFish) $ do
      -- there are other nodes and the pool has too few sparks;
      -- set flag indicating that we are going to send a FISH
      ok <- setFlag fishingFlag
      when ok $ do
        -- flag was clear before: go ahead sending FISH;
        -- construct message
        fisher <- liftCommM $ Comm.myNode
        hops <- getMaxHops
        -- target is node where most recent spark came from (if such exists)
        maybe_target <- readSparkOrigHist
        target <- case maybe_target of
                    Just node -> return node
                    Nothing   -> do allNodes <- liftCommM $ Comm.allNodes
                                    let avoidNodes = Set.singleton fisher
                                    -- select random target (other than fisher)
                                    randomOtherElem avoidNodes allNodes nodes
        let msg = FISH fisher target hops :: Msg m
        -- send message
        debug dbgMsgSend $
          show msg ++ " ->> " ++ show target
        liftCommM $ Comm.send target $ encodeLazy msg
        -- update stats
        getFishSentCtr >>= incCtr


-- Dispatch FISH, SCHEDULE and NOWORK messages to their respective handlers.
dispatch :: Msg m -> SparkM m ()
dispatch msg@(FISH _ _ _)   = handleFISH msg
dispatch msg@(SCHEDULE _ _) = handleSCHEDULE msg
dispatch msg@(NOWORK)       = handleNOWORK msg
dispatch msg = error $ "HdpH.Internal.Sparkpool.dispatch: " ++
                       show msg ++ " unexpected"


-- Handle a FISH message; replies
-- * with SCHEDULE if pool has enough sparks, or else
-- * with NOWORK if FISH has travelled far enough, or else
-- * forwards FISH to a random node (other than fisher or target).
handleFISH :: forall m . Msg m -> SparkM m ()
handleFISH msg@(FISH fisher target hops) = do
  here <- liftCommM $ Comm.myNode
  sparks <- readPoolSize
  minSched <- getMinSched
  -- send SCHEDULE if pool has enough sparks
  done <- if sparks < minSched
             then do return False
             else do
               pool <- getPool
               maybe_spark <- liftIO $ popBackIO pool
               case maybe_spark of
                 Just spark -> do let msg = SCHEDULE spark here :: Msg m
                                  debug dbgMsgSend $
                                    show msg ++ " ->> " ++ show fisher
                                  liftCommM $ Comm.send fisher $ encodeLazy msg
                                  return True
                 Nothing    -> do return False
  unless done $ do
    -- no SCHEDULE sent; check whether to forward FISH
    nodes <- liftCommM $ Comm.nodes
    let avoidNodes = Set.fromList [fisher, target, here]
    if hops > 0 && nodes > Set.size avoidNodes
       then do  -- fwd FISH to random node (other than those in avoidNodes)
         allNodes <- liftCommM $ Comm.allNodes
         node <- randomOtherElem avoidNodes allNodes nodes
         let msg = FISH fisher target (hops - 1) :: Msg m
         debug dbgMsgSend $
           show msg ++ " ->> " ++ show node
         liftCommM $ Comm.send node $ encodeLazy msg
       else do  -- notify fisher that there is no work
         let msg = NOWORK :: Msg m
         debug dbgMsgSend $
           show msg ++ " ->> " ++ show fisher
         liftCommM $ Comm.send fisher $ encodeLazy msg


-- Handle a SCHEDULE message;
-- * puts the spark at the front of the spark pool,
-- * records spark sender and updates stats, and
-- * clears the "FISH outstanding" flag.
handleSCHEDULE :: Msg m -> SparkM m ()
handleSCHEDULE msg@(SCHEDULE spark sender) = do
  -- put spark into pool
  pool <- getPool
  liftIO $ pushFrontIO pool spark
  -- record sender of spark
  updateSparkOrigHist sender
  -- update stats
  getSparkRcvdCtr >>= incCtr
  -- clear FISHING flag
  getFishingFlag >>= clearFlag
  return ()


-- Handle a NOWORK message; 
-- asynchronously, after a random delay, clear the "FISH outstanding" flag 
-- and wake one scheduler (if some are sleeping) to resume fishing.
-- Rationale for random delay: to prevent FISH flooding when there is
--   (almost) no work.
handleNOWORK :: Msg m -> SparkM m ()
handleNOWORK msg@(NOWORK) = do
  fishingFlag   <- getFishingFlag
  noWorkServer  <- getNoWorkServer
  idleSchedsSem <- getIdleSchedsSem
  minDelay      <- getMinFishDly
  maxDelay      <- getMaxFishDly
  -- compose delay and clear flag action
  let action = do -- random delay
                  delay <- randomRIO (minDelay, max minDelay maxDelay)
                  threadDelay delay
                  -- clear fishing flag
                  atomicModifyIORef fishingFlag $ const (False, ())
                  -- wakeup 1 sleeping scheduler (to fish again)
                  Sem.signal idleSchedsSem
  -- post action request to server
  liftIO $ reqAction noWorkServer action


-----------------------------------------------------------------------------
-- auxiliary stuff

readFlag :: IORef Bool -> SparkM m Bool
readFlag = liftIO . readIORef

-- Sets given 'flag'; returns True iff 'flag' did actually change.
setFlag :: IORef Bool -> SparkM m Bool
setFlag flag = liftIO $ atomicModifyIORef flag $ \ v -> (True, not v)

-- Clears given 'flag'; returns True iff 'flag' did actually change.
clearFlag :: IORef Bool -> SparkM m Bool
clearFlag flag = liftIO $ atomicModifyIORef flag $ \ v -> (False, v)


readCtr :: IORef Int -> SparkM m Int
readCtr = liftIO . readIORef

incCtr :: IORef Int -> SparkM m ()
incCtr ctr = liftIO $ atomicModifyIORef ctr $ \ v ->
                        let v' = v + 1 in v' `seq` (v', ())


-- 'randomOtherElem avoid xs n' returns a random element of the list 'xs'
-- different from any of the elements in the set 'avoid'.
-- Requirements: 'n <= length xs' and 'xs' contains no duplicates.
randomOtherElem :: (Ord a) => Set a -> [a] -> Int -> SparkM m a
randomOtherElem avoid xs n = do
  let candidates = filter (`Set.notMember` avoid) xs
  -- length candidates == length xs - Set.size avoid >= n - Set.size avoid
  i <- liftIO $ randomRIO (0, n - Set.size avoid - 1)
  -- 0 <= i <= n - Set.size avoid - 1 < length candidates
  return (candidates !! i)


-- debugging
debug :: Int -> String -> SparkM m ()
debug level message = liftIO $ Location.debug level message
