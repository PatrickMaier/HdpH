-- Spark pool and fishing
--
-- Author: Patrick Maier
-----------------------------------------------------------------------------

{-# LANGUAGE ScopedTypeVariables #-}  -- req'd for type annotations
{-# LANGUAGE BangPatterns #-}

module Control.Parallel.HdpH.Internal.Sparkpool
  ( -- * spark pool monad
    SparkM,      -- synonym: SparkM m = ReaderT <State m> IO
    run,         -- :: RTSConf -> ActionServer -> Sem -> SparkM m a -> IO a
    liftIO,      -- :: IO a -> SparkM m a

    -- * blocking and unblocking idle schedulers
    blockSched,      -- :: SparkM m ()
    wakeupSched,     -- :: Int -> SparkM m ()

    -- * local (ie. scheduler) access to spark pool
    getLocalSpark,   -- :: Int -> SparkM m (Maybe (Spark m))
    putLocalSpark,   -- :: Int -> Dist -> Spark m -> SparkM m ()

    -- * messages
    Msg(..),         -- instances: Show, NFData, Serialize

    -- * handle messages related to fishing
    dispatch,        -- :: Msg m -> SparkM m ()
    handleFISH,      -- :: Msg m -> SparkM m ()
    handleSCHEDULE,  -- :: Msg m -> SparkM m ()
    handleNOWORK,    -- :: Msg m -> SparkM m ()

    -- * access to stats data
    readPoolSize,      -- :: Dist -> SparkM m Int
    readFishSentCtr,   -- :: SparkM m Int
    readSparkRcvdCtr,  -- :: SparkM m Int
    readMaxSparkCtr,   -- :: Dist -> SparkM m Int
    readMaxSparkCtrs,  -- :: SparkM m [Int]
    readSparkGenCtr,   -- :: SparkM m Int
    readSparkConvCtr   -- :: SparkM m Int
  ) where

import Prelude hiding (error)
import Control.Concurrent (threadDelay)
import Control.DeepSeq (NFData, rnf)
import Control.Monad (when, replicateM_, void)
import Control.Monad.Reader (ReaderT, runReaderT, ask)
import Control.Monad.Trans (lift)
import qualified Data.ByteString.Lazy as BS
import Data.Functor ((<$>))
import Data.IORef (IORef, newIORef, readIORef, writeIORef, atomicModifyIORef)
import Data.List (insertBy, sortBy)
import Data.Ord (comparing)
import Data.Serialize (Serialize)
import qualified Data.Serialize (put, get)
import Data.Word (Word8)
import System.Random (randomRIO)
import System.Clock

import Control.Parallel.HdpH.Conf
       (RTSConf( maxHops
               , maxFish
               , minSched
               , minFishDly
               , maxFishDly
               , useLastStealOptimisation
               , useLowWatermarkOptimisation))

import Control.Parallel.HdpH.Dist (Dist, zero, one, mul2, div2)
import qualified Control.Parallel.HdpH.Internal.Comm as Comm
       (send, nodes, myNode, equiDistBases)
import Control.Parallel.HdpH.Internal.Data.Deque
       (DequeIO, emptyIO, pushBackIO, popFrontIO, popBackIO,
        lengthIO, maxLengthIO)
import Control.Parallel.HdpH.Internal.Data.DistMap (DistMap)
import qualified Control.Parallel.HdpH.Internal.Data.DistMap as DistMap
       (new, toDescList, lookup, keys, minDist)
import Control.Parallel.HdpH.Internal.Data.Sem (Sem)
import qualified Control.Parallel.HdpH.Internal.Data.Sem as Sem (wait, signal)
import Control.Parallel.HdpH.Internal.Location
       (Node, dbgMsgSend, dbgSpark, dbgWSScheduler, error)
import qualified Control.Parallel.HdpH.Internal.Location as Location (debug)
import Control.Parallel.HdpH.Internal.Topology (dist)
import Control.Parallel.HdpH.Internal.Misc
       (encodeLazy, ActionServer, reqAction, timeDiffMSecs)
import Control.Parallel.HdpH.Internal.Type.Par (Spark)



-----------------------------------------------------------------------------
-- SparkM monad

-- 'SparkM m' is a reader monad sitting on top of the 'IO' monad;
-- the parameter 'm' abstracts a monad (cf. module HdpH.Internal.Type.Par).
type SparkM m = ReaderT (State m) IO


-- spark pool state (mutable bits held in IORefs and the like)
data State m =
  State {
    s_conf             :: RTSConf,            -- config data
    s_pools            :: DistMap (DequeIO (Spark m)), -- actual spark pools
    s_sparkOrig        :: IORef (Maybe Node),-- primary FISH target (recent src)
    s_fishing          :: IORef Bool,        -- True iff FISH outstanding
    s_noWork           :: ActionServer,      -- for clearing "FISH outstndg" flag
    s_idleScheds       :: Sem,               -- semaphore for idle schedulers
    s_fishSent         :: IORef Int,         -- #FISH sent
    s_sparkRcvd        :: IORef Int,         -- #sparks received
    s_sparkGen         :: IORef Int,         -- #sparks generated
    s_sparkConv        :: IORef Int,         -- #sparks converted
    s_lastFISHSendTime :: IORef TimeSpec     -- #Time the last FISH left
    }


-- Eliminates the 'SparkM' layer by executing the given 'SparkM' action on
-- an empty spark pool; expects a config data, an action server (for
-- clearing "FISH outstanding" flag) and a semaphore (for idle schedulers).
run :: RTSConf -> ActionServer -> Sem -> SparkM m a -> IO a
run conf noWorkServer idleSem action = do
  -- set up spark pool state (with as many pools as there are equidist bases)
  rs           <- getDistsIO
  pools        <- DistMap.new <$> sequence [emptyIO | _ <- rs]
  sparkOrig    <- newIORef Nothing
  fishing      <- newIORef False
  fishSent     <- newIORef 0
  sparkRcvd    <- newIORef 0
  sparkGen     <- newIORef 0
  sparkConv    <- newIORef 0
  lastFISHTime <- newIORef (TimeSpec 0 0)
  let s0 = State { s_conf             = conf,
                   s_pools            = pools,
                   s_sparkOrig        = sparkOrig,
                   s_fishing          = fishing,
                   s_noWork           = noWorkServer,
                   s_idleScheds       = idleSem,
                   s_fishSent         = fishSent,
                   s_sparkRcvd        = sparkRcvd,
                   s_sparkGen         = sparkGen,
                   s_sparkConv        = sparkConv,
                   s_lastFISHSendTime = lastFISHTime
                   }
  -- run monad
  runReaderT action s0


-- Lifting lower layers.
liftIO :: IO a -> SparkM m a
liftIO = lift


-----------------------------------------------------------------------------
-- access to state

getPool :: Dist -> SparkM m (DequeIO (Spark m))
getPool r = DistMap.lookup r . s_pools <$> ask

readPoolSize :: Dist -> SparkM m Int
readPoolSize r = getPool r >>= liftIO . lengthIO

getSparkOrigHist :: SparkM m (IORef (Maybe Node))
getSparkOrigHist = s_sparkOrig <$> ask

readSparkOrigHist :: SparkM m (Maybe Node)
readSparkOrigHist = do
  useLastSteal <- useLastStealOptimisation <$> s_conf <$> ask
  if useLastSteal
   then getSparkOrigHist >>= liftIO . readIORef
   else return Nothing


setSparkOrigHist :: Node -> SparkM m ()
setSparkOrigHist mostRecentOrigin = do
  sparkOrigHistRef <- getSparkOrigHist
  liftIO $ writeIORef sparkOrigHistRef (Just mostRecentOrigin)

clearSparkOrigHist :: SparkM m ()
clearSparkOrigHist = do
  sparkOrigHistRef <- getSparkOrigHist
  liftIO $ writeIORef sparkOrigHistRef Nothing

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

readMaxSparkCtr :: Dist -> SparkM m Int
readMaxSparkCtr r = getPool r >>= liftIO . maxLengthIO

readMaxSparkCtrs :: SparkM m [Int]
readMaxSparkCtrs = liftIO getDistsIO >>= mapM readMaxSparkCtr

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

setFISHTimer :: TimeSpec -> SparkM m ()
setFISHTimer s = do
  t <- getLastFISHSendTime
  liftIO $ atomicModifyIORef t (const (s, ()))

resetFISHTimer :: SparkM m ()
resetFISHTimer = do
  let noTime = TimeSpec 0 0
  t <- getLastFISHSendTime
  liftIO $ atomicModifyIORef t (const (noTime, ()))


getLastFISHSendTime :: SparkM m (IORef TimeSpec)
getLastFISHSendTime = s_lastFISHSendTime <$> ask

-----------------------------------------------------------------------------
-- access to Comm module state

singleNode :: SparkM m Bool
singleNode = (< 2) <$> liftIO Comm.nodes

getEquiDistBasesIO :: IO (DistMap [(Node, Int)])
getEquiDistBasesIO = Comm.equiDistBases

getDistsIO :: IO [Dist]
getDistsIO = DistMap.keys <$> Comm.equiDistBases

getMinDistIO :: IO Dist
getMinDistIO = DistMap.minDist <$> Comm.equiDistBases


-----------------------------------------------------------------------------
-- blocking and unblocking idle schedulers

-- Put executing scheduler to sleep.
blockSched :: SparkM m ()
blockSched = getIdleSchedsSem >>= liftIO . Sem.wait


-- Wake up 'n' sleeping schedulers.
wakeupSched :: Int -> SparkM m ()
wakeupSched n = getIdleSchedsSem >>= liftIO . replicateM_ n . Sem.signal


-----------------------------------------------------------------------------
-- spark selection policies

-- Local spark selection policy (pick sparks from back of queue),
-- starting from radius 'r' and and rippling outwords;
-- the scheduler ID may be used for logging.
selectLocalSpark :: Int -> Dist -> SparkM m (Maybe (Spark m, Dist))
selectLocalSpark schedID !r = do
  pool <- getPool r
  maybe_spark <- liftIO $ popBackIO pool
  case maybe_spark of
    Just spark          -> return $ Just (spark, r)          -- return spark
    Nothing | r == one  -> return Nothing                    -- pools empty
            | otherwise -> selectLocalSpark schedID (mul2 r) -- ripple out


-- Remote spark selection policy (pick sparks from front of queue),
-- starting from radius 'r0' and rippling outwords, but only if the pools
-- hold at least 'minSched' sparks in total;
-- schedID (expected to be msg handler ID 0) may be used for logging.
-- TODO: Track total number of sparks in pools more effectively.
selectRemoteSpark :: Int -> Dist -> SparkM m (Maybe (Spark m, Dist))
selectRemoteSpark _schedID r0 = do
  may <- maySCHEDULE
  if may
    then pickRemoteSpark r0
    else return Nothing
      where
        pickRemoteSpark :: Dist -> SparkM m (Maybe (Spark m, Dist))
        pickRemoteSpark !r = do
          pool <- getPool r
          maybe_spark <- liftIO $ popFrontIO pool
          case maybe_spark of
            Just spark          -> return $ Just (spark, r)  -- return spark
            Nothing | r == one  -> return Nothing            -- pools empty
                    | otherwise -> pickRemoteSpark (mul2 r)  -- ripple out


-- Returns True iff total number of sparks in pools is at least 'minSched'.
maySCHEDULE :: SparkM m Bool
maySCHEDULE = do
  min_sched <- getMinSched
  r_min <- liftIO getMinDistIO
  checkPools r_min min_sched one
    where
      checkPools :: Dist -> Int -> Dist -> SparkM m Bool
      checkPools r_min !min_sparks !r = do
        pool <- getPool r
        sparks <- liftIO $ lengthIO pool
        let min_sparks' = min_sparks - sparks
        if min_sparks' <= 0
          then return True
          else if r == r_min
            then return False
            else checkPools r_min min_sparks' (div2 r)


-----------------------------------------------------------------------------
-- access to spark pool

-- Get a spark from the back of a spark pool with minimal radius, if any exists;
-- possibly send a FISH message and update stats (ie. count sparks converted);
-- the scheduler ID argument may be used for logging.
getLocalSpark :: Int -> SparkM m (Maybe (Spark m))
getLocalSpark schedID = do
  -- select local spark, starting with smallest radius
  r_min <- liftIO getMinDistIO
  maybe_spark <- selectLocalSpark schedID r_min
  case maybe_spark of
    Nothing         -> do
      sendFISH zero
      return Nothing
    Just (spark, r) -> do
      useLowWatermark <- useLowWatermarkOptimisation <$> s_conf <$> ask
      when useLowWatermark $ sendFISH r

      getSparkConvCtr >>= incCtr
      debug dbgSpark $
        "(spark converted)"

      return $ Just spark


-- Put a new spark at the back of the spark pool at radius 'r', wake up
-- 1 sleeping scheduler, and update stats (ie. count sparks generated locally);
-- the scheduler ID argument may be used for logging.
putLocalSpark :: Int -> Dist -> Spark m -> SparkM m ()
putLocalSpark _schedID r spark = do
  pool <- getPool r
  liftIO $ pushBackIO pool spark
  wakeupSched 1
  getSparkGenCtr >>= incCtr
  debug dbgSpark $
    "(spark created)"


-- Put received spark at the back of the spark pool at radius 'r', wake up
-- 1 sleeping scheduler, and update stats (ie. count sparks received);
-- schedID (expected to be msg handler ID 0) may be used for logging.
putRemoteSpark :: Int -> Dist -> Spark m -> SparkM m ()
putRemoteSpark _schedID r spark = do
  pool <- getPool r
  liftIO $ pushBackIO pool spark
  wakeupSched 1
  getSparkRcvdCtr >>= incCtr


-----------------------------------------------------------------------------
-- HdpH messages (peer to peer)

-- 5 different types of messages dealing with fishing and pushing sparks
data Msg m = TERM        -- termination message (broadcast from root and back)
               !Node       -- root node
           | FISH        -- thief looking for work
               !Node       -- thief
               [Node]      -- targets already visited
               [Node]      -- remaining candidate targets
               [Node]      -- remaining primary source targets
               !Bool       -- True iff FISH may be forwarded to primary source
               !Int        -- Forward Count
           | NOWORK      -- reply to thief's FISH (when there is no work)
               !Int        -- forward count of FISH responded to.
           | SCHEDULE    -- reply to thief's FISH (when there is work)
               (Spark m)   -- spark
               !Dist       -- spark's radius
               !Node       -- victim
               !Int        -- forward count of FISH responded to.
           | PUSH        -- eagerly pushing work
               (Spark m)   -- spark

-- Invariants for 'FISH thief avoid candidates sources fwd :: Msg m':
-- * Lists 'candidates' and 'sources' are sorted in order of ascending
--   distance from 'thief'.
-- * Lists 'avoid', 'candidates' and 'sources' are sets (ie. no duplicates).
-- * Sets 'avoid', 'candidates' and 'sources' are pairwise disjoint.
-- * At the thief, a FISH message is launched with empty sets 'avoid' and
--   'sources', and at most 'maxHops' 'candidates'. As the message is being
--   forwarded, the cardinality of the sets 'avoid', 'candidates' and 'sources'
--   combined will never exceeed 2 * 'maxHops' + 1. This should be kept in mind 
--   when choosing 'maxHops', to ensure that FISH messages stay small (ie. fit
--   into one packet).

-- Show instance (mainly for debugging)
instance Show (Msg m) where
  showsPrec _ (TERM root)                = showString "TERM(" . shows root .
                                           showString ")"
  showsPrec _ (FISH thief avoid candidates sources fwd fwdcnt)
                                         = showString "FISH(" . shows thief .
                                           showString "," . shows avoid .
                                           showString "," . shows candidates .
                                           showString "," . shows sources .
                                           showString "," . shows fwd .
                                           showString "," . shows fwdcnt .
                                           showString ")"
  showsPrec _ (NOWORK fwdc)              = showString "NOWORK(" . shows fwdc .
                                           showString ")"
  showsPrec _ (SCHEDULE _spark r victim fwdcnt)
                                         = showString "SCHEDULE(_," . shows r .
                                           showString "," . shows victim .
                                           showString "," . shows fwdcnt .
                                           showString ")"
  showsPrec _ (PUSH _spark)              = showString "PUSH(_)"


instance NFData (Msg m) where
  rnf (TERM _root)                                = ()
  rnf (FISH _thief avoid candidates sources _fwd fwdcnt) =
                                                    rnf avoid `seq`
                                                    rnf candidates `seq`
                                                    rnf sources
  rnf (NOWORK fwdcnt)                             = ()
  rnf (SCHEDULE spark _r _victimm fwdcnt)         = rnf spark
  rnf (PUSH spark)                                = rnf spark


-- TODO: Derive this instance.
instance Serialize (Msg m) where
  put (TERM root)               = Data.Serialize.put (0 :: Word8) >>
                                  Data.Serialize.put root
  put (FISH thief avoid candidates sources fwd fwdcnt)
                                = Data.Serialize.put (1 :: Word8) >>
                                  Data.Serialize.put thief >>
                                  Data.Serialize.put avoid >>
                                  Data.Serialize.put candidates >>
                                  Data.Serialize.put sources >>
                                  Data.Serialize.put fwd >>
                                  Data.Serialize.put fwdcnt
  put (NOWORK fwdcnt)           = Data.Serialize.put (2 :: Word8) >>
                                  Data.Serialize.put fwdcnt
  put (SCHEDULE spark r victim fwdcnt)
                                = Data.Serialize.put (3 :: Word8) >>
                                  Data.Serialize.put spark >>
                                  Data.Serialize.put r >>
                                  Data.Serialize.put victim >>
                                  Data.Serialize.put fwdcnt
  put (PUSH spark)              = Data.Serialize.put (4 :: Word8) >>
                                  Data.Serialize.put spark

  get = do tag <- Data.Serialize.get
           case tag :: Word8 of
             0 -> do root <- Data.Serialize.get
                     return $ TERM root
             1 -> do thief      <- Data.Serialize.get
                     avoid      <- Data.Serialize.get
                     candidates <- Data.Serialize.get
                     sources    <- Data.Serialize.get
                     fwd        <- Data.Serialize.get
                     fwdcnt     <- Data.Serialize.get
                     return $ FISH thief avoid candidates sources fwd fwdcnt
             2 -> do fwdcnt <- Data.Serialize.get
                     return $ NOWORK fwdcnt
             3 -> do spark  <- Data.Serialize.get
                     r      <- Data.Serialize.get
                     victim <- Data.Serialize.get
                     fwdcnt <- Data.Serialize.get
                     return $ SCHEDULE spark r victim fwdcnt
             4 -> do spark  <- Data.Serialize.get
                     return $ PUSH spark
             _ -> error "panic in instance Serialize (Msg m): tag out of range"


-----------------------------------------------------------------------------
-- fishing and the like

-- Returns True iff FISH message should be sent;
-- assumes spark pools at radius < r_min are empty, and
-- all pools are empty if r_min == zero.
goFISHing :: Dist -> SparkM m Bool
goFISHing r_min = do
  fishingFlag <- getFishingFlag
  isFishing   <- readFlag fishingFlag
  isSingle    <- singleNode
  max_fish    <- getMaxFish
  if isFishing || isSingle || max_fish < 0
    then do
      -- don't fish if * a FISH is outstanding, or
      --               * there is only one node, or
      --               * fishing is switched off
      return False
    else do
      -- check total number of sparks is above low watermark
      if r_min == zero
        then return True
        else checkPools max_fish r_min
          where
            checkPools :: Int -> Dist -> SparkM m Bool
            checkPools min_sparks r = do
              pool <- getPool r
              sparks <- liftIO $ lengthIO pool
              let min_sparks' = min_sparks - sparks
              if min_sparks' < 0
                then return True
                else if r == one
                  then return $ min_sparks' == 0
                  else checkPools min_sparks' (mul2 r)


-- Send a FISH message, but only if there is no FISH outstanding and the total
-- number of sparks in the pools is less or equal to the 'maxFish' parameter;
-- assumes pools at radius < r_min are empty, and all pools are empty if
-- r_min == zero; the FISH victim is one of minimal distance, selected
-- according to the 'selectFirstVictim' policy.
sendFISH :: Dist -> SparkM m ()
sendFISH r_min = do
  -- check whether a FISH message should be sent
  go <- goFISHing r_min
  when go $ do
    -- set flag indicating that FISH is going to be sent
    fishingFlag <- getFishingFlag
    ok <- setFlag fishingFlag
    when ok $ do
      -- flag was clear before: go ahead sending FISH
      -- select victim
      thief <- liftIO $ Comm.myNode
      max_hops <- getMaxHops
      candidates <- liftIO $ randomCandidates max_hops

      maybe_src <- readSparkOrigHist

      -- compose FISH message
      let (target, msg) =
            case maybe_src of
              Just src -> (src, FISH thief [] candidates [] False 0)
              Nothing  ->
                case candidates of
                  cand:cands -> (cand, FISH thief [] cands [] True 0)
                  []         -> (thief, NOWORK 0)  -- no candidates --> NOWORK
      -- send FISH (or NOWORK) message
      debug dbgMsgSend $ let msg_size = BS.length (encodeLazy msg) in
        show msg ++ " ->> " ++ show target ++ " Length: " ++ show msg_size

      s <- liftIO $ getTime Monotonic
      setFISHTimer s

      liftIO $ Comm.send target $ encodeLazy msg
      case msg of
        FISH _ _ _ _ _ _ -> getFishSentCtr >>= incCtr  -- update stats
        _                -> return ()

-- Return up to 'n' random candidates drawn from the equidistant bases,
-- excluding the current node and sorted in order of ascending distance.
randomCandidates :: Int -> IO [Node]
randomCandidates n = do
  bases <- reverse . DistMap.toDescList <$> getEquiDistBasesIO
  let universes = [[(r, p) | (p, _) <- tail_basis] | (r, _:tail_basis) <- bases]
  map snd . sortBy (comparing fst) <$> uniqRandomsRR n universes


-- Dispatch FISH, SCHEDULE and NOWORK messages to their respective handlers.
dispatch :: Msg m -> SparkM m ()
dispatch msg@(FISH _ _ _ _ _ _)   = handleFISH msg
dispatch msg@(SCHEDULE _ _ _ _)   = handleSCHEDULE msg
dispatch msg@(NOWORK _)           = handleNOWORK msg
dispatch msg = error $ "HdpH.Internal.Sparkpool.dispatch: " ++
                       show msg ++ " unexpected"


-- Handle a FISH message; replies
-- * with SCHEDULE if pool has enough sparks, or else
-- * with NOWORK if FISH has travelled far enough, or else
-- * forwards FISH to a candidate target or a primary source of work.
handleFISH :: Msg m -> SparkM m ()
handleFISH msg@(FISH thief _avoid _candidates _sources _fwd fwdcnt) = do
  me <- liftIO Comm.myNode
  maybe_spark <- selectRemoteSpark 0 (dist thief me)
  case maybe_spark of
    Just (spark, r) -> do -- compose and send SCHEDULE
      let scheduleMsg = SCHEDULE spark r me fwdcnt
      debug dbgMsgSend $ let msg_size = BS.length (encodeLazy scheduleMsg) in
        show scheduleMsg ++ " ->> " ++ show thief ++ " Length: " ++ show msg_size
      liftIO $ Comm.send thief $ encodeLazy scheduleMsg
    Nothing -> do
      maybe_src <- readSparkOrigHist
      -- compose FISH message to forward
      let (target, forwardMsg) = forwardFISH me maybe_src msg
      -- send message
      debug dbgMsgSend $ let msg_size = BS.length (encodeLazy forwardMsg) in
        show forwardMsg ++ " ->> " ++ show target ++ " Length: " ++ show msg_size
      liftIO $ Comm.send target $ encodeLazy forwardMsg
handleFISH _ = error "panic in handleFISH: not a FISH message"

-- Auxiliary function, called by 'handleFISH' when there is nought to schedule.
-- Constructs a forward and selects a target, or constructs a NOWORK reply.
forwardFISH :: Node -> Maybe Node -> Msg m -> (Node, Msg m)
forwardFISH me _          (FISH thief avoid candidates sources False fc) =
  dispatchFISH (FISH thief (me:avoid) candidates sources False fc)
forwardFISH me Nothing    (FISH thief avoid candidates sources _ fc)     =
  dispatchFISH (FISH thief (me:avoid) candidates sources False fc)
forwardFISH me (Just src) (FISH thief avoid candidates sources True fc)  =
  spineList sources' `seq`
  dispatchFISH (FISH thief (me:avoid) candidates sources' False fc)
    where
      sources' = if src `elem` thief:(sources ++ avoid ++ candidates)
                   then sources  -- src is already known
                   else insertBy (comparing $ dist thief) src sources
forwardFISH _ _ _ = error "panic in forwardFISH: not a FISH message"

-- Auxiliary function, called by 'forwardFISH'.
-- Extracts target and message from preliminary FISH message.
dispatchFISH :: Msg m -> (Node, Msg m)
dispatchFISH (FISH thief avoid' candidates sources' _ fwdcnt) =
  let fc = fwdcnt + 1 in
  case (candidates, sources') of
    ([],         [])       -> (thief, NOWORK fwdcnt)
    (cand:cands, [])       -> (cand,  FISH thief avoid' cands []   True  fc)
    ([],         src:srcs) -> (src,   FISH thief avoid' []    srcs False fc)
    (cand:cands, src:srcs) ->
      if dist thief cand < dist thief src
        then -- cand is closest to thief
             (cand, FISH thief avoid' cands      sources' True fc)
        else -- otherwise prefer src
             (src,  FISH thief avoid' candidates srcs     False fc)
dispatchFISH _ = error "panic in dispatchFISH: not a FISH message"


-- Handle a SCHEDULE message;
-- * puts received spark at the back of the appropriate spark pool,
-- * wakes up 1 sleeping scheduler,
-- * records spark sender and updates stats, and
-- * clears the "FISH outstanding" flag.
handleSCHEDULE :: Msg m -> SparkM m ()
handleSCHEDULE (SCHEDULE spark r victim fwdcnt) = do
  debugWSSchedulerLatency "SCHEDULE" (Just victim) fwdcnt

  putRemoteSpark 0 r spark

  setSparkOrigHist victim
  debug dbgWSScheduler $
    "Spark received from radius: " ++ show r

  void $ getFishingFlag >>= clearFlag

handleSCHEDULE _ = error "panic in handleSCHEDULE: not a SCHEDULE message"


-- Handle a NOWORK message;
-- clear primary fishing target, then asynchronously, after a random delay,
-- clear the "FISH outstanding" flag and wake one scheduler (if some are
-- sleeping) to resume fishing.
-- Rationale for random delay: to prevent FISH flooding when there is
--   (almost) no work.
handleNOWORK :: Msg m -> SparkM m ()
handleNOWORK (NOWORK fwdcnt) = do
  debugWSSchedulerLatency "NOWORK" Nothing fwdcnt

  clearSparkOrigHist
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
handleNOWORK _ = error "panic in handleNOWORK: not a NOWORK message"


-----------------------------------------------------------------------------
-- auxiliary stuff

-- walks along a list, forcing its spine
spineList :: [a] -> ()
spineList []     = ()
spineList (_:xs) = spineList xs


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


-- Returns up to 'n' unique random elements from the given list of 'universes',
-- which are cycled through in a round robin fashion.
uniqRandomsRR :: Int -> [[a]] -> IO [a]
uniqRandomsRR n universes =
  if n <= 0
    then return []
    else case universes of
      []         -> return []
      []:univs   -> uniqRandomsRR n univs
      univ:univs -> do
        i <- randomRIO (0, length univ - 1)
        let (prefix, x:suffix) = splitAt i univ
        xs <- uniqRandomsRR (n - 1) (univs ++ [prefix ++ suffix])
        return (x:xs)


-- debugging
debug :: Int -> String -> SparkM m ()
debug level message = liftIO $ Location.debug level message

debugWSSchedulerLatency :: String -> Maybe Node -> Int -> SparkM m ()
{-# INLINE debugWSSchedulerLatency #-}
debugWSSchedulerLatency msgType node fcnt = do
  e <- liftIO $ getTime Monotonic
  s <- getLastFISHSendTime >>= liftIO . readIORef
  case node of
    Nothing -> debug dbgWSScheduler $
      "Time between FISH and " ++ msgType ++ " = "
      ++ show (timeDiffMSecs s e) ++ " ms. " ++ show fcnt++ " forwards."
    Just n-> debug dbgWSScheduler $
      "Time between FISH and " ++ msgType ++ " from " ++ show n ++ " = "
      ++ show (timeDiffMSecs s e) ++ " ms. " ++ show fcnt ++ " forwards."
  resetFISHTimer
