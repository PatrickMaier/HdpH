-- Message passing via MPI
-- Variant: Messages represented as lazy byte strings
--
-- Visibility: external
-- Author: Patrick Maier <P.Maier@hw.ac.uk>
-- Created: 29 May 2011
--
-----------------------------------------------------------------------------

{-# LANGUAGE ForeignFunctionInterface #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE CPP #-}

module MP.MPI_ByteString
  ( -- * MPI ranks
    Rank,       -- abstract; instances: Eq, Ord, Ix, NFData, Serialize
    rank0,      -- :: Rank
    myRank,     -- :: IO Rank
    allRanks,   -- :: IO [Rank]
    procs,      -- :: IO Int

    -- * Messages and message handles
    Msg,        -- synonym: Data.ByteString.Lazy.ByteString
    MsgHandle,  -- abstract; no instances
    sender,     -- :: MsgHandle -> Rank
    getMsg,     -- :: MsgHandle -> IO Msg

    -- * packing messages; replacing functions from Data.ByteString.Lazy
    pack,       -- :: [Word8] -> IO Msg
    unpack,     -- :: Msg -> IO [Word8]

    -- * sending and receiving
    send,       -- :: Rank -> Msg -> IO ()
    recv,       -- :: IO MsgHandle

    -- * MPI startup and shutdown wrappers
    defaultBufferSize,  -- :: Int
    defaultWithMPI,     -- :: IO () -> IO ()
    withMPI,            -- :: Int -> IO () -> IO ()
    uncleanWithMPI,     -- :: Int -> IO () -> IO ()

    -- * inspecting MPI state
    initialized,        -- :: IO Bool
    finalized           -- :: IO Bool
  ) where

import Prelude hiding (init)
import Control.Applicative ((<$>))
import Control.Concurrent (forkIO, threadDelay)
import Control.Concurrent.Chan (newChan, readChan, writeChan, getChanContents)
import Control.Concurrent.MVar (MVar, newEmptyMVar, newMVar,
                                takeMVar, putMVar, readMVar, modifyMVar_)
import Control.DeepSeq (NFData(rnf))
import Control.Exception (evaluate, onException)
import Control.Monad (when, unless, replicateM)
import Data.Array (array, elems, (!))
import Data.Array.IO (IOArray, newArray, readArray, writeArray)
import qualified Data.ByteString
       as Strict (ByteString, length, empty, concat, pack, unpack, foldl')
import qualified Data.ByteString.Unsafe
       as Strict (unsafeUseAsCStringLen)
import qualified Data.ByteString.Internal
       as Strict (create)
import qualified Data.ByteString.Lazy
       as Lazy (ByteString, null, splitAt, fromChunks, toChunks, unpack)
import Data.ByteString.Lazy.Internal  -- reqd to get our hands on overheads
       as Lazy (chunkOverhead)
import Data.IORef (readIORef, modifyIORef)
import Data.List (foldl')
import Data.Word (Word8)
import Foreign.C.Types (CInt)
import Foreign.Ptr (Ptr, nullPtr, plusPtr, castPtr)
import Foreign.Marshal.Alloc (mallocBytes, free)
import Foreign.Marshal.Array (peekArray, pokeArray)
import Foreign.Marshal.Utils (copyBytes)
import System.IO (stderr, hPutStrLn)

import MP.MPI.State_ByteString
         (Rank(Rank_),
          Msg, MsgHandle(MsgHandle_), Chunk, SendQ, RecvQ,
          SendBuf(SendBuf), RecvBuf(RecvBuf),
          State(State), stateRef, Flag(PreMPI, InMPI, PostMPI), flagRef)
import qualified MP.MPI.State_ByteString as MPIState (State(..))

-- for declaring instances
import Data.Ix (Ix(..))
import Control.DeepSeq (NFData(..))
import Data.Serialize (Serialize(..))


-----------------------------------------------------------------------------
-- MPI ranks; instances and support functions

deriving instance Eq Rank
deriving instance Ord Rank
deriving instance Ix Rank

instance Enum Rank where
  toEnum = Rank_
  fromEnum (Rank_ i) = i

instance Show Rank where  -- for debugging only
  showsPrec _ (Rank_ i) = showString "rank" . shows i

instance NFData Rank where
  rnf (Rank_ i) = rnf i

instance Serialize Rank where
  put (Rank_ i) = put i
  get = Rank_ <$> get

-- The smallest possible (and usually the master) rank
rank0 :: Rank
rank0 = toEnum 0

myRank :: IO Rank
myRank = MPIState.myRank <$> readIORef stateRef

allRanks :: IO [Rank]
allRanks = do
  processes <- procs
  return $ take processes $ enumFrom rank0

procs :: IO Int
procs = MPIState.procs <$> readIORef stateRef


-----------------------------------------------------------------------------
-- startup and shutdown wrapper

-- Default buffer size is 4 KBytes or 32 KBytes.
-- Rationale:
-- * 4 KBytes is standard page size. If buffers were page aligned (which
--   we can't currently enforce but might happen when allocating multiples
--   of 4 KBytes) then the MPI or the OS might be able to achieve zero-copy 
--   send to the network (by mapping buffer pages to the network device
--   driver).
-- * 32 KBytes (minus some small overhead) is currently the default
--   chunk size for lazy bytestrings.
-- * 4 KBytes appears to perform better (sometimes by a factor of 2)
--   than 32 KBytes; the reason might be the GHC allocator for large
--   objects.
defaultBufferSize :: Int
defaultBufferSize = 4 * k where k = 1024
-- defaultBufferSize = 32 * k where k = 1024

defaultWithMPI :: IO () -> IO ()
defaultWithMPI = withMPI defaultBufferSize

withMPI :: Int -> IO () -> IO ()
withMPI bufferSize action =
  (init bufferSize >> action >> finalize)
  `onException` mpi_unclean_finalize 1

uncleanWithMPI :: Int -> IO () -> IO ()
uncleanWithMPI bufferSize action =
  (init bufferSize >> action >> mpi_unclean_finalize 0)
  `onException` mpi_unclean_finalize 1


-----------------------------------------------------------------------------
-- exported send and receive functions (accessing send/recv queues)

send :: Rank -> Msg -> IO ()
send dest msg = do
  state <- readIORef stateRef
  let (q,_) = MPIState.sendQs state ! dest
  writeChan q $ Just msg


recv :: IO MsgHandle
recv = do
  state <- readIORef stateRef
  readChan (MPIState.recvQ state)


-----------------------------------------------------------------------------
-- state of MPI module

deriving instance Eq Flag

initialized :: IO Bool
initialized = (== InMPI) <$> readMVar flagRef

finalized :: IO Bool
finalized = (== PostMPI) <$> readMVar flagRef


-----------------------------------------------------------------------------
-- initialisation

-- 'init size' initialises MPI and sets up send/recv buffers of 'size' bytes.
init :: Int -> IO ()
init size = do
  let name = "MP.MPI.init: "

  -- lock flag
  flag <- takeMVar flagRef

  -- init MPI (only 1 thread can ever get through this; abort on failure)
  processes <- mpi_init
  case processes of
    Nothing        -> error $ name ++ "full thread support failed"
    Just processes -> do

      -- check MPI not initialised before  
      unless (flag == PreMPI) $
        error $ name ++ "MPI initialised before"

      -- determine own rank and list of all ranks
      ownRank <- mpi_rank
      let ranks = take processes $ enumFrom rank0

      -- check argument (abort if buffer size too small)
      unless (size > Lazy.chunkOverhead) $
        error $ name ++ "buffer size not positive"

      -- allocate 1 receive and 'processes' send buffers (abort on failure);
      -- let's hope the system ensures automatic page alignment here
      bufferSpacePtr <- mallocBytes ((processes + 1) * size)
      when (bufferSpacePtr == nullPtr) $
        error $ name ++ "buffer allocation failed"

      -- allocate send queues associate send buffers
      queues <- replicateM processes newChan
      let sendBufferPtrs = iterate (`plusPtr` size) bufferSpacePtr
      let sendQueues = zip queues $ map SendBuf sendBufferPtrs
      let rankedSendQs = zip ranks sendQueues

      -- allocate active sender counter and senders terminated signal
      sendCounter <- newMVar processes
      sendSignal  <- newEmptyMVar

      -- allocate receive queue and associate receive buffer
      queue <- newChan
      let recvBuffer = RecvBuf (bufferSpacePtr `plusPtr` (processes * size))

      -- allocate receiver terminated signal
      recvSignal  <- newEmptyMVar

      -- initialise state
      modifyIORef stateRef $ \ _state ->
        State { MPIState.procs   = processes,
                MPIState.myRank  = ownRank,
                MPIState.bufSize = size,
                MPIState.sendQs  = array (head ranks, last ranks) rankedSendQs,
                MPIState.sendCtr = sendCounter,
                MPIState.sendSig = sendSignal,
                MPIState.recvQ   = queue,
                MPIState.recvBuf = recvBuffer, 
                MPIState.recvSig = recvSignal }

      -- fork receiver thread
      forkIO recvSrv

      -- fork sender threads (one per send queue)
      mapM_ (forkIO . sendSrv) rankedSendQs

      -- set and unlock flag
      putMVar flagRef InMPI


-----------------------------------------------------------------------------
-- finalisation

-- Properly finalizes MPI (all messages sent are received, resources freed)
finalize :: IO ()
finalize = do
  state <- readIORef stateRef
  let sendQueues = MPIState.sendQs state
  let (_,SendBuf bufferSpacePtr) = sendQueues ! rank0

  -- set flag
  takeMVar flagRef
  putMVar flagRef PostMPI

  -- flush send queues (including shutdown messages)
  mapM_ (\ (q,_) -> writeChan q Nothing) $ elems $ sendQueues

  -- wait for send servers to terminate
  takeMVar (MPIState.sendSig state)

  -- wait for receive server to terminate
  takeMVar (MPIState.recvSig state)

  -- destroy state
  modifyIORef stateRef $ \ s ->
    error $ show (MPIState.myRank s) ++ ": MP.MPI.State.stateRef destroyed"

  -- free buffers
  free bufferSpacePtr

  -- finalize MPI
  mpi_finalize


-----------------------------------------------------------------------------
-- send server; running in its own thread, processing one send queue

sendSrv :: (Rank, (SendQ, SendBuf)) -> IO ()
sendSrv (dest, (q, buf)) = do
  state <- readIORef stateRef
  let buf_size = MPIState.bufSize state
  let ctr      = MPIState.sendCtr state
  let sig      = MPIState.sendSig state
  let processQ :: IO ()
      processQ = do
        -- block waiting for a message to send
        job <- readChan q
        case job of
          Nothing  -> do -- send shutdown message
                         mpi_send_shutdown dest
                         -- decrement active sender threads counter
                         modifyMVar_ ctr $ \ n -> do
                           when (n <= 1) $
                             putMVar sig ()  -- signal 'all threads terminated'
                           return (n - 1)
          Just msg -> do -- send message and recurse
                         sendChunked dest buf buf_size msg
                         processQ
  processQ


sendChunked :: Rank -> SendBuf -> Int -> Msg -> IO ()
sendChunked dest buf buf_size msg =
  mapM_ (sendChunk dest buf) tagged_chunks where
    tagged_chunks = tag $ chunk (buf_size - Lazy.chunkOverhead) $ msg


{-
-- OLD version: copies chunk into send buffer
sendChunk :: Rank -> SendBuf -> TaggedChunk -> IO ()
sendChunk dest buf@(SendBuf bufPtr) (chunk, isTail) = do
  -- write data to send buffer
  pokeArray bufPtr $ Strict.unpack chunk
  -- call MPI send function
  mpi_send_chunk dest buf (Strict.length chunk) isTail
-}

-- NEW version: uses chunk as send buffer;
-- SendBuf arg superfluous; should rewrite to avoid alloc'ing send buffers
sendChunk :: Rank -> SendBuf -> TaggedChunk -> IO ()
sendChunk dest _ (chunk, isTail) = do
#ifdef HDPH_DEBUG
  dbg $ "! " ++ show dest ++ " " ++ show (take 256 (Strict.unpack chunk))
#endif
  Strict.unsafeUseAsCStringLen chunk $ \ (bufPtr,size) ->
    mpi_send_chunk dest (SendBuf (castPtr bufPtr)) size isTail


-----------------------------------------------------------------------------
-- receive server; running in its own thread, feeding receive queue

recvSrv :: IO()
recvSrv = do
  tab   <- newMsgHandleTable
  state <- readIORef stateRef
  let buf_size = MPIState.bufSize state
  let q        = MPIState.recvQ state
  let sig      = MPIState.recvSig state
  let buf      = MPIState.recvBuf state
  let feedQ :: IO ()
      feedQ = do
        -- block to receive a chunk
        sender <- mpi_recv_chunk buf buf_size
        case sender of
          Nothing     -> do -- no more msgs; signal termination and terminate
                            putMVar sig ()
          Just sender -> do -- handle chunk and recurse
                            handleChunk q tab sender buf
                            feedQ
  feedQ

{-      
-- OLD version: copies receive buffer to chunk byte-wise by (pack . peek)
handleChunk :: RecvQ -> MsgHandleTable -> Rank -> RecvBuf -> IO ()
handleChunk q tab sender (RecvBuf bufPtr) = do
  -- extract chunk length and flag marking tail chunk
  size <- mpi_recvd_size
  isTail <- mpi_recvd_tail
  -- extract chunk from buffer
  bytes <- peekArray size bufPtr
  let chunk = Strict.pack bytes
  evaluate chunk  -- forcing chunk to trigger packing
  -- pop through message handle table
  maybe_hdl <- updateMsgHandleTable tab sender isTail chunk
  case maybe_hdl of
    Just hdl -> -- new handle; feed it into receive queue
                writeChan q hdl
    Nothing  -> -- no new handle to be fed into receive queue
                return ()
-}

-- NEW version: copies receive buffer to chunk more efficiently
handleChunk :: RecvQ -> MsgHandleTable -> Rank -> RecvBuf -> IO ()
handleChunk q tab sender (RecvBuf bufPtr) = do
  -- extract chunk length and flag marking tail chunk
  size <- mpi_recvd_size
  isTail <- mpi_recvd_tail
  -- allocate a new byte string, copying receive buffer contents
  chunk <- Strict.create size $ \ chunkPtr -> copyBytes chunkPtr bufPtr size
  {- OBSOLETE
  -- strictly eval chunk
  evaluate (rnf chunk)
  -}
  -- pop through message handle table
  maybe_hdl <- updateMsgHandleTable tab sender isTail chunk
  case maybe_hdl of
    Just hdl -> -- new handle; feed it into receive queue
                writeChan q hdl
    Nothing  -> -- no new handle to be fed into receive queue
                return ()

{- OBSOLETE
instance NFData Strict.ByteString where
  rnf = Strict.foldl' (\ _ -> rnf) ()
-}


-----------------------------------------------------------------------------
-- packing and unpacking messages;
-- these functions create chunks that will fit into the buffer

pack :: [Word8] -> IO Msg
pack xs = do
  state <- readIORef stateRef
  let buf_size = MPIState.bufSize state
  return $ Lazy.fromChunks $ chunkBytes (buf_size - Lazy.chunkOverhead) xs

unpack :: Msg -> IO [Word8]
unpack = return . Lazy.unpack

chunkBytes :: Int -> [Word8] -> [Chunk]
chunkBytes n xs
  | n < 1     = error "MP.MPI.chunkBytes: chunk size not positive"
  | null xs   = [Strict.empty]
  | otherwise = go xs where
                  go [] = []
                  go xs = Strict.pack ys : go zs where (ys,zs) = splitAt n xs


-----------------------------------------------------------------------------
-- chunking byte strings

-- 'chunk n lbs' produces a non-null list of chunks 'bs' such that
-- * their concatentation yields 'lbs', and
-- * all but possibly the last chunk are of size 'n'.
chunk :: Int -> Lazy.ByteString -> [Chunk]
chunk n lbs
  | n < 1         = error "MP.MPI.chunk: chunk size not positive"
  | Lazy.null lbs = [Strict.empty]
  | otherwise     = go lbs where
                      go lbs | Lazy.null lbs = []
                             | otherwise     = bs : go lbs' where
                                                 (bs, lbs') = firstChunk n lbs

-- 'firstChunk n lbs' splits the first chunk of size 'n' off 'lbs';
-- 'n' must be positive and 'lbs' non-empty.
firstChunk :: Int -> Lazy.ByteString -> (Chunk, Lazy.ByteString)
firstChunk n lbs = (Strict.concat (Lazy.toChunks prefix), suffix) where
                     (prefix, suffix) = Lazy.splitAt (fromIntegral n) lbs

-- 'unchunk' is the inverse of 'chunk n', for any 'n'.
unchunk :: [Chunk] -> Lazy.ByteString
unchunk = Lazy.fromChunks


-- A tagged chunk is a pair consisting of a chunk and Boolean flag,
-- which is True iff the chunk in the tail chunk of a message
type TaggedChunk = (Chunk, Bool)

-- 'tag cs' tags the last chunk of the non-empty list 'cs' with 'True',
-- all other chunks with 'False'.
tag :: [Chunk] -> [TaggedChunk]
tag [c]    = [(c, True)]
tag (c:cs) = (c, False) : tag cs

-- 'untag' is the inverse of 'tag'.
untag :: [TaggedChunk] -> [Chunk]
untag = map fst


-----------------------------------------------------------------------------
-- managing chunks received

-- The message handle table maps ranks to message handles (Maybe-lifted).
-- Invariant: Slot i of the message handle table holds a handle iff in the past
-- rank i has sent chunks of a message for which the tail chunk is outstanding.
type MsgHandleTable = IOArray Rank (Maybe MsgHandle)

-- Allocate an empty message handle table.
newMsgHandleTable :: IO MsgHandleTable
newMsgHandleTable = do
  ranks <- allRanks
  newArray (head ranks, last ranks) Nothing

-- Update message handle table on receipt of a 'chunk' from 'sender';
-- the argument 'isTail' is True iff the chunk received is the tail chunk.
-- Returns a new message handle, if the chunk received is the first chunk from
-- 'sender', returns 'Nothing' otherwise.
updateMsgHandleTable ::
  MsgHandleTable -> Rank -> Bool -> Chunk -> IO (Maybe MsgHandle)
updateMsgHandleTable tab sender isTail chunk = do
  maybe_hdl <- readArray tab sender
  case maybe_hdl of
    Nothing  -> do hdl <- newMsgHandle sender
                   writeMsgHandle hdl chunk
                   if isTail
                      then do closeMsgHandle hdl
                              -- unnecessary: writeArray tab sender Nothing
                              return (Just hdl)
                      else do writeArray tab sender (Just hdl)
                              return (Just hdl)
    Just hdl -> do writeMsgHandle hdl chunk
                   if isTail
                      then do closeMsgHandle hdl
                              writeArray tab sender Nothing
                              return Nothing
                      else do -- unnecessary: writeArray tab sender (Just hdl)
                              return Nothing


-----------------------------------------------------------------------------
-- message handles

newMsgHandle :: Rank -> IO MsgHandle
newMsgHandle rank = do ch <- newChan
                       return $ MsgHandle_ rank ch

-- Beware: No call to 'writeMsgHandle' after 'closeMsgHandle'
closeMsgHandle :: MsgHandle -> IO ()
closeMsgHandle (MsgHandle_ _ ch) = writeChan ch Nothing

writeMsgHandle :: MsgHandle -> Chunk -> IO ()
writeMsgHandle (MsgHandle_ _ ch) = writeChan ch . Just

-- Returns the sender associated with the message handle.
sender :: MsgHandle -> Rank
sender (MsgHandle_ rank _) = rank

-- Lazily returns the message (in the IO monad) transmitted through the handle.
getMsg :: MsgHandle -> IO Msg
getMsg (MsgHandle_ _ ch) = unchunk <$> unJust <$> getChanContents ch


-----------------------------------------------------------------------------
-- foreign function declarations

---- used during initialisation

-- Initialises MPI; returns number of processes on success, Nothing otherwise;
-- must be called before any other 'mpi_' function
mpi_init :: IO (Maybe Int)
mpi_init = do
  procs <- c_mpi_init
  if procs < 0
     then return $ Nothing
     else return $ Just $ fromIntegral procs
foreign import ccall safe "wrap_mpi.h mpi_init"             -- safe (may block)
  c_mpi_init :: IO CInt

-- Returns the rank of the calling process
mpi_rank :: IO Rank
mpi_rank =
  toEnum <$> fromIntegral <$> c_mpi_rank
foreign import ccall unsafe "wrap_mpi.h mpi_rank"
  c_mpi_rank :: IO CInt


---- used for receiving message chunks

-- Blocks to receive a message chunk into the given buffer (which is of the 
-- the given size in Bytes); returns rank of sender on success,
-- Nothing if message traffic to this process has ceased
mpi_recv_chunk :: RecvBuf -> Int -> IO (Maybe Rank)
mpi_recv_chunk (RecvBuf buf) size = do
  sender <- c_mpi_recv_chunk buf (intToCInt size)
  if sender < 0
     then return $ Nothing
     else return $ Just $ toEnum $ fromIntegral sender
foreign import ccall safe "wrap_mpi.h mpi_recv_chunk"       -- safe (may block)
  c_mpi_recv_chunk :: Ptr a -> CInt ->  IO CInt

-- Returns the size (in Bytes) of the last chunk received;
-- call *after* 'mpi_recv_chunk' has returned successfully
-- and *before* any subsequent call to 'mpi_recv_chunk'
mpi_recvd_size :: IO Int
mpi_recvd_size =
  fromIntegral <$> c_mpi_recvd_size
foreign import ccall unsafe "wrap_mpi.h mpi_recvd_size"
  c_mpi_recvd_size :: IO CInt

-- Returns True iff the last chunk received was a tail chunk;
-- call *after* 'mpi_recv_chunk' has returned successfully
-- and *before* any subsequent call to 'mpi_recv_chunk'
mpi_recvd_tail :: IO Bool
mpi_recvd_tail = do
  is_tail <- c_mpi_recvd_tail
  return $ is_tail /= 0
foreign import ccall unsafe "wrap_mpi.h mpi_recvd_tail"
  c_mpi_recvd_tail :: IO CInt


---- used for sending message chunks

-- Sends a message chunk (of the given size) stored in the given buffer
-- to the given rank; the last argument is True iff the buffered chunk is
-- the message's tail chunk
mpi_send_chunk :: Rank -> SendBuf -> Int -> Bool -> IO ()
mpi_send_chunk dest (SendBuf buf) size isTail =
  c_mpi_send_chunk (rankToCInt dest) buf (intToCInt size) (boolToCInt isTail)
foreign import ccall safe "wrap_mpi.h mpi_send_chunk"       -- safe (may block)
  c_mpi_send_chunk :: CInt -> Ptr a -> CInt -> CInt -> IO ()


---- used during finalisation

-- Sends a shutdown message to the given rank;
-- no other message may be sent to that rank after this one
mpi_send_shutdown :: Rank -> IO ()
mpi_send_shutdown dest =
  c_mpi_send_shutdown (rankToCInt dest)
foreign import ccall safe "wrap_mpi.h mpi_send_shutdown"   -- safe (may block)
  c_mpi_send_shutdown :: CInt -> IO ()

-- Finalizes MPI cleanly;
-- call *after* all messages sent have been received;
-- no other 'mpi_' function may be called after this one
mpi_finalize :: IO ()
mpi_finalize =
  c_mpi_finalize
foreign import ccall safe "wrap_mpi.h mpi_finalize"         -- safe (may block)
  c_mpi_finalize :: IO ()

-- Aborts MPI uncleanly and returns the error code (to the program's caller);
-- no other 'mpi_' function may be called after this one
mpi_unclean_finalize :: Int -> IO ()
mpi_unclean_finalize errorCode =
  c_mpi_unclean_finalize (intToCInt errorCode)
foreign import ccall safe "wrap_mpi.h mpi_unclean_finalize" -- safe (may block)
  c_mpi_unclean_finalize :: CInt -> IO ()


-----------------------------------------------------------------------------
-- auxiliary functions

-- Un-Maybe-lift a list, cutting off at the first 'Nothing'
unJust :: [Maybe a] -> [a]
unJust (Nothing : _) = []
unJust (Just x : xs) = x : unJust xs

intToCInt :: Int -> CInt
intToCInt i = fromIntegral i

boolToCInt :: Bool -> CInt
boolToCInt False = 0
boolToCInt True  = 1

rankToCInt :: Rank -> CInt
rankToCInt = fromIntegral . fromEnum


-----------------------------------------------------------------------------
-- debugging

#ifdef HDPH_DEBUG
dbg :: String -> IO ()
dbg s = do
  i <- myRank
  hPutStrLn stderr $ show i ++ ": " ++ s
#endif


-----------------------------------------------------------------------------
--
-- Steps of shutdown protocol
--
-- 1) Every process initiating MPI finalisation flushes its send queues, which
--    entails broadcasting an empty shutdown message (including to itself).
-- 2) On receipt of a shutdown message, each process decrements its shutdown 
--    counter (which was initialised to the number of processes).
-- 3) When a process' shutdown counter reaches 0 then that process has 
--    received shutdown messages from all processes (including itself).
--    Since a shutdown message is the last message any process sends,
--    that means that this process will not receive any further messages,
--    hence can shut down its receive server.
-- 4) Process frees send and receive buffers.
-- 5) Process calls MPI_Finalize().


-----------------------------------------------------------------------------
-- FUTURE WORK

-- Cut down on the number of send buffers.
--
-- Instead of one send buffer per rank, maintain a fixed pool of send
-- buffers, and a single message send queue. Pick a message from the
-- queue as soon as a send buffer is available.
-- Beware: Do not send two messages concurrently to the same destination.
-- Hence, need to record which destinations are currently being sent messages.
-- As a result, may need to pick not front of the queue, but message
-- behind front.

-- Implement throttle control for long messages.
--
-- Messages are being pushed rather than pulled, hence the receiver of a
-- long message may not be able to consume it straight away, which may
-- result in the message filling up the receiver's memory.
-- As a remedy, implement a throttle control. Transmit long messages in
-- bursts of n chunks. When receiver has consumed all but m < n of these
-- chunks then it demands more (sending a special message, to be received
-- out-of-band by the sender), resulting in another burst of n chunks.
-- Requires: queue that can track their current length.

-- Limit contention by limiting number of senders to a particular rank.
--
-- Again due to message pushing, a receiver can be overwhelmed by the
-- number of senders. It is unclear, though, how to react to this scenario.
-- Accepting connections from a fixed number of senders at any given time
-- is possible but may lead to MPI system queues being filled up instead.
-- Would be worth a test, though.
-- Otherwise, the only remedy would be to implement a pulling (rather
-- than a pushing) protocol on top of the MPI module.

-- Latency hiding while sending.
--
-- Use a circular ring of pre-allocated buffers (at least two), presented
-- as an infinite list of buffers. While MPI_Send blocks to send one buffer,
-- another one can be filled concurrently. Should also develop a custom
-- version of Binary/Serialize that takes a lazy stream of empty buffers
-- and packs directly into these (rather than allocating on its own).

-- Use MPI_Probe to determine the size of messages. 
--
-- Before actually receiving a message use MPI_Probe to learn its size.
-- Allocate a properly sized receive buffer, then use MPI_Recv *and* pass
-- the source rank and tag found by MPI_Probe as arguments.

-- Use one bit of the MPI_TAG to mark system messages.
--
-- There should thus be 3 bits to encode tags:
-- > SHUTDOWN bit (always set on its own)
-- > TAIL/CHUNK bit, combined with
-- > SYSTEM/PAYLOAD bit.
-- These make up 5 different tag values.
