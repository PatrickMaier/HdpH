-- Message passing via MPI; stateful sub-module
-- Variant: Messages represented as lazy byte strings
--
-- Visibility: MP.MPI internal
-- Author: Patrick Maier <P.Maier@hw.ac.uk>
-- Created: 30 May 2011
--
-----------------------------------------------------------------------------

{-# OPTIONS_GHC -fno-cse #-}     -- required to protect unsafePerformIO hack

module MP.MPI.State_ByteString
  ( -- * MPI rank
    Rank(..),

    -- * message queues and the like
    Msg,
    Chunk,
    MsgHandle(..),
    RecvQ,
    SendQ,
    SendQs,

    -- * message buffers
    SendBuf(..),
    RecvBuf(..),

    -- * internal state
    State(..),
    stateRef,
    Flag(..),
    flagRef
  ) where

import Prelude
import Control.Concurrent.Chan (Chan)
import Control.Concurrent.MVar (MVar, newMVar)
import Data.Array (Array)
import qualified Data.ByteString as Strict (ByteString)
import qualified Data.ByteString.Lazy as Lazy (ByteString)
import Data.IORef (IORef, newIORef)
import Data.Word (Word8)
import Foreign.Ptr (Ptr)
import System.IO.Unsafe (unsafePerformIO)


-- MPI rank (represented as Int)
newtype Rank = Rank_ Int


-- Messages (represented as lazy byte strings)
type Msg = Lazy.ByteString


-- Chunks of messages (represented as strict byte strings)
type Chunk = Strict.ByteString


-- Message handles (realised as pair of sender rank and channel of
-- strict byte strings, Mabye lifted to signal end)
data MsgHandle = MsgHandle_ Rank (Chan (Maybe Chunk))


-- Receive queue (channel of message handles)
type RecvQ = Chan MsgHandle


-- Send and receive buffers (ptrs to memory with byte-wise access)
newtype SendBuf = SendBuf (Ptr Word8)
newtype RecvBuf = RecvBuf (Ptr Word8)


-- Send queue (channel of messages, Maybe lifted)
type SendQ = Chan (Maybe Msg)


-- Send queues with associated send buffers (destination rank indexed array 
-- of pairs consisting of a send queue and send buffer)
type SendQs = Array Rank (SendQ, SendBuf)


-- Internal state
data State = State {
               procs   :: Int,      -- number of processes  
               myRank  :: Rank,     -- rank of this process
               bufSize :: Int,      -- length of buffers (in bytes)
               sendQs  :: SendQs,   -- array of send queues and ptrs to buffers
               sendCtr :: MVar Int, -- count active sender threads
               sendSig :: MVar (),  -- signal sender threads terminated
               recvQ   :: RecvQ,    -- receive queue
               recvBuf :: RecvBuf,  -- ptr to receive buffer
               recvSig :: MVar () } -- signal receiver thread terminated


{-# NOINLINE stateRef #-}  -- required to protect unsafePerformIO hack
stateRef :: IORef State
stateRef = unsafePerformIO $
             newIORef $ error "MP.MPI.State.stateRef not initialised"


-- Flag reflecting status of MPI module
data Flag = PreMPI | InMPI | PostMPI


{-# NOINLINE flagRef #-}  -- required to protect unsafePerformIO hack
flagRef :: MVar Flag
flagRef = unsafePerformIO $             
            newMVar PreMPI
