-- Node to node communication; wrapper module
--
-- Visibility: HdpH, HdpH.Internal
-- Author: Patrick Maier <P.Maier@hw.ac.uk>
-- Created: 06 May 2011
--
-----------------------------------------------------------------------------

module HdpH.Internal.Comm
  ( -- re-exports from HdpH.Internal.Comm_MPI

    -- * CommM monad
    CommM,           -- synonym: Control.Monad.Reader.ReaderT <State> IO
    run_,            -- :: RTSConf -> CommM () -> IO ()
    runWithMPI_,     -- :: CommM () -> IO ()
    liftIO,          -- :: IO a -> CommM a

    -- * information about the virtual machine
    nodes,           -- :: CommM Int
    allNodes,        -- :: CommM [NodeId]
    myNode,          -- :: CommM NodeId
    isMain,          -- :: CommM Bool

    -- * sending and receiving messages
    Message,         -- synomyn: MPI.Msg (= Data.ByteString.Lazy.ByteString)
    send,            -- :: NodeId -> Message -> CommM ()
    receive,         -- :: CommM Message
    shutdown,        -- :: CommM ()
    waitShutdown     -- :: CommM ()
  ) where
       
-- import what's re-exported above
import HdpH.Internal.Comm_MPI
