{-# LANGUAGE ForeignFunctionInterface #-}

module Control.Parallel.HdpH.MPI.Allgather
  ( -- * MPI wrapper
    Size(..),            -- Size type (wraps an Int)
    Rank(..),            -- Rank type (wraps an Int)
    withMPI,             -- :: (Size -> Rank -> IO a) -> IO a

    -- * allgather operation
    allgatherByteString  -- :: ByteString -> IO [ByteString]
  ) where

import Prelude hiding (sum)
import Control.Concurrent.MVar (MVar, newEmptyMVar, putMVar, takeMVar, withMVar)
import Control.Monad (when)
import Data.ByteString (ByteString)
import qualified Data.ByteString
       as BS (take, drop, useAsCStringLen, packCStringLen)
import Data.Functor ((<$>))
import Data.IORef (IORef, newIORef, atomicModifyIORef')
import System.IO.Unsafe (unsafePerformIO)

import Foreign.C (CInt(..), CChar)
import Foreign.Ptr (Ptr)
import Foreign.Marshal.Alloc (alloca, mallocBytes, free)
import Foreign.Marshal.Array (mallocArray, peekArray)
import Foreign.Storable (peek, poke)

thisModule :: String
thisModule = "Control.Parallel.HdpH.MPI.Allgather"

-------------------------------------------------------------------------------
-- MPI wrapper

-- | Wrapper type for size of MPI_COMM_WORLD.
newtype Size = Size { sizeInt :: Int }

-- | Wrapper type for rank in MPI_COMM_WORLD.
newtype Rank = Rank { rankInt :: Int }

-- | Startup MPI, execute 'action', and shutdown MPI.
--   The 'action' will be supplied with the size of MPI_COMM_WORLD
--   (ie. the number of MPI processes) and the rank (ie. a process ID).
--   Note that 'withMPI' can be called at most once; subsequent or
--   concurrent calls will abort with an error.
withMPI :: (Size -> Rank -> IO a) -> IO a
withMPI action = do
  isInit <- initTestAndSet
  if isInit
    then error $ thisModule ++ ".withMPI: MPI already started"
    else do
      mpi_Init
      putMVar busyRef ()
      size <- mpi_Comm_world_size
      rank <- mpi_Comm_world_rank
      result <- action (Size size) (Rank rank)
      _ <- takeMVar busyRef
      mpi_Errhandler_set_return
      mpi_Finalize
      return result


-------------------------------------------------------------------------------
-- distribute ByteStrings

-- | Contibutes given byte string via synchronous allgather operation,
--   returning a strict list of all byte strings contributed by participants
--   (in an unspecified order).
--   Must be called within 'withMPI' block; calling outwith may lead to
--   undefined results, including deadlocks.
allgatherByteString :: ByteString -> IO [ByteString]
allgatherByteString bs0 =
  withMVar busyRef $ \ _ ->
    -- MVar ensures thread-safe access to MPI
    mpi_Allgatherv_bytestring bs0


-------------------------------------------------------------------------------
-- raw foreign imports (defined in cbits/mpi_wrapper.c)

foreign import ccall unsafe
  mPI_SUCCESS :: CInt

foreign import ccall safe
  mPI_Init :: IO CInt

foreign import ccall safe
  mPI_Errhandler_set_return :: IO CInt

foreign import ccall safe
  mPI_Finalize :: IO CInt

foreign import ccall safe
  mPI_Comm_world_size :: Ptr CInt -> IO CInt

foreign import ccall safe
  mPI_Comm_world_rank :: Ptr CInt -> IO CInt

foreign import ccall safe
  mPI_Allgather_1int :: Ptr CInt -> Ptr CInt -> IO CInt

foreign import ccall safe
  mPI_Allgatherv_bytes :: Ptr CChar -> CInt -> Ptr CChar -> Ptr CInt -> Ptr CInt
                       -> IO CInt

foreign import ccall safe
  c_scan_sum :: CInt -> CInt -> Ptr CInt -> Ptr CInt -> IO CInt


-------------------------------------------------------------------------------
-- sanitised foreign imports (doing error handling and buffer allocation)

mpi_Init :: IO ()
mpi_Init = do
  err_code <- mPI_Init
  when (err_code /= mPI_SUCCESS) $
    fail $ "MPI_Init() returned error code " ++ show err_code

mpi_Errhandler_set_return :: IO ()
mpi_Errhandler_set_return = do
  err_code <- mPI_Errhandler_set_return
  when (err_code /= mPI_SUCCESS) $
    fail $ "MPI_Errhandler_set() returned error code " ++ show err_code

mpi_Finalize :: IO ()
mpi_Finalize = do
  err_code <- mPI_Finalize
  when (err_code /= mPI_SUCCESS) $
    fail $ "MPI_Finalize() returned error code " ++ show err_code

mpi_Comm_world_size :: IO Int
mpi_Comm_world_size = alloca $ \ size_ptr -> do
  err_code <- mPI_Comm_world_size size_ptr
  when (err_code /= mPI_SUCCESS) $
    fail $ "MPI_Comm_size() returned error code " ++ show err_code
  cIntToInt <$> peek size_ptr

mpi_Comm_world_rank :: IO Int
mpi_Comm_world_rank = alloca $ \ rank_ptr -> do
  err_code <- mPI_Comm_world_rank rank_ptr
  when (err_code /= mPI_SUCCESS) $
    fail $ "MPI_Comm_rank() returned error code " ++ show err_code
  cIntToInt <$> peek rank_ptr

mpi_Allgather_1int :: Int -> Int -> IO (Ptr CInt)
mpi_Allgather_1int n x0 = do
  recvbuf <- mallocArray n
  alloca $ \ sendbuf -> do
    poke sendbuf (intToCInt x0)
    err_code <- mPI_Allgather_1int sendbuf recvbuf
    when (err_code /= mPI_SUCCESS) $
      fail $ "MPI_Allgather() returned error code " ++ show err_code
    return recvbuf

scan_sum :: Int -> Ptr CInt -> IO (Ptr CInt, Int)
scan_sum n countbuf = do
  displbuf <- mallocArray n
  z <- c_scan_sum (maxBound :: CInt) (intToCInt n) countbuf displbuf
  when (z < 0) $
    error $ thisModule ++ ".scan_sum: overflow"
  let sum = cIntToInt z
  sum `seq` return (displbuf, sum)

mpi_Allgatherv_bytestring :: ByteString -> IO [ByteString]
mpi_Allgatherv_bytestring bs0 = do
  n <- mpi_Comm_world_size
  -- allocate send buffer
  BS.useAsCStringLen bs0 $ \ (sendbuf, count0) -> do
    -- collect lengths of all n bytestrings
    countbuf <- mpi_Allgather_1int n count0
    -- compute displacements and total size
    (displbuf, sum_count) <- scan_sum n countbuf
    -- allocate receive buffers
    recvbuf <- mallocBytes sum_count
    -- collect all bytestrings
    err_code <- mPI_Allgatherv_bytes sendbuf (intToCInt count0)
                                     recvbuf countbuf displbuf
    when (err_code /= mPI_SUCCESS) $
      fail $ "MPI_Allgatherv() returned error code " ++ show err_code
    -- pack receive buffer into bytestring
    bs <- BS.packCStringLen (recvbuf, sum_count)
    -- split bytestring into list of n bytestrings
    counts <- map cIntToInt <$> peekArray n countbuf
    displs <- map cIntToInt <$> peekArray n displbuf
    let bss = zipWith (\ d c -> BS.take c $ BS.drop d bs) displs counts
    -- free buffers (but normalise bss first)
    seqList bss `seq` do { free recvbuf; free countbuf; free displbuf }
    -- return list of bytestrings
    return bss


-------------------------------------------------------------------------------
-- auxiliary functions

cIntToInt :: CInt -> Int
cIntToInt x
  | i < toInteger (minBound :: Int) = error "cIntToInt: arg too small"
  | i > toInteger (maxBound :: Int) = error "cIntToInt: arg too big"
  | otherwise                       = fromInteger i
    where
      i = toInteger x

intToCInt :: Int -> CInt
intToCInt x
  | i < toInteger (minBound :: CInt) = error "intToCInt: arg too small"
  | i > toInteger (maxBound :: CInt) = error "intToCInt: arg too big"
  | otherwise                        = fromInteger i
    where
      i = toInteger x


-- force all entries of a list to WHNF
seqList :: [a] -> ()
seqList []     = ()
seqList (x:xs) = x `seq` seqList xs


-------------------------------------------------------------------------------
-- state (enforcing MPI wrapper called once and allgather thread safe)

initRef :: IORef Bool
initRef = unsafePerformIO $ newIORef False
{-# NOINLINE initRef #-}    -- required to protect unsafePerformIO hack


-- returns False the first time it is called; True subsequently
initTestAndSet :: IO Bool
initTestAndSet = atomicModifyIORef' initRef $ \ flag -> (True, flag)


busyRef :: MVar ()
busyRef = unsafePerformIO $ newEmptyMVar
{-# NOINLINE busyRef #-}    -- required to protect unsafePerformIO hack
