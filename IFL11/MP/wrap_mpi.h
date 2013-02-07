// FFI interface to MPI; C bits header
//
// Visibility: MP.MPI internal
// Author: Patrick Maier <P.Maier@hw.ac.uk>
// Created: 28 May 2011
//
/////////////////////////////////////////////////////////////////////////////

/////////////////////////////////////////////////////////////////////////////
// type synonyms

// Booleans with the usual C encoding: 0 = False, non-zero integer = True
typedef int bool_t;

// Number of processes: positive integer
typedef int procs_t;

// Maybe-lifted procs_t: -1 = Nothing
typedef int maybe_procs_t;

// Rank of a process: non-negative integer
typedef int rank_t;

// Maybe-lifted rank_t: -1 = Nothing
typedef int maybe_rank_t;

// Send resp. receive buffers: non-NULL pointers
typedef void* send_buf_t;
typedef void* recv_buf_t;

// Size of send and receive buffers (in bytes): positive integer
typedef int buf_size_t;

// Size of a message chunk (in bytes): non-negative integer
typedef int chunk_size_t;

// Error code with usual POSIX encoding: 0 = success, positive integer = error
typedef int error_t;


/////////////////////////////////////////////////////////////////////////////
// function prototypes

// Initialises MPI; returns number of processes on success, Nothing otherwise;
// must be called before any other function
maybe_procs_t mpi_init();

// Returns the rank of the calling process
rank_t mpi_rank();


// Blocks to receive a message chunk into the given buffer;
// returns rank of sender on success, Nothing if message traffic has ceased
maybe_rank_t mpi_recv_chunk( recv_buf_t, buf_size_t );

// Returns the size of the last chunk received;
// call *after* mpi_recv_chunk has returned successfully and *before* any
// subsequent call to mpi_recv_chunk
chunk_size_t mpi_recvd_size();

// Returns True iff the last chunk received was a tail chunk;
// call *after* mpi_recv_chunk has returned successfully and *before* any
// subsequent call to mpi_recv_chunk
bool_t mpi_recvd_tail();


// Sends a message chunk stored in the given buffer to the given destination;
// the last argument is True iff this is the message's tail chunk
void mpi_send_chunk( rank_t, send_buf_t, chunk_size_t, bool_t );


// Sends a shutdown message to the given rank;
// no other message may be sent to that rank after this one
void mpi_send_shutdown( rank_t );


// Finalizes MPI cleanly;
// call *after* all messages sent have been received;
// no other function may be called after this one
void mpi_finalize();

// Aborts MPI uncleanly and returns the error code (to program's caller);
// no other function may be called after this one
void mpi_unclean_finalize( error_t );
