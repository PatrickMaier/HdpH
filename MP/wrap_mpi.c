// FFI interface to MPI; C bits
//
// Visibility: MP.MPI internal
// Author: Patrick Maier <P.Maier@hw.ac.uk>
// Created: 28 May 2011
//
/////////////////////////////////////////////////////////////////////////////

#include <stdlib.h>
#include "mpi.h"
#include "wrap_mpi.h"

#ifdef MPI_DEBUG
#include <stdio.h>
#endif


// Assumption: Errors are fatal, ie. MPI will abort the job on discovering
//             the first error. Hence, we don't need to check return values
//             for MPI functions (they don't return on error).


// Message tags
#define TAIL_TAG     1  // tags the last chunk of a normal message
#define CHUNK_TAG    2  // tags any other chunk of a normal message
#define SHUTDOWN_TAG 4  // tags an (empty) shutdown message


// Internal state
static MPI_Comm comm;      // local communicator (shadowing MPI_COMM_WORLD)
static MPI_Status status;  // status of most recent MPI_Recv
static int shutdown_ctr;   // counting down shutdown messages received


/////////////////////////////////////////////////////////////////////////////
// functions used during initialisation

maybe_procs_t mpi_init()
{
  int provided;
  int ranks;

  // init MPI with full thread support
  MPI_Init_thread(NULL, NULL, MPI_THREAD_MULTIPLE, &provided);

  // dup communicator: local copy of MPI_COMM_WORLD
  MPI_Comm_dup(MPI_COMM_WORLD, &comm);

  // init shutdown ctr to number of processes
  MPI_Comm_size(comm, &ranks);
  shutdown_ctr = ranks;

  // return Nothing if full thread support is denied
  if (provided != MPI_THREAD_MULTIPLE)
    return -1;

  // otherwise return number of processes
  return ranks;
}


rank_t mpi_rank()
{
  int rank;

  MPI_Comm_rank(comm, &rank);
  return rank;
}


/////////////////////////////////////////////////////////////////////////////
// functions used for receiving message chunks

maybe_rank_t mpi_recv_chunk( recv_buf_t buf,
                             buf_size_t size )
{
  while (shutdown_ctr > 0)
  {
#ifdef MPI_DEBUG
    int myRank;
    MPI_Comm_rank(comm, &myRank);
    printf("[%d] receive posted\n", myRank);
    fflush(stdout);
#endif

    // receive a message
    MPI_Recv(buf, size, MPI_BYTE, MPI_ANY_SOURCE, MPI_ANY_TAG, comm, &status);

    // check tag
    switch (status.MPI_TAG) {
      case TAIL_TAG:
      case CHUNK_TAG:
        // return sender
        return status.MPI_SOURCE;

      case SHUTDOWN_TAG:
        // decrement shutdown counter and continue
        shutdown_ctr--;
        break;

      default:
        // do nothing (ie. ignore message) and continue
        break;
    }
  }

  // shutdown_ctr == 0, ie. message traffic has ceased; return Nothing
  return -1;
}


chunk_size_t mpi_recvd_size()
{
  int count;

  // status must contain info about size of last chunk received
  MPI_Get_count(&status, MPI_BYTE, &count);
  return count;
}


bool_t mpi_recvd_tail()
{
  // status.MPI_TAG must be either TAIL_TAG or CHUNK_TAG
  return status.MPI_TAG == TAIL_TAG;
}

/////////////////////////////////////////////////////////////////////////////
// functions used for sending message chunks

void mpi_send_chunk( rank_t dest,
                     send_buf_t buf,
                     chunk_size_t size,
                     bool_t is_tail )
{
#ifdef MPI_DEBUG
  int myRank;
  MPI_Comm_rank(comm, &myRank);
  printf("[%d] sending %s (%d bytes) to %d\n",
         myRank, is_tail ? "TAIL" : "CHUNK", size, dest);
  fflush(stdout);
#endif

  // send message chunk (tagged as TAIL or CHUNK)
  MPI_Send(buf, size, MPI_BYTE, dest, is_tail ? TAIL_TAG : CHUNK_TAG, comm);

#ifdef MPI_DEBUG
  printf("[%d] sent\n", myRank);
  fflush(stdout);
#endif
}


/////////////////////////////////////////////////////////////////////////////
// functions used during finalisation

void mpi_send_shutdown( rank_t dest )
{
#ifdef MPI_DEBUG
  int myRank;
  MPI_Comm_rank(comm, &myRank);
  printf("[%d] sending SHUTDOWN to %d\n", myRank, dest);
  fflush(stdout);
#endif

  // send empty message (tagged as shutdown message), buffer address is NULL
  MPI_Send(NULL, 0, MPI_BYTE, dest, SHUTDOWN_TAG, comm);

#ifdef MPI_DEBUG
  printf("[%d] SHUTDOWN sent\n", myRank);
  fflush(stdout);
#endif
}


void mpi_finalize()
{
  // free internal communicator
  MPI_Comm_free(&comm);

  // terminate MPI environment
  MPI_Finalize();
}


void mpi_unclean_finalize( error_t error_code )
{
  // abort MPI environment with given error code
  MPI_Abort(comm, error_code);
}
