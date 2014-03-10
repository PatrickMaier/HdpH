#include <stdlib.h>
#include <mpi.h>

// constant MPI_SUCCESS
int mPI_SUCCESS() { return MPI_SUCCESS; }

// MPI_Init(), with args specialised to NULL pointers
int mPI_Init() { return MPI_Init(NULL, NULL); }

// MPI_Errhandler_set(), with
//   comm arg specialised to MPI_COMM_WORLD,
//   handler arg specialised to MPI_ERRORS_RETURN
int mPI_Errhandler_set_return()
  { return MPI_Errhandler_set(MPI_COMM_WORLD, MPI_ERRORS_RETURN); }

// MPI_Finalize()
int mPI_Finalize() { return MPI_Finalize(); }

// MPI_Comm_size(), with comm arg specialised to MPI_COMM_WORLD
int mPI_Comm_world_size(int* size_ptr)
  { return MPI_Comm_size(MPI_COMM_WORLD, size_ptr); }

// MPI_Comm_rank(), with comm arg specialised to MPI_COMM_WORLD
int mPI_Comm_world_rank(int* rank_ptr)
  { return MPI_Comm_rank(MPI_COMM_WORLD, rank_ptr); }

// MPI_Allgather(), with
//   sendtype and recvtype args specialised to MPI_INT,
//   sendcount and recvcount args specialised to 1,
//   comm arg specialised to MPI_COMM_WORLD
int mPI_Allgather_1int(int* sendbuf, int* recvbuf)
  { return MPI_Allgather(sendbuf, 1, MPI_INT,
                         recvbuf, 1, MPI_INT,
                         MPI_COMM_WORLD); }

// MPI_Allgatherv(), with
//   sendtype and recvtype args specialised to MPI_BYTE,
//   comm arg specialised to MPI_COMM_WORLD
int mPI_Allgatherv_bytes(char* sendbuf, int sendcount,
                         char* recvbuf, int* recvcounts, int* displs)
  { return MPI_Allgatherv(sendbuf, sendcount, MPI_BYTE,
                          recvbuf, recvcounts, displs, MPI_BYTE,
                          MPI_COMM_WORLD); }

// Sums array 'x' of 'n' non-negative integers, storing partial sums in
// array 'sum_upto'; returns the total sum, or -1 if there was an overflow
// (assuming that the value of 'max_int' is the largest 'int').
int c_scan_sum(int max_int, int n, int *x, int *sum_upto)
  {
    unsigned int sum, i;
    for (i = 0; i < n; i++)
      sum_upto[i] = -1;
    sum = 0;
    for (i = 0; i < n; i++) {
      sum_upto[i] = sum;
      sum = sum + x[i];
      if (sum > max_int)
        return -1;
    }
    return sum;
  }
