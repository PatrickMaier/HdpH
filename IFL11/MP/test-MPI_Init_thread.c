// Test whether MPI can provide MPI_THREAD_MULTIPLE
//
// compile: mpicc -o test-MPI_Init_thread test-MPI_Init_thread.c
// run:     ./test-MPI_Init_thread

#include <stdio.h>
#include "mpi.h"

int main(int argc, char** argv)
{
  int provided;

  MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);
  printf("MPI_Init_thread: required=%d provided=%d\n",
         MPI_THREAD_MULTIPLE, provided);
  MPI_Finalize();
  return 0;
}
