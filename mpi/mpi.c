#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>

#include "mpi.h"

int INITIALIZED = 0;

FILE* input;
FILE* output;

int MPI_Init(int *argc, char ***argv) {
  INITIALIZED = 1;

  char* input_path = getenv("bsp.mpi.imf"); 
  input = fopen(input_path, "w");
  if (input == NULL) {
    return -1;
  }

  char* output_path = getenv("bsp.mpi.omf");
  output = fopen(output_path, "r");
  if (output == NULL) {
    return -1;
  }

  fprintf(input, "MPI_Init\n\n");
  fflush(input);
  return 0;
}

int MPI_Finalize(void) {
  if (!INITIALIZED) {
    return -1;
  }

  fprintf(input, "MPI_Finalize\n\n");
  fflush(input);
  fclose(input);
  fclose(output);
  return 0;
}

int MPI_Comm_size(MPI_Comm comm, int *size) {
  if (!INITIALIZED) {
    return -1;
  }

  fprintf(input, "MPI_Comm_size\ncomm=%d\n\n", comm);
  fflush(input);
  fscanf(output, "%d", size);
  return 0;
}

int MPI_Comm_rank(MPI_Comm comm, int *rank) {
  if (!INITIALIZED) {
    return -1;
  }

  fprintf(input, "MPI_Comm_rank\ncomm=%d\n\n", comm);
  fflush(input);
  fscanf(output, "%d", rank);
  return 0;
}

int MPI_Send(void* buffer, int count, MPI_Datatype type, int dest, int tag, MPI_Comm comm) {
  //write_int(f,);
}

int MPI_Recv(void* buffer, int count, MPI_Datatype type, int source, int tag, MPI_Comm comm, MPI_Status *status) {

}

