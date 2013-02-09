#include "mpi.h"
#include <stdio.h>
#include <unistd.h>
#include <signal.h>
#include <stdlib.h>

char* get_input_meta_file();
char* get_output_meta_file();
void wait_for_output();

int INITIALIZED = 0;

FILE* input;
FILE* output;

int MPI_Init(int *argc, char ***argv) {
  INITIALIZED = 1;

  input = fopen(get_input_meta_file(), "w");
  if (input == NULL) {
    return -1;
  }

  output = fopen(get_output_meta_file(), "r");
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

  fprintf(input, "MPI_Comm_size\n\n");
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

char* get_input_meta_file() {
  return (char*) getenv("bsp.mpi.imf");
}

char* get_output_meta_file() {
  return (char*) getenv("bsp.mpi.omf");
}

void wait_for_output() {
  char buffer[8];
  fgets(buffer, 8, output);
}
