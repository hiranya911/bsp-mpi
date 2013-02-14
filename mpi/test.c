#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "mpi.h"

int main(int argc, char *argv[]) {
  MPI_Init(&argc, &argv);
  int rank;
  int size;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &size);
  printf("I'm %d out of %d nodes\n", rank, size);
  char* data = "Hello World\n";
  if (rank == 0) {
    MPI_Send(data, strlen(data) + 1, MPI_CHAR, 1, 100, MPI_COMM_WORLD);
    printf("Sent message...\n");
    int numbers[] = {1, 2, 3, 4, 5};
    MPI_Send(numbers, 5, MPI_INT, 1, 101, MPI_COMM_WORLD);
    printf("Sent numbers...\n");

    double* matrix = malloc(sizeof(double) * 1000 * 1000);
    int i = 0;
    for (i = 0; i < 1000 * 1000; i++) {
      matrix[i] = (double) i;
    }
    printf("%g %g\n", matrix[0], matrix[1000*1000 - 1]);
    MPI_Bcast(matrix, 1000 * 1000, MPI_DOUBLE, 0, MPI_COMM_WORLD);
    printf("Sent matrix...\n");
    free(matrix);
  } else {
    char output[13];
    MPI_Status status;
    MPI_Recv(output, 13, MPI_CHAR, 0, 100, MPI_COMM_WORLD, &status);
    printf("%s", output);
    int numbers[5];
    MPI_Recv(numbers, 5, MPI_INT, 0, 101, MPI_COMM_WORLD, &status);
    int i;
    for (i = 0; i < 5; i++) {
      printf("%d ", numbers[i]);
    }
    printf("\n");
    double matrix[1000 * 1000];
    MPI_Bcast(matrix, 1000 * 1000, MPI_DOUBLE, 0, MPI_COMM_WORLD);
    printf("%g %g\n", matrix[0], matrix[1000000 - 1]);
  }
  MPI_Finalize();
  return 0;
}
