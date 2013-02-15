#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "mpi.h"

int count = 1000 * 1000;

int main(int argc, char *argv[]) {
  MPI_Init(&argc, &argv);
  int rank;
  int size;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &size);
  printf("I'm %d out of %d nodes\n", rank, size);
  char* data = "Hello World\n";
  if (rank == 0) {
    FILE* log = fopen("/tmp/main.log", "a");
    fprintf(log, "Starting\n");
    fflush(log);
    int i;
    MPI_Send(data, strlen(data) + 1, MPI_CHAR, 1, 100, MPI_COMM_WORLD);
    printf("Sent message...\n");
    
    double* numbers = malloc(sizeof(double) * count);
    for (i = 0; i < count; i++) {
      numbers[i] = i;
    }
    fprintf(log, "Ready to send\n");
    fflush(log);
    MPI_Send(numbers, count, MPI_DOUBLE, 1, 101, MPI_COMM_WORLD);
    printf("Sent numbers...\n");
    free(numbers);

    double* matrix = malloc(sizeof(double) * 1000 * 1000);
    for (i = 0; i < 1000 * 1000; i++) {
      matrix[i] = (double) i;
    }
    MPI_Bcast(matrix, 1000 * 1000, MPI_DOUBLE, 0, MPI_COMM_WORLD);
    printf("Sent matrix...\n");
    free(matrix);

    int numbers2[] = { 1, 2, 3, 4, 5 };
    MPI_Reduce(MPI_IN_PLACE, numbers2, 5, MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);

    for (i = 0; i < 5; i++) {
      printf("%d ", numbers2[i]);
    }
    printf("\n");
  } else {
    char output[13];
    MPI_Status status;
    MPI_Recv(output, 13, MPI_CHAR, 0, 100, MPI_COMM_WORLD, &status);
    printf("%s", output);
    double* numbers = malloc(sizeof(double) * count);
    MPI_Recv(numbers, count, MPI_DOUBLE, 0, 101, MPI_COMM_WORLD, &status);
    printf("%g %g\n", numbers[0], numbers[count - 1]);
    printf("\n");
    free(numbers);

    double matrix[1000 * 1000];
    MPI_Bcast(matrix, 1000 * 1000, MPI_DOUBLE, 0, MPI_COMM_WORLD);
    printf("%g %g\n", matrix[0], matrix[1000000 - 1]);

    int numbers2[] = { 1, 2, 3, 4, 5 };
    MPI_Reduce(numbers2, numbers2, 5, MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);
  }
  MPI_Finalize();
  return 0;
}
