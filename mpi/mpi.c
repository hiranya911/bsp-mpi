#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>

#include "mpi.h"
#include "cpool.h"

#define TRUE 1
#define FALSE 0

int mpi2bsp(char* input, void* input_data, int data_length, char* output, int length);

int INITIALIZED = FALSE;
struct conn_pool* pool;

int MPI_Init(int *argc, char ***argv) {
  if (INITIALIZED) {
    return -1;
  }

  INITIALIZED = TRUE;
  char* port = getenv("bsp.mpi.port");
  pool = new_connection_pool("localhost", atoi(port));
  char output[8];
  mpi2bsp("MPI_Init\n\n", NULL, 0, output, 8);
  return 0;
}

int mpi2bsp(char* input, void* input_data, int data_length, char* output, int length) {
  int sockfd = get(pool);
  if (sockfd < 0) {
    printf("Failed to open a socket to the parent process\n");
    exit(1);
  }
  send(sockfd, input, strlen(input), 0);
  if (data_length > 0) {
    send(sockfd, input_data, data_length, 0);
  }

  int bytes_read = 0;
  while (bytes_read < length) {
    int ret = recv(sockfd, output + bytes_read, length - bytes_read, 0);
    if (ret <= 0) {
      break;
    } else {
      bytes_read += ret;
      if (output[bytes_read - 1] == '\0') {
	break;
      }
    }
  }

  release(pool, sockfd);
  return 0;
}

int MPI_Finalize(void) {
  if (!INITIALIZED) {
    return -1;
  }

  char output[8];
  mpi2bsp("MPI_Finalize\n\n", NULL, 0, output, 8);
  delete_connection_pool(pool);
  return 0;
}

int MPI_Comm_size(MPI_Comm comm, int *size) {
  if (!INITIALIZED) {
    return -1;
  }

  char input[64];
  char output[10];
  sprintf(input, "MPI_Comm_size\ncomm=%d\n\n", comm);
  mpi2bsp(input, NULL, 0, output, 10);
  *size = atoi(output);
  return 0;
}

int MPI_Comm_rank(MPI_Comm comm, int *rank) {
  if (!INITIALIZED) {
    return -1;
  }

  char input[64];
  char output[10];
  sprintf(input, "MPI_Comm_rank\ncomm=%d\n\n", comm);
  mpi2bsp(input, NULL, 0, output, 10);
  *rank = atoi(output);
  return 0;
}

int MPI_Send(void* buffer, int count, MPI_Datatype type, int dest, int tag, MPI_Comm comm) {
  if (!INITIALIZED) {
    return -1;
  }

  char input[64];
  char output[8];
  sprintf(input, "MPI_Send\ncount=%d\ndest=%d\ntag=%d\ncomm=%d\ntype=%d\n\n", count, dest, tag, comm, type);
  mpi2bsp(input, buffer, count * mpi_sizeof(type), output, 8);
  return 0; 
}

int MPI_Recv(void* buffer, int count, MPI_Datatype type, int source, int tag, MPI_Comm comm, MPI_Status *status) {
  if (!INITIALIZED) {
    return -1;
  }

  char input[64];
  sprintf(input, "MPI_Recv\nrcount=%d\nsource=%d\ntag=%d\ncomm=%d\ntype=%d\n\n", count, source, tag, comm, type);
  mpi2bsp(input, NULL, 0, buffer, count);
  return 0; 
}

int mpi_sizeof(MPI_Datatype type) {
  switch(type) {
  case MPI_CHAR:
    return sizeof(char);
  case MPI_INT:
    return sizeof(int);
  case MPI_DOUBLE:
    return sizeof(double);
  }
  return 1;
}
