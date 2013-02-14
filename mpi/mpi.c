#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>

#include "mpi.h"
#include "cpool.h"

#define TRUE 1
#define FALSE 0

int mpi2bsp(char* input, void* input_data, int in_length, void* output, int out_length, int binary);

struct connection_info {
  char* host;
  int port;
};

int INITIALIZED = FALSE;
int MPI_RANK = -1;
int MPI_SIZE = -1;
int LOCAL_PORT = -1;
struct connection_pool* POOL;
struct connection_info* CONNECTION_INFO = NULL;

int MPI_Init(int *argc, char ***argv) {
  if (INITIALIZED) {
    return -1;
  }

  INITIALIZED = TRUE;
  char* port = /*getenv("bsp.mpi.port")*/ "51298";
  LOCAL_PORT = atoi(port);
  POOL = new_connection_pool();

  char output[16384];
  int length = mpi2bsp("MPI_Init\n\n", NULL, 0, output, 8, FALSE);
  char* token = strtok(output, "\n");
  while (token != NULL) {
    int token_length = strlen(token);
    if (MPI_RANK == -1) {
      MPI_RANK = atoi(token);
    } else if (MPI_SIZE == -1) {
      MPI_SIZE = atoi(token);
      CONNECTION_INFO = malloc(sizeof(struct connection_info) * MPI_SIZE);
    } else {
      char* rank = token;
      char* host = strchr(token, ':') + 1;
      char* port = strchr(host, ':') + 1;
      *(host - 1) = '\0';
      *(port - 1) = '\0';
      struct connection_info* info = CONNECTION_INFO + atoi(rank);
      info->host = malloc(strlen(host) + 1);
      strcpy(info->host, host);
      info->port = atoi(port);
    }
    token = strtok(NULL, "\n");
  }
  return 0;
}

int mpi2bsp(char* input, void* input_data, int in_length, void* output, int out_length, int binary) {
  int sockfd = get_connection(POOL, "localhost", LOCAL_PORT);
  if (sockfd < 0) {
    printf("Failed to open a socket to the parent process\n");
    exit(1);
  }
  send(sockfd, input, strlen(input), 0);
  if (in_length > 0) {
    send(sockfd, input_data, in_length, 0);
  }

  int bytes_read = 0;
  if (binary) {
    char temp[4];
    while (bytes_read < 4) {
      int ret = recv(sockfd, &temp[bytes_read], 4 - bytes_read, 0);
      if (ret <= 0) {
	exit(1);
      } else {
	bytes_read += ret;
      }
    }

    int* actual_length = (int*) &temp;
    bytes_read = 0;
    while (bytes_read < *actual_length) {
      int ret = recv(sockfd, output + bytes_read, *actual_length - bytes_read, 0);
      if (ret <= 0) {
	exit(1);
      } else {
	bytes_read += ret;
      }
    }

  } else {
    while (bytes_read < out_length) {
      int ret = recv(sockfd, output + bytes_read, out_length - bytes_read, 0);
      if (ret <= 0) {
	exit(1);
      } else {
	bytes_read += ret;
	char* char_output = (char*) output;
	if (char_output[bytes_read - 1] == 0) {
	  break;
	}
      }
    }
    
  }

  release_connection(POOL, "localhost", LOCAL_PORT, sockfd);
  return bytes_read;
}

int MPI_Finalize(void) {
  if (!INITIALIZED) {
    return -1;
  }

  int i;
  for (i = 0; i < MPI_SIZE; i++) {
    if (i != MPI_RANK) {
      free((CONNECTION_INFO + i)->host);
    }
  }
  free(CONNECTION_INFO);

  char output[8];
  mpi2bsp("MPI_Finalize\n\n", NULL, 0, output, 8, FALSE);
  delete_connection_pool(POOL);
  return 0;
}

int MPI_Comm_size(MPI_Comm comm, int *size) {
  if (!INITIALIZED) {
    return -1;
  }

  *size = MPI_SIZE;
  return 0;
}

int MPI_Comm_rank(MPI_Comm comm, int *rank) {
  if (!INITIALIZED) {
    return -1;
  }

  *rank = MPI_RANK;
  return 0;
}

int MPI_Send(void* buffer, int count, MPI_Datatype type, int dest, int tag, MPI_Comm comm) {
  if (!INITIALIZED) {
    return -1;
  }

  char input[64];
  int size = count * mpi_sizeof(type);
  if (dest >= MPI_SIZE || dest == MPI_RANK) {
    printf("Illegal destination ID: %d\n", dest);
    exit(1);
  }

  struct connection_info* info = CONNECTION_INFO + dest;
  int sockfd = get_connection(POOL, info->host, info->port);
  if (sockfd < 0) {
    printf("Failed to open a socket to remote process\n");
    exit(1);
  }

  sprintf(input, "MPI_Send\ncount=%d\nsource=%d\ndest=%d\ntag=%d\ncomm=%d\ntype=%d\n\n", size, MPI_RANK, dest, tag, comm, type);
  send(sockfd, input, strlen(input), 0);
  send(sockfd, buffer, size, 0);
  release_connection(POOL, info->host, info->port, sockfd);
  return 0; 
}

int MPI_Recv(void* buffer, int count, MPI_Datatype type, int source, int tag, MPI_Comm comm, MPI_Status *status) {
  if (!INITIALIZED) {
    return -1;
  }

  char input[64];
  int mpi_size = mpi_sizeof(type);
  int size = count * mpi_size;
  sprintf(input, "MPI_Recv\nrcount=%d\nsource=%d\ntag=%d\ncomm=%d\ntype=%d\n\n", size, source, tag, comm, type);
  int bytes_read = mpi2bsp(input, NULL, 0, buffer, size, TRUE);
  if (status != NULL && status != MPI_STATUS_IGNORE) {
    status->count = bytes_read/mpi_size;
    status->MPI_SOURCE = source;
    status->MPI_TAG = tag;
  }
  return 0; 
}

int MPI_Bcast(void* buffer, int count, MPI_Datatype type, int source, MPI_Comm comm) {
  if (!INITIALIZED) {
    return -1;
  }

  char input[64];
  char output[8];
  int size = count * mpi_sizeof(type);  
  if (MPI_RANK == source) {
    sprintf(input, "MPI_Bcast\ncount=%d\nsource=%d\ncomm=%d\ntype=%d\n\n", size, source, comm, type);
    mpi2bsp(input, buffer, size, output, 8, FALSE);
  } else {
    sprintf(input, "MPI_Bcast\nrcount=%d\nsource=%d\ncomm=%d\ntype=%d\n\n", size, source, comm, type);
    int mpi_size = mpi_sizeof(type);
    int size = count * mpi_size;
    mpi2bsp(input, NULL, 0, buffer, size, TRUE);
  }
  return 0;
}

int mpi_sizeof(MPI_Datatype type) {
  switch(type) {
  case MPI_CHAR:
  case MPI_UNSIGNED_CHAR:
    return sizeof(char);
  case MPI_INT:
  case MPI_UNSIGNED:
    return sizeof(int);
  case MPI_DOUBLE:
    return sizeof(double);
  case MPI_LONG:
  case MPI_UNSIGNED_LONG:
    return sizeof(long);
  case MPI_SHORT:
  case MPI_UNSIGNED_SHORT:
    return sizeof(short);
  case MPI_FLOAT:
    return sizeof(float);
  case MPI_LONG_DOUBLE:
    return sizeof(long double);
  case MPI_LONG_LONG_INT:
    return sizeof(long long int);
  }
  exit(1);
  return -1;
}
