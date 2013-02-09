#include "mpi.h"
#include <stdio.h>
#include <unistd.h>
#include <pthread.h>
#include <signal.h>
#include <stdlib.h>

char* get_input_meta_file();
char* get_output_meta_file();
void wait_for_output();
void handle_signal(int signal);

pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t condition;

int INITIALIZED = 0;

int MPI_Init(int *argc, char ***argv) {
  signal(SIGUSR1, handle_signal);
  INITIALIZED = 1;

  FILE* f;
  f = fopen(get_input_meta_file(), "w");
  if (f == NULL) {
    return -1;
  }
  fprintf(f, "MPI_Init\n");
  fprintf(f, "%d\n", getpid());
  fclose(f);
  wait_for_output();
  unlink(get_output_meta_file());
  return 0;
}

int MPI_Finalize(void) {
  if (!INITIALIZED) {
    return -1;
  }

  FILE* f;
  f = fopen(get_input_meta_file(), "w");
  if (f == NULL) {
    return -1;
  }
  fprintf(f, "MPI_Finalize\n");
  fclose(f);
  wait_for_output();
  unlink(get_output_meta_file());
  return 0;
}

int MPI_Comm_size(MPI_Comm comm, int *size) {
  if (!INITIALIZED) {
    return -1;
  }

  FILE* f;
  f = fopen(get_input_meta_file(), "w");
  if (f == NULL) {
    return -1;
  }
  fprintf(f, "MPI_Comm_size\n");
  fclose(f);
  wait_for_output();

  f = fopen(get_output_meta_file(), "r");
  if (f == NULL) {
    return -1;
  }
  fscanf(f, "%d", size);
  fclose(f);
  unlink(get_output_meta_file());

  return 0;
}

int MPI_Comm_rank(MPI_Comm comm, int *rank) {
  if (!INITIALIZED) {
    return -1;
  }

  FILE* f;
  f = fopen(get_input_meta_file(), "w");
  if (f == NULL) {
    return -1;
  }
  fprintf(f, "MPI_Comm_rank\n");
  fprintf(f, "%d\n", comm);
  fclose(f);
  wait_for_output();

  f = fopen(get_output_meta_file(), "r");
  if (f == NULL) {
    return -1;
  }
  fscanf(f, "%d", rank);
  fclose(f);
  unlink(get_output_meta_file());
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
  pthread_mutex_lock(&lock);
  pthread_cond_init(&condition, NULL);
  int parent_pid = atoi(getenv("bsp.mpi.parent"));
  kill(parent_pid, SIGUSR1);
  pthread_cond_wait(&condition, &lock);
  pthread_cond_destroy(&condition);
  pthread_mutex_unlock(&lock);
}

void handle_signal(int signal) {
  if (signal == SIGUSR1) {
    pthread_mutex_lock(&lock);
    pthread_cond_signal(&condition);
    pthread_mutex_unlock(&lock);
  }
}
