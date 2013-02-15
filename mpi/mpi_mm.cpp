/////////////////////////////////////////////////////////////////////////////
//                    Matrix Multiplication with MPI                       //
//                         CS240A Assignment 1                             //
//                                                                         //
// Author: Hiranya Jayathilaka (hiranya@cs.ucsb.edu)                       //
/////////////////////////////////////////////////////////////////////////////

#include <iostream>
#include <cstdlib>
#include <ctime>
#include <sys/time.h>

#include "mpi.h"

#define FROM_MASTER  100
#define FROM_WORKER  200

#define N 1000

using namespace std;

void do_worker();
void check_results(double* C, double* D);
void print(double* matrix, int n);

int main(int argc, char **argv) {
  int rank, process_count;

  MPI_Init(&argc, &argv);
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &process_count);

  if (rank == 0) {
    // I'm the master
    double* A = new double[N * N];
    double* B = new double[N * N];
    double* C = new double[N * N];
    double* D = new double[N * N];
    MPI_Status status;
    int rows, offset;
    int worker_count = process_count - 1;

    cout << "Worker count: " << worker_count << endl;

    // Initialize input matrices A and B
    for (int i = 0; i < N; i++) {
      for (int j = 0; j < N; j++) {
	A[i*N + j] = 2 * drand48( ) - 1;
	B[i*N + j] = 2 * drand48( ) - 1;
      }
    }

    // Run the sequential algorithm first (Reference)
    clock_t start = clock();
    for (int i = 0; i < N; i++) {
      for (int j = 0; j < N; j++) {
	double cij = 0.0;
	for (int k = 0; k < N; k++) {
	  cij += A[i*N + k] * B[k*N + j];
	}
	C[i*N + j] = cij;
      }
    }
    clock_t end = clock();

    double time = (end - start)/(double) CLOCKS_PER_SEC;
    double mflops = (2e-6 * N * N * N) / time;
    cout << endl;
    cout << "Serial Execution" << endl;
    cout << "================" << endl;
    cout << "Time Elapsed: " << time << " seconds" << endl;
    cout << "Mflop/s: " << mflops << endl;

    int rows_per_worker = N / worker_count;
    int extra_rows = N % worker_count;
    offset = 0;

    // Run the MPI based parallel algorithm
    struct timeval start1, end1;
    gettimeofday(&start1, NULL);
    for (int worker = 1; worker <= worker_count; worker++) {
      // Try to equally divide the available rows among workers
      rows = (worker <= extra_rows) ? rows_per_worker + 1 : rows_per_worker; 
      MPI_Send(&rows, 1, MPI_INT, worker, FROM_MASTER, MPI_COMM_WORLD);
      MPI_Send(&offset, 1, MPI_INT, worker, FROM_MASTER, MPI_COMM_WORLD);
      MPI_Send(A + offset * N, rows * N, MPI_DOUBLE, worker, FROM_MASTER, MPI_COMM_WORLD);
      MPI_Send(B, N * N, MPI_DOUBLE, worker, FROM_MASTER, MPI_COMM_WORLD);
      offset += rows;
    }
    
    for (int worker = 1; worker <= worker_count; worker++) {
      // Retrieve the results from workers
      MPI_Recv(&offset, 1, MPI_INT, worker, FROM_WORKER, MPI_COMM_WORLD, &status);
      MPI_Recv(&rows, 1, MPI_INT, worker, FROM_WORKER, MPI_COMM_WORLD, &status);
      MPI_Recv(D + offset * N, rows * N, MPI_DOUBLE, worker, FROM_WORKER, MPI_COMM_WORLD, &status);
    }
    gettimeofday(&end1, NULL);

    time = (end1.tv_sec - start1.tv_sec) + (end1.tv_usec - start1.tv_usec)/1000000;
    mflops = (2e-6 * N * N * N) / time;
    cout << endl;
    cout << "Parallel Execution" << endl;
    cout << "==================" << endl;
    cout << "Time Elapsed: " << time << " seconds" << endl;
    cout << "Mflop/s: " << mflops << endl;

    // Compare with the results from naive execution to ensure accurancy
    check_results(C, D);
    delete [] A;
    delete [] B;
    delete [] C;
    delete [] D;
  } else {
    // I'm the worker
    do_worker();
  }

  MPI_Finalize();
  return 0;
}

void check_results(double* C, double* D) {
  cout << endl << "Verifying the multiplication results..." << endl;
  for (int i = 0; i < N; i++) {
    for (int j = 0; j < N; j++) {
      double diff = C[i*N + j] - D[i*N + j];
      if (diff < 0) {
	diff = diff * -1;
      }
      if (diff > 0.0001) {
	cout << "Multiplication error detected" << endl;
	cout << C[i*N+j] << ", " << D[i*N+j] << endl;
	exit(1);
      }
    }
  }
  cout << "All good..." << endl;
}

/*
 * This functon should be only called by worker nodes
 */
void do_worker() {
  int rows, offset;
  MPI_Status status;
  double* A = new double[N * N];
  double* B = new double[N * N];
  double* C = new double[N * N];

  MPI_Recv(&rows, 1, MPI_INT, 0, FROM_MASTER, MPI_COMM_WORLD, &status);
  MPI_Recv(&offset, 1, MPI_INT, 0, FROM_MASTER, MPI_COMM_WORLD, &status);
  MPI_Recv(A, rows * N, MPI_DOUBLE, 0, FROM_MASTER, MPI_COMM_WORLD, &status);
  MPI_Recv(B, N * N, MPI_DOUBLE, 0, FROM_MASTER, MPI_COMM_WORLD, &status);
  
  // Multiply the set of rows assigned to me
  for (int i = 0; i < rows; i++) {
    for (int j = 0; j < N; j++) {
      double cij = 0.0;
      for (int k = 0; k < N; k++) {
	cij += A[i*N + k] * B[k*N + j];
      }
      C[i*N + j] = cij;
    }
  }
  
  MPI_Send(&offset, 1, MPI_INT, 0, FROM_WORKER, MPI_COMM_WORLD);
  MPI_Send(&rows, 1, MPI_INT, 0, FROM_WORKER, MPI_COMM_WORLD);
  MPI_Send(C, rows * N, MPI_DOUBLE, 0, FROM_WORKER, MPI_COMM_WORLD);

  delete [] A;
  delete [] B;
  delete [] C;
}

void print(double* matrix, int n) {
  for (int i = 0; i < n; i++) {
    for (int j = 0; j < n; j++) {
      cout << matrix[i*n + j] << " ";
    }
    cout << endl;
  }
  cout << endl << endl;
}
