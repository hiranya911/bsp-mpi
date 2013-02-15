#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <float.h>
#include <math.h>

#include <sys/time.h>
#include "mpi.h"


const char* dgemm_desc = "Naive MPI";
void square_dgemm( int M, double *A, double *B, double *C, const int &rank, const int &size );

/* Helper functions */

double read_timer( )
{
  static bool initialized = false;
  static struct timeval start;
  struct timeval end;
  if( !initialized )
    {
      gettimeofday( &start, NULL );
      initialized = true;
    }

  gettimeofday( &end, NULL );

  return (end.tv_sec - start.tv_sec) + 1.0e-6 * (end.tv_usec - start.tv_usec);
}

void fill( double *p, int n )
{
  for( int i = 0; i < n; i++ )
    p[i] = 2 * drand48( ) - 1;
}

void absolute_value( double *p, int n )
{
  for( int i = 0; i < n; i++ )
    p[i] = fabs( p[i] );
}

int main( int argc, char *argv[])
{
  int rank, size;
  int isize;
  //int test_sizes[] = {
  //    50, 100, 200, 400, 600, 800, 1000
  //};
  int test_sizes[] = {
        1000
  };
  MPI_Init( &argc, &argv );
  MPI_Comm_size( MPI_COMM_WORLD, &size );
  MPI_Comm_rank( MPI_COMM_WORLD, &rank );
  //printf( "Hello world from process %d of %d\n", rank, size );
  if (rank == 0)
    {
      //root needs to: generate matrices, bcast, reduce
      printf ("Description:\t%s\n\n", dgemm_desc);
    }
  /*For each test size*/
  for( isize = 0; isize < sizeof(test_sizes)/sizeof(test_sizes[0]); isize++ )
    {
      /*Create and fill 3 random matrices A,B,C*/
      int n = test_sizes[isize];
      double val = -3.0*DBL_EPSILON*n;
      double const1 = 1;
      double constn1 = -1;

      double *A = (double*) malloc( n * n * sizeof(double) );
      double *B = (double*) malloc( n * n * sizeof(double) );
      double *C = (double*) malloc( n * n * sizeof(double) );
      double *D = (double*) malloc( n * n * sizeof(double) );

      memset(D, 0, n * n * sizeof(double));

      //root node decides the values and then broadcasts them
      if (rank == 0)
        {
	  fill( A, n * n );
	  fill( B, n * n );
	  fill( C, n * n );
        }


      /*  measure Mflop/s rate; time a sufficiently long sequence of calls to eliminate noise*/
      double Mflop_s, seconds = -1.0;
      char finished = 0;

      if (rank == 0)
        {
	  for( int n_iterations = 1; seconds < 0.1; n_iterations *= 2 ) 
            {
	      //this tells the other nodes to continue;
	      MPI_Bcast(&finished, 1, MPI_CHAR, 0, MPI_COMM_WORLD);
	      /* warm-up */
	      square_dgemm( n, A, B, C, rank, size );

	      /*  measure time */
	      seconds = read_timer( );
	      for( int i = 0; i < n_iterations; i++ )
		square_dgemm( n, A, B, C, rank, size );
	      seconds = read_timer( ) - seconds;

	      /*  compute Mflop/s rate */
	      Mflop_s = 2e-6 * n_iterations * n * n * n / seconds;
            }
	  finished = 1;
	  //tell the worker nodes we are finished
	  MPI_Bcast(&finished, 1, MPI_CHAR, 0, MPI_COMM_WORLD);
        }
      else 
        {
	  char finished = 0;
	  int n_iterations = 1;
	  while(finished == 0) 
            {
	      //worker nodes keep going until root says we finished the last timing round
	      MPI_Bcast(&finished, 1, MPI_CHAR, 0, MPI_COMM_WORLD);
	      if (finished == 0)
                {
		  /* warm-up */
		  square_dgemm( n, A, B, C, rank, size );

		  /*  measure time */
		  for( int i = 0; i < n_iterations; i++ )
		    square_dgemm( n, A, B, C, rank, size );

		  n_iterations *= 2 ;
                }
            }
        }

      //The last run where we actually verify the results
      memset( C, 0, sizeof( double ) * n * n );
      square_dgemm( n, A, B, C, rank, size );
      //all processes do the mul, onlt root verifies+prints the results
      if (rank == 0)
        {
	  for (int i = 0; i < n; i++) {
	    for (int j = 0; j < n; j++) {
	      double cij = D[i+j*n];
	      for( int k = 0; k < n; k++ ) {
		cij += A[i+k*n] * B[k+j*n];
	      }
	      D[i+j*n] = cij;
	    }
	  }

	  printf ("Size: %d\tMflop/s: %g\tSeconds: %g\n", n, Mflop_s, seconds);
	  for (int i = 0; i < n * n; i++) {
	    //printf("%g %g\n", C[i], D[i]);
	    if (C[i] != D[i]) {
	      printf("Detected calculation error\n");
	      //break;
	    }
	  }
	  /*  Ensure that error does not exceed the theoretical error bound */

	  /*Subtract A*B from C using standard dgemm (note that this should be 0 to within machine roundoff)*/
        }

      /*Deallocate memory*/
      free( D );
      free( C );
      free( B );
      free( A );
    }

  MPI_Finalize();
  return 0;
}

//MPI version of naive matrix dgemm (C = C + AB)
void square_dgemm( int n, double *A, double *B, double *C, const int &rank, const int &size )
{
  //n is the width/height, N is the total num of elements
  int N = n * n;
  MPI_Bcast(A, N, MPI_DOUBLE, 0, MPI_COMM_WORLD);
  MPI_Bcast(B, N, MPI_DOUBLE, 0, MPI_COMM_WORLD);

  int start = (N * rank) / size;
  int end = (N * (rank + 1)) / size;
  //each worker does one chunk of the work
  for( int index = start; index < end; index++ )
    {
      int i = index / n;
      int j = index % n;
      double cij = C[i+j*n];
      for( int k = 0; k < n; k++ )
	cij += A[i+k*n] * B[k+j*n];
      C[i+j*n] = cij;
    }


  if (rank == 0) //root does in-place reduce so we don't need another buffer
    MPI_Reduce(MPI_IN_PLACE, C, N, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);
  else
    MPI_Reduce(C, C, N, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);
}
