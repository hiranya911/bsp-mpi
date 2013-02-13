#!/bin/sh
gcc -c cpool.c
gcc -c mpi.c
gcc test.c cpool.o mpi.o