#!/bin/bash
BASEDIR=$(dirname $0)
echo $BASEDIR

rm -rf $BASEDIR/lib
mkdir $BASEDIR/lib
gcc -c $BASEDIR/mpi/cpool.c -o $BASEDIR/lib/cpool
gcc -c $BASEDIR/mpi/mpi.c -o $BASEDIR/lib/mpi

echo "Installation complete"

