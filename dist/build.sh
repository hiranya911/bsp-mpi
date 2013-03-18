#!/bin/sh

VERSION=1.0
HAMA_BIN=/Users/hiranya/Projects/bsp-mpi/sandbox/hama-0.6.0.tar
HADOOP_BIN=/Users/hiranya/Projects/bsp-mpi/sandbox/hadoop-1.1.1.tar
TARGET_DIR=mpi2bsp-${VERSION}

rm -rf ${TARGET_DIR}
rm ${TARGET_DIR}.zip

mkdir -v ${TARGET_DIR}

cp -v core-site-raw.xml ${TARGET_DIR}/ 
cp -v hama-site-raw.xml ${TARGET_DIR}/
cp -v install.sh ${TARGET_DIR}/
cp -v mpicc ${TARGET_DIR}/
cp -v mpirun ${TARGET_DIR}/
cp -v start-hama.sh ${TARGET_DIR}/
cp -v stop-hama.sh ${TARGET_DIR}/

mkdir -v ${TARGET_DIR}/mpi
cp -v ../mpi/*.c ${TARGET_DIR}/mpi
cp -v ../mpi/*.h ${TARGET_DIR}/mpi

cd ${TARGET_DIR}
tar xvf $HAMA_BIN
tar xvf $HADOOP_BIN
cd ..

zip -r ${TARGET_DIR}.zip ${TARGET_DIR}
rm -rf ${TARGET_DIR}
