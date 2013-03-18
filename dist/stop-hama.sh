#!/bin/bash
BASEDIR=$(dirname $0)
HAMA_HOME=$BASEDIR/hama-0.6.0
HADOOP_HOME=$BASEDIR/hadoop-1.1.1
MASTER_NODE=`head -n 1 $PBS_NODEFILE`
if [ "`hostname`" == "$MASTER_NODE.local" ]; then
    echo "this is master shutting down the cluster";
    echo "this is master shutting down the cluster" >&2;
    $HAMA_HOME/bin/stop-bspd.sh
    $HADOOP_HOME/bin/stop-all.sh
    #$HADOOP_HOME/bin/stop-dfs.sh
    #$HADOOP_HOME/bin/stop-mapred.sh
    #$MY_HADOOP_HOME/bin/cleanup.sh -n $NUM_NODES
fi
