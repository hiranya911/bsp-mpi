#!/bin/bash

BASEDIR=$(dirname $0)
HAMA_HOME=$BASEDIR/hama-0.6.0
HADOOP_HOME=$BASEDIR/hadoop-1.1.1

rm -rf /tmp/hadoop-$USER/dfs/name
mkdir -p /tmp/hadoop-$USER/dfs/name

#copy the nodefile to the list of groomservers
cp $PBS_NODEFILE $HAMA_HOME/conf/groomservers
#replace $MASTER with the master node (1st node in the list)
MASTER_NODE=`head -n 1 $PBS_NODEFILE`
masterreplace="s/\$MASTER_NODE/$MASTER_NODE/"
#fill in the list of zookeeper nodes (i.e. all the nodes)
ZOOKEEPER_QUORUM="`awk '{if (line > 0) printf(","); else line++; printf("%s"i,$0);}' $PBS_NODEFILE`"
zookeeperreplace="s/\$ZOOKEEPER_QUORUM/$ZOOKEEPER_QUORUM/"
cat $BASEDIR/core-site-raw.xml | sed "$masterreplace" > $HADOOP_HOME/conf/core-site.xml
cat $BASEDIR/hama-site-raw.xml | sed "$masterreplace" | sed "$zookeeperreplace" > $HAMA_HOME/conf/hama-site.xml

if [ "`hostname`" == "$MASTER_NODE.local" ]; then
    $HADOOP_HOME/bin/hadoop namenode -format
    $HADOOP_HOME/bin/start-all.sh
    $HAMA_HOME/bin/start-bspd.sh
    echo "Hadoop Cluster Started"
    echo "Hadoop Cluster Started" >&2
fi
