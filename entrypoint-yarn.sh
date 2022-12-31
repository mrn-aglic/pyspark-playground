#!/bin/bash

SPARK_WORKLOAD=$1

echo "SPARK_WORKLOAD: $SPARK_WORKLOAD"

/opt/hadoop/bin/hdfs namenode -format cluster_name -force

/etc/init.d/ssh start

if [ "$SPARK_WORKLOAD" == "master" ];
then
  hdfs --daemon start namenode
  hdfs --daemon start secondarynamenode
  yarn --daemon start resourcemanager

  hdfs dfs -mkdir -p /opt/spark/data
  hdfs dfs -mkdir -p /spark-logs

  hdfs dfs -copyFromLocal /opt/spark/data/* /opt/spark/data
  hdfs dfs -ls /opt/spark/data

  start-history-server.sh

elif [ "$SPARK_WORKLOAD" == "worker" ];
then
  hdfs --daemon start datanode
  yarn --daemon start nodemanager
fi

tail -f /dev/null
