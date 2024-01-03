#!/bin/bash

SPARK_WORKLOAD=$1

echo "SPARK_WORKLOAD: $SPARK_WORKLOAD"

/etc/init.d/ssh start

if [ "$SPARK_WORKLOAD" == "master" ];
then
  hdfs namenode -format

  # start the master node processes
  hdfs --daemon start namenode
  hdfs --daemon start secondarynamenode
  yarn --daemon start resourcemanager

  # create required directories
  while ! hdfs dfs -mkdir -p /spark-logs;
  do
    echo "Failed creating /spark-logs hdfs dir"
  done
  echo "Created /spark-logs hdfs dir"
  hdfs dfs -mkdir -p /opt/spark/data
  echo "Created /opt/spark/data hdfs dir"

  # copy the data to the data HDFS directory
  hdfs dfs -copyFromLocal /opt/spark/data/* /opt/spark/data
  hdfs dfs -ls /opt/spark/data

  zeppelin-daemon.sh start

  # Start PostgreSQL service in the background
  service postgresql start

  # For HIVE
  hdfs dfs -mkdir /hive /hive/warehouse
  hdfs dfs -chmod -R 775 /hive
  # hdfs dfs -chown -R hive:hadoop /hive

  # Create Hive schema (PostgreSQL)
  ${HIVE_HOME}/bin/schematool -initSchema -dbType postgres

elif [ "$SPARK_WORKLOAD" == "worker" ];
then
  hdfs namenode -format

  # start the worker node processes
  hdfs --daemon start datanode
  yarn --daemon start nodemanager
elif [ "$SPARK_WORKLOAD" == "history" ];
then

  while ! hdfs dfs -test -d /spark-logs;
  do
    echo "spark-logs doesn't exist yet... retrying"
    sleep 1;
  done
  echo "Exit loop"

  # start the spark history server
  start-history-server.sh
fi

tail -f /dev/null

