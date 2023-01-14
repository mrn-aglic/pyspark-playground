#!/bin/bash

NUM_WORKERS=$1

DOCKER_COMPOSE_NAME="docker-compose.generated.yml"

HDFS_SITE_TEMPLATE=$(cat templates/hdfs-site.xml.tmpl)
YARN_SITE_TEMPLATE=$(cat templates/yarn-site.xml.tmpl)

mkdir -p yarn-generated
HDFS_FILE_TARGET_DIR="yarn-generated"

port1=8042
port2=9861

echo "" > $DOCKER_COMPOSE_NAME

DOCKER_WORKERS_TMPL=""

for((i=1; i <= NUM_WORKERS; i++))
do
  DOCKER_WORKER_TMPL=$(cat templates/yarn-data-node.tmpl)

  num=$i
  HDFS_SITE_FILE_NAME="hdfs-site.$num.xml"
  YARN_SITE_FILE_NAME="yarn-site.$num.xml"

  DOCKER_WORKER_TMPL="${DOCKER_WORKER_TMPL//\{worker_num\}/$num}"
  DOCKER_WORKER_TMPL="${DOCKER_WORKER_TMPL//\{port1\}/$port1}"
  DOCKER_WORKER_TMPL="${DOCKER_WORKER_TMPL//\{port2\}/$port2}"
  DOCKER_WORKER_TMPL="${DOCKER_WORKER_TMPL/\{hdfs-site-file\}/$HDFS_SITE_FILE_NAME}"
  DOCKER_WORKER_TMPL="${DOCKER_WORKER_TMPL/\{yarn-site-file\}/$YARN_SITE_FILE_NAME}"

  YARN_FILE_CONTENT="${YARN_SITE_TEMPLATE/\{port\}/$port1}"
  echo "$YARN_FILE_CONTENT" > "$HDFS_FILE_TARGET_DIR/$YARN_SITE_FILE_NAME"

  HDFS_FILE_CONTENT="${HDFS_SITE_TEMPLATE/\{port\}/$port2}"
  echo "$HDFS_FILE_CONTENT" > "$HDFS_FILE_TARGET_DIR/$HDFS_SITE_FILE_NAME"

  DOCKER_WORKERS_TMPL="$DOCKER_WORKERS_TMPL\n\n$DOCKER_WORKER_TMPL"

  port1=$((port1 + 1))
  port2=$((port2 + 1))

done

DOCKER_COMPOSE_TMPL=$(cat templates/docker-compose.yarn.tmpl)

DOCKER_COMPOSE_TMPL="${DOCKER_COMPOSE_TMPL/\{nginx_conf\}/$NGINX_FILE_NAME}"
DOCKER_COMPOSE_TMPL="${DOCKER_COMPOSE_TMPL/\{workers\}/$DOCKER_WORKERS_TMPL}"

echo "$DOCKER_COMPOSE_TMPL" > $DOCKER_COMPOSE_NAME
