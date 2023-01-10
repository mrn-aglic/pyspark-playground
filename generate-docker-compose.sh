#!/bin/bash

NUM_WORKERS=$1

DOCKER_COMPOSE_NAME="docker-compose.generated.yml"

#NGINX_FILE_NAME="nginx.generated.conf"

port1=8042
port2=9864

echo "" > $DOCKER_COMPOSE_NAME

DOCKER_WORKERS_TMPL=""
#NGINX_FULL_TMPL=""

for((i=1; i <= NUM_WORKERS; i++))
do
  DOCKER_WORKER_TMPL=$(cat templates/yarn-data-node.tmpl)
#  NGINX_TMPL=$(cat templates/nginx.tmpl)

  num=$i
  DOCKER_WORKER_TMPL="${DOCKER_WORKER_TMPL//\{worker_num\}/$num}"
  DOCKER_WORKER_TMPL="${DOCKER_WORKER_TMPL/\{port1\}/$port1}"
  DOCKER_WORKER_TMPL="${DOCKER_WORKER_TMPL/\{port2\}/$port2}"


#  NGINX_TMPL="${NGINX_TMPL/\{num\}/$num}"
#  NGINX_TMPL="${NGINX_TMPL/\{port\}/$port}"

  DOCKER_WORKERS_TMPL="$DOCKER_WORKERS_TMPL\n\n$DOCKER_WORKER_TMPL"

#  if [ "$NGINX_FULL_TMPL" == "" ]
#  then
#    NGINX_FULL_TMPL="$NGINX_TMPL"
#  else
#    NGINX_FULL_TMPL="$NGINX_FULL_TMPL\n\n$NGINX_TMPL"
#  fi

  port1=$((port1 + 1))
  port2=$((port2 + 1))

done

DOCKER_COMPOSE_TMPL=$(cat templates/docker-compose.yarn.tmpl)

DOCKER_COMPOSE_TMPL="${DOCKER_COMPOSE_TMPL/\{nginx_conf\}/$NGINX_FILE_NAME}"
DOCKER_COMPOSE_TMPL="${DOCKER_COMPOSE_TMPL/\{workers\}/$DOCKER_WORKERS_TMPL}"

echo "$DOCKER_COMPOSE_TMPL" > $DOCKER_COMPOSE_NAME

#echo "$NGINX_FULL_TMPL" > $NGINX_FILE_NAME