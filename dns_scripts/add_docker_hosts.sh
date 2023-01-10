#!/bin/bash

set -euo pipefail

unset BACKUP NUMBACKUP v

function help() {
    printf "\nUsage: add_docker_hosts.sh -b [true|false]. Optional: -n for number of backups \n\n"
    exit 2
}

while getopts ":o:n:h" v
do
  [[ ${OPTARG} == -* ]] && { echo "Missing argument for -${v}" ; exit 1 ; }
  case $v in
    o) OVERWRITE="$OPTARG";;
    n) NUMBACKUP="$OPTARG";;
    h|\?) help;;
    :) echo "Option -$OPTARG requires an argument" >& 2
      exit 1;;
  esac
done

OVERWRITE=${OVERWRITE:-}

if [ -z "${OVERWRITE}" ]
then
  echo "Overwrite option (-o) not set"
  help
fi

BACKUPS_COUNT=$(find backup -name "hosts*" 2> /dev/null | wc -l | sed -e 's/^[[:space:]]*//')

: "${NUMBACKUP:=3}"

HOSTS_BACKUP_FILE="backup/hosts"

for((i=0;i<NUMBACKUP;i++))
do
  if [ "$BACKUPS_COUNT" = 0 ] && [ "$i" = 0 ]
  then
    cp /etc/hosts "$HOSTS_BACKUP_FILE"
  else
    NUM=$((BACKUPS_COUNT + i))
    cp /etc/hosts "$HOSTS_BACKUP_FILE.$NUM"
  fi
done

mkdir -p backup

HOSTS_CONTENT=$(cat /etc/hosts)

START_LINE="####Added docker dns by script add_docker_hosts.sh"
END_LINE="####End docker added dns names"

LINES=$START_LINE

PYTHON_PROGRAM="import json,sys;js=json.load(sys.stdin)[0];print(js['Config']['Hostname'])"
CMD=""

echo "You need to have python3 installed and the command named either python or python3"
[ "$(which -s python)" ] && CMD=python || CMD=python3

for id in $(docker ps -q);
do
  hostname=$(docker inspect "$id" | $CMD -c "$PYTHON_PROGRAM")

  echo "HOSTNAME: $hostname"

  LINES="$LINES\n127.0.0.1 $hostname"
done

LINES="$LINES\n$END_LINE"

NEW_HOSTS="$HOSTS_CONTENT\n$LINES"

if [ "$OVERWRITE" = "true" ]
then
  echo "The following content will be written to file /etc/hosts:"
  echo "$NEW_HOSTS"

  echo "This will overwrite your /etc/hosts file, do you wish to continue? (enter number)"
  select yn in "Yes" "No"; do
      case $yn in
          Yes ) break;;
          No ) exit;;
      esac
  done

  echo "$NEW_HOSTS" > /etc/hosts
else
  echo "The following content was generated:"
  echo "$NEW_HOSTS"
fi
