#!/bin/bash
discovery_uri=$1
if [[ -n "$2" ]]; then
  node_id=$2
else
  node_id=${HOSTNAME}
fi

python ${TRINO_HOME}scripts/render.py \
  --node-id $node_id \
  --discovery-uri $discovery_uri \
  etc/node.properties.template \
  etc/config.properties.template

${TRINO_HOME}bin/launcher run

# ./trino.sh http://coordinator:8080 coordinator

# tail -f /dev/null