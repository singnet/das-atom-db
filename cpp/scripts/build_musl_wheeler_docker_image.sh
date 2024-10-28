#!/bin/bash

source $(dirname $0)/common.sh

docker buildx build -t ${WHEELER_MUSL_CONTAINER_NAME} \
  --load -f ${WHEELER_MUSL_DOCKER_FILE} .

