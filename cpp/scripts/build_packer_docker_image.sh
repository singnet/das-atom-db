#!/bin/bash

source $(dirname $0)/common.sh

docker buildx build -t ${PACKER_CONTAINER_NAME} \
  --load -f ${PACKER_DOCKER_FILE} .

