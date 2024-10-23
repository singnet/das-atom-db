#!/bin/bash

source ${PWD}/scripts/common.sh

docker buildx build -t ${WHEELER_GLIBC_CONTAINER_NAME} \
  --load -f ${WHEELER_GLIBC_DOCKER_FILE} .

