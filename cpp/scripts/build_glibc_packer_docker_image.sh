#!/bin/bash -x

source $(dirname $0)/common.sh

# Build the base image
BASE_IMAGE_NAME="${PACKER_GLIBC_CONTAINER_NAME}-base:latest"
docker buildx build -t ${BASE_IMAGE_NAME} \
  --load -f ${PACKER_GLIBC_DOCKER_FILE} .

docker buildx build -t ${PACKER_GLIBC_CONTAINER_NAME} \
  --build-arg BASE_IMAGE=${BASE_IMAGE_NAME} \
  --load -f ${PACKER_COMMON_DOCKER_FILE} .

