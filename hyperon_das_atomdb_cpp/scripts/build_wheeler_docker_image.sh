#!/bin/bash

source common.sh

docker buildx build -t $WHEELER_CONTAINER_NAME --load -f $WHEELER_DOCKER_FILE .

