#!/bin/bash

source ${PWD}/scripts/common.sh

docker run --rm \
    -e _USER=$(id -u) \
    -e _GROUP=$(id -g) \
    -v $PWD:/hyperon_das_atomdb_cpp \
    --workdir /hyperon_das_atomdb_cpp \
    --name=$WHEELER_CONTAINER_NAME \
    $WHEELER_CONTAINER_NAME

