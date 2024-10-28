#!/bin/bash

source $(dirname $0)/common.sh

# Build the wheel for GLIBC
docker run --rm \
    -e _USER=$(id -u) \
    -e _GROUP=$(id -g) \
    -v $PWD:/hyperon_das_atomdb_cpp \
    --workdir /hyperon_das_atomdb_cpp \
    --name=$WHEELER_GLIBC_CONTAINER_NAME \
    $WHEELER_GLIBC_CONTAINER_NAME

