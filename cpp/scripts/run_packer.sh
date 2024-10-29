#!/bin/bash

source $(dirname $0)/common.sh

# Build packages (DEB, RPM, TGZ, etc)
docker run --rm \
    -e _USER=$(id -u) \
    -e _GROUP=$(id -g) \
    -e CMAKE_PROJECT_VERSION=${PROJECT_VERSION} \
    -v $PWD:/hyperon_das_atomdb_cpp \
    --workdir /hyperon_das_atomdb_cpp \
    --name=$PACKER_CONTAINER_NAME \
    $PACKER_CONTAINER_NAME


