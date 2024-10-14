#!/bin/bash

source common.sh

docker run --rm \
    --name=$WHEELER_CONTAINER_NAME \
    --volume .:/hyperon_das_atomdb_cpp \
    --workdir /hyperon_das_atomdb_cpp \
    $WHEELER_CONTAINER_NAME

