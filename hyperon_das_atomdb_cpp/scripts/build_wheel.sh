#!/bin/bash

CONTAINER_NAME="das-atomdb-cpp-wheeler"

docker run \
    --name=$CONTAINER_NAME \
    --volume .:/hyperon_das_atomdb_cpp \
    --workdir /hyperon_das_atomdb_cpp \
    das-atomdb-cpp-wheeler

sleep 1
docker rm $CONTAINER_NAME