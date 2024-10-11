#!/bin/bash

docker buildx build -t das-atomdb-cpp-wheeler --load -f docker/Dockerfile.wheel .
