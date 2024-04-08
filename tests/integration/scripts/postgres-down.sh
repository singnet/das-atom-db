#!/bin/bash

if [ -z "$1" ]
then
    echo "Usage: postgres-down.sh PORT"
    exit 1
else
    PORT=$1
fi

echo "Destroying Postgres container on port $PORT"

docker stop postgres_$PORT && docker rm postgres_$PORT && docker volume rm postgresdbdata_$PORT >& /dev/null
