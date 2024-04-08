#!/bin/bash

if [ -z "$1" ]
then
    echo "Usage: postgresql-down.sh PORT"
    exit 1
else
    PORT=$1
fi

echo "Destroying PostgreSQL container on port $PORT"

docker stop postgresql_$PORT && docker rm postgresql_$PORT && docker volume rm postgresqldbdata_$PORT >& /dev/null
