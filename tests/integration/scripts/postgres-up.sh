#!/bin/bash

if [ -z "$1" ]
then
    echo "Usage: postgres-up.sh PORT"
    exit 1
else
    PORT=$1
fi

echo "Starting Postgres on port $PORT"

docker stop postgres_$PORT >& /dev/null
docker rm postgres_$PORT >& /dev/null
docker volume rm postgresdbdata_$PORT >& /dev/null

sleep 1
docker run \
        --detach \
        --name postgres_$PORT \
        --env POSTGRES_USER="dbadmin" \
        --env POSTGRES_PASSWORD="dassecret" \
        --env POSTGRES_DB="postgres" \
        --env TZ=${TZ} \
        --network="host" \
        --volume /tmp:/tmp \
        --volume /mnt:/mnt \
        --volume postgresdbdata_$PORT:/data/db \
        -p 5432:$PORT \
        postgres:latest \
        postgres -p $PORT
sleep 5