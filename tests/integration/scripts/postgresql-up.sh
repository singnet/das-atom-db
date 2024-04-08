#!/bin/bash

if [ -z "$1" ]
then
    echo "Usage: postgresql-up.sh PORT"
    exit 1
else
    PORT=$1
fi

echo "Starting PostgreSQL on port $PORT"

docker stop postgresql_$PORT >& /dev/null
docker rm postgresql_$PORT >& /dev/null
docker volume rm postgresqldbdata_$PORT >& /dev/null

sleep 1
docker run \
        --detach \
        --name postgresql_$PORT \
        --env POSTGRES_USER="dbadmin" \
        --env POSTGRES_PASSWORD="dassecret" \
        --env POSTGRES_DB="postgres" \
        --env TZ=${TZ} \
        --network="host" \
        --volume /tmp:/tmp \
        --volume /mnt:/mnt \
        --volume postgresqldbdata_$PORT:/data/db \
        -p 5432:$PORT \
        postgres:latest \
        postgres -p $PORT
sleep 5