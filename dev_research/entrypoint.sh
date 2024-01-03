#!/bin/bash

# Start PostgreSQL service in the background
service postgresql start

# Wait for PostgreSQL to start
sleep 5

# Tail the PostgreSQL logs to keep the container running
tail -f /var/log/postgresql/postgresql-11-main.log &
PID=$!

# Trap signals to properly stop PostgreSQL when the container is stopped
trap "service postgresql stop && kill $PID" SIGINT SIGTERM

# Wait for the tail process to exit
wait $PID
