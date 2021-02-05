#!/bin/bash

# reference: https://docs.docker.com/config/containers/multi-service_container/

python /catasyn/entrypoints/scheduler.py -D
status=$?
if [ $status -ne 0 ]; then
  echo "Failed to start scheduler: $status"
  exit $status
fi

python /catasyn/entrypoints/flask_app.py -D
status=$?
if [ $status -ne 0 ]; then
  echo "Failed to start scheduler: $status"
  exit $status
fi

while sleep 60; do
  ps aux |grep scheduler |grep -q -v grep
  PROCESS_1_STATUS=$?
  ps aux |grep flask_app |grep -q -v grep
  PROCESS_2_STATUS=$?
  # If the greps above find anything, they exit with 0 status
  # If they are not both 0, then something is wrong
  if [ $PROCESS_1_STATUS -ne 0 -o $PROCESS_2_STATUS -ne 0 ]; then
    echo "One of the processes has already exited."
    exit 1
  fi
done