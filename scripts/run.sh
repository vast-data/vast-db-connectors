#!/bin/bash
set -eu

cd $1/data

CLI="./cli --catalog vast"
CMD=${2:-$CLI}

cleanup() {
  bin/launcher stop
}
trap cleanup SIGINT SIGTERM EXIT

bin/launcher start

for i in `seq 1 60`; do
    sleep 1
    (curl -s localhost:8080/v1/info | grep '"starting":false') || continue
    tail -n1 var/log/server.log
    $CMD
    exit 0
done
exit 1  # server failed
