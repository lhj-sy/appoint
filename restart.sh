#!/bin/bash
source config/env

echo "kill old process ..."
kill -QUIT `cat run/msgevent.pid`

echo "sleep 5 second ..."
sleep 5

echo "start new process ..."
nohup ./msgevent -c config/queues.yml -log log/msgevent.log -pidfile run/msgevent.pid &

exit 0
