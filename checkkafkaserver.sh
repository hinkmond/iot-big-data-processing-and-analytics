#!/bin/bash

export TZ="US/Pacific"; date

kafka_connection="`nc  -v -w 1 -i 1 35.86.119.218 9092 2>&1 | grep Connected`"
if [ "$kafka_connection" == "" ]; then
  pkill -9 java 
  pkill -9 python
  echo "Killed servers"
else
  echo "kafka connection OK"
fi

kafka_processing="`cd ~/kafka_[0-9].*[0-9]/; bin/kafka-console-consumer.sh --timeout-ms 2500 --bootstrap-server 35.86.119.218:9092 --topic iotmsgs-amy --from-beginning 2>&1 | grep -v ERROR | grep -v Timeout | grep Processed |  tr -s ' ' | cut -f5 -d' '`"
if [ "$kafka_processing" == "0" ]; then
  kafka_pid="`ps -ef | grep java | grep 'server.properties' | tr -s ' ' | tail -1 | cut -f2 -d' '`"
  if [ "$kafka_pid" != "" ]; then
    kill -9 $kafka_pid
    echo "Killed kafka processing PID: $kafka_pid"
  else
    echo "No kafka processing due to no kafka running"
  fi
else
  echo "kafka processing OK"
fi

zookeeper_running="`ps -ef | grep java | grep zookeeper.properties | tr -s ' ' | cut -f3 -d' ' | xargs`"
kafka_running="`ps -ef | grep java | grep server.properties | tr -s ' ' | cut -f3 -d' ' | xargs`"


cd ${HOME}/kafka_*-[0-9].[0-9].[0-9]
if [ "$zookeeper_running" == "" ]; then
  KAFKA_HEAP_OPTS="-Xmx32M"   bin/zookeeper-server-start.sh config/zookeeper.properties >   /tmp/zookeeper.log 2>&1 &
  echo "Started zookeeper"
  sleep 7 
else
  echo "zookeeper already running"
fi

if [ "$kafka_running" == "" ]; then
  KAFKA_HEAP_OPTS="-Xmx200M"   bin/kafka-server-start.sh config/server.properties >   /tmp/kafka.log 2>&1 &
  echo "Started kafka"
else
  echo "kafka already running"
fi

