#!/bin/bash

export TZ="US/Pacific"; date

kafka_connection="`nc  -v -w 1 -i 1 18.236.157.126 22 2>&1 | grep Connected`"
if [ "$kafka_connection" == "" ]; then
  pkill -9 java 
  pkill -9 python
  echo "Killed servers"
else
  echo "kafka connection OK"
fi

zookeeper_running="`ps -ef | grep java | grep zookeeper.properties | tr -s ' ' | cut -f3 -d' ' | xargs`"
kafka_running="`ps -ef | grep java | grep server.properties | tr -s ' ' | cut -f3 -d' ' | xargs`"


cd ${HOME}/kafka_*-[0-9].[0-9].[0-9]
if [ "$zookeeper_running" == "" ]; then
  KAFKA_HEAP_OPTS="-Xmx32M"   bin/zookeeper-server-start.sh config/zookeeper.properties >   /tmp/zookeeper.log 2>&1 &
  echo "Started zookeeper"
  sleep 5 
else
  echo "zookeeper already running"
fi

if [ "$kafka_running" == "" ]; then
  KAFKA_HEAP_OPTS="-Xmx200M"   bin/kafka-server-start.sh config/server.properties >   /tmp/kafka.log 2>&1 &
  echo "Started kafka"
else
  echo "kafka already running"
fi


