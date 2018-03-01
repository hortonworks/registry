#!/bin/bash

find /opt/kafka/config/ -type f -exec sed -i "s/TODO_BROKER_ID/${BROKER_ID}/g" {} \;
find /opt/kafka/config/ -type f -exec sed -i "s/TODO_HOSTNAME/$(hostname -f)/g" {} \;
find /opt/kafka/config/ -type f -exec sed -i "s/TODO_ZK_CONNECT/${ZK_CONNECT}/g" {} \;

JMX_PORT=9991 nohup sh bin/kafka-server-start.sh config/server.properties &
