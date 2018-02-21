#!/bin/bash

set -e

kstop() {
    ZK_PID=`jps -l | grep zookeeper | awk '{print $1}'`
    AF_PID=`jps -l | grep kafka | awk '{print $1}'`

    if [[ -n "$AF_PID" ]]; then
        echo "Killing Apache Kafka with PID: ${AF_PID}"
        kill -9 "${AF_PID}"
    else
        echo "Kafka service not running!!"
    fi

    if [[ -n "$ZK_PID" ]]; then
        echo "Killing Apache Zookeeper with PID: ${ZK_PID}"
        kill -9 "${ZK_PID}"
    else
        echo "Zookeeper service not running!!"
    fi

    echo "Removing the nohup, logs and data"
    rm -rvf nohup.out bin/nohup.out logs /tmp/zookeeper /tmp/kafka-logs
}


kstart() {
    echo "Starting Apache Zookeeper"
    cd /opt/kafka/bin
    JMX_PORT=9990 nohup sh zookeeper-server-start.sh ../config/zookeeper.properties &
    SLEEP_SECS=3
    echo "Sleeping for ${SLEEP_SECS} seconds" 
    sleep "${SLEEP_SECS}"

    cd /opt/kafka
    echo "Starting Apache Kafka"
    JMX_PORT=9991 nohup sh bin/kafka-server-start.sh config/server.properties &

    echo "Successfully started Apache Zookeeper and Kafka"
}

host_name=`hostname -f`
find /opt/kafka/config/ -type f -exec sed -i "s/TODO_HOSTNAME/$host_name/g" {} \;

# Set KAFKA specific environment variables here.

# for kerberos
export KAFKA_KERBEROS_PARAMS=-Djava.security.auth.login.config=/opt/kafka/config/kafka_jaas.conf
export KRB_DEBUG_PARAMS=-Dsun.security.krb5.debug=false

# apache kafka does not have KAFKA_KERBEROS_PARAMS so use KAFKA_HEAP_OPTS
export KAFKA_HEAP_OPTS="-Xmx1G -Xms1G"
export KAFKA_HEAP_OPTS="$KAFKA_HEAP_OPTS $KAFKA_KERBEROS_PARAMS $KRB_DEBUG_PARAMS"
export KAFKA_CLIENT_KERBEROS_PARAMS=-Djava.security.auth.login.config=/opt/kafka/config/kafka_client_jaas.conf

option=$1
case "${option}" in 
    start)  kstart;;
    stop)   kstop;;
    restart) kstop && kstart;;
    *)  echo "Unknown option. Valid options are start / stop / restart";;
esac

exit 0;
