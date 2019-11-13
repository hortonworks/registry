#!/bin/bash

if [[ "${IS_SECURED}" == "yes" ]]; then
    find /opt/kafka/config/ -type f -exec sed -i "s@TODO_LISTENERS@SASL_PLAINTEXT://$(hostname -f):9092@g" {} \; \
                                    -exec sed -i "s@TODO_INTER_BROKER_PROTOCOL@SASL_PLAINTEXT@g" {} \;
else
    find /opt/kafka/config/ -type f -exec sed -i "s@TODO_LISTENERS@PLAINTEXT://$(hostname -f):9092@g" {} \; \
                                    -exec sed -i "s@TODO_INTER_BROKER_PROTOCOL@PLAINTEXT@g" {} \;
fi

find /opt/kafka/config/ -type f -exec sed -i "s/TODO_BROKER_ID/${BROKER_ID}/g" {} \; \
                                -exec sed -i "s/TODO_ZK_CONNECT/${ZK_CONNECT}/g" {} \; \
                                -exec sed -i "s/TODO_HOSTNAME/$(hostname -f)/g" {} \;

JMX_PORT=9991 nohup sh bin/kafka-server-start.sh config/server.properties &