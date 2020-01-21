#!/bin/bash -ex

export CLIPASS=cli123
PREFIX=ssl
CA_SUBJ="/CN=Registry-Security-CA"
CA_CERT=${PREFIX}/ca-cert 
CA_KEY=${PREFIX}/ca-key
CLIENT_TRUSTSTORE=${PREFIX}/registry.client.truststore.jks

rm -rf ${PREFIX}
mkdir -p ${PREFIX}
# create ca:
openssl req -new -newkey  rsa:4096 -days 365 -x509 -subj ${CA_SUBJ} -keyout ${CA_KEY} -out ${CA_CERT} -nodes

# create client truststore
keytool -keystore ${CLIENT_TRUSTSTORE} -alias CARoot -import -file ${CA_CERT} -storepass $CLIPASS -keypass $CLIPASS -noprompt
