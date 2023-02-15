#!/bin/sh

cd /opt/schemaregistry/
chmod 755  ./bootstrap/bootstrap-storage.sh
./bootstrap/bootstrap-storage.sh create
./bin/registry-server-start.sh /opt/schemaregistry/conf/registry.yaml