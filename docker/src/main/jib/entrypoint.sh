#!/bin/sh

cd /opt/registry/
chmod 755  ./bootstrap/bootstrap-storage.sh
./bootstrap/bootstrap-storage.sh create
./bin/registry-server-start.sh /opt/registry/conf/registry.yaml