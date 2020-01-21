#!/bin/bash -x

kadmin -p 'admin/admin' -w Hortonworks -q "addprinc -randkey ${2}"
kadmin -p 'admin/admin' -w Hortonworks -q "ktadd -norandkey -k /tmp/registry.service.keytab ${2}"

cd /opt/registry/
chmod 755  ./bootstrap/bootstrap-storage.sh
./bootstrap/bootstrap-storage.sh create
./bin/registry-server-start.sh ${1}

