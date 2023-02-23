#!/bin/bash

# replace placeholders in krb5.conf
sed -i "s/<<HOSTNAME>>/$(hostname -f)/g" /etc/krb5.conf
sed -i "s/<<REALM>>/$REALM/g" /etc/krb5.conf

# replace kdc.conf realm
sed -i "s/EXAMPLE.COM/$REALM/g" /var/kerberos/krb5kdc/kdc.conf

# create kerberos database
kdb5_util create -s -P databasekey

# servies are daemonized
/usr/sbin/kadmind -P /var/run/kadmind.pid
/usr/sbin/krb5kdc -P /var/run/krb5kdc.pid

# create the admin principal
kadmin.local -q "addprinc -pw Hortonworks admin/admin"

# create keytabs we need
mkdir /root/keytabs
principals[0]="bob"
principals[1]="alice"

for principal in "${principals[@]}";  do
  kadmin.local -q "addprinc -randkey $principal@$REALM"
  kadmin.local -q "xst -norandkey -k /root/keytabs/$principal.headless.keytab $principal"
done

function kill_process() {
  echo "killing child process '$(ps -p $1 -o comm=)' with pid: '$1'"
  kill -s SIGTERM $1
}

function signal_handler() {
  echo -e "\n\nReceived signal: $(kill -l $?). Executing signal handler";
  kill_process $(cat /var/run/kadmind.pid)
  kill_process $(cat /var/run/krb5kdc.pid)
  echo "Exiting main process. Good bye!"
  exit
}

trap 'signal_handler' SIGINT SIGTERM

while true; do
  tail -f /dev/null & wait ${!}
done
