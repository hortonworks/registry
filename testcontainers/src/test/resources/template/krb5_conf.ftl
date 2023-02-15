[libdefaults]
#  renew_lifetime = 7d (should be commented otherwise "Attempt to obtain new INITIATE credentials failed! (null)")
  forwardable = true
  default_realm = K8S.COM
  ticket_lifetime = 24h
  dns_lookup_realm = false
  dns_lookup_kdc = false
  allow_weak_crypto = false
  clockskew = 300
  rdns = false
  udp_preference_limit = 1
[logging]
  default = FILE:/tmp/kerberos_default.log
  admin_server = FILE:/tmp/kerberos_admin_server.log
  kdc = FILE:/tmp/kerberos_kdc.log
  debug = true
[realms]
  K8S.COM = {
    admin_server = ${kerberosConf.adminServerHost}:${kerberosConf.adminServerPort?c}
    kdc = ${kerberosConf.kdcHost}:${kerberosConf.kdcPort?c}
  }



