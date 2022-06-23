{{- define "entrypoint.sh" }}
#!/bin/bash 
{{ if eq .Values.security.type "kerberos" }}
bash /tmp/create-certificate.sh
KERBEROS_PRINCIPAL="HTTP/{{ .Release.Name }}.{{ .Values.namespace }}.svc.cluster.local@K8S.COM"
kadmin -p 'admin/admin' -w Hortonworks -q "addprinc -randkey ${KERBEROS_PRINCIPAL}"
kadmin -p 'admin/admin' -w Hortonworks -q "ktadd -norandkey -k /tmp/registry.service.keytab ${KERBEROS_PRINCIPAL}"

{{ end }}
cd /opt/registry/
chmod 755  ./bootstrap/bootstrap-storage.sh
./bootstrap/bootstrap-storage.sh create
./bin/registry-server-start.sh {{ .Values.configfile }}

{{- end }}

