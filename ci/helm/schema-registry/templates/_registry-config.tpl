{{- define "registry.config" }}

# registries configuration
schemaProviders:
  - providerClass: "com.hortonworks.registries.schemaregistry.avro.AvroSchemaProvider"
    defaultSerializerClass: "com.hortonworks.registries.schemaregistry.serdes.avro.AvroSnapshotSerializer"
    defaultDeserializerClass: "com.hortonworks.registries.schemaregistry.serdes.avro.AvroSnapshotDeserializer"


servletFilters:
{{ if eq .Values.security.type "kerberos" }}
  - className: "com.hortonworks.registries.auth.server.AuthenticationFilter"
    params:
      type: "kerberos"
      kerberos.principal: "HTTP/{{ .Release.Name }}.{{ .Values.namespace }}.svc.cluster.local@K8S.COM"

      kerberos.keytab: "/tmp/registry.service.keytab"
      kerberos.name.rules: "RULE:[2:$1@$0]([jt]t@.*K8S.COM)s/.*/$MAPRED_USER/ RULE:[2:$1@$0]([nd]n@.*K8S.COM)s/.*/$HDFS_USER/DEFAULT"
      allowed.resources: "401.html,back-default.png,favicon.ico"
{{ end }}
#Note that it is highly recommended to force ssl connections if you are using the jwt handler config below. Unsecured connections will expose jwt
# - className: "com.hortonworks.registries.auth.server.AuthenticationFilter"
#   params:
#     type: "com.hortonworks.registries.auth.server.JWTAuthenticationHandler"
#     allowed.resources: "401.html,back-default.png,favicon.ico"
#     authentication.provider.url: "https://localhost:8883/gateway/knoxsso/api/v1/websso"
#     public.key.pem: "<public key corresponding to the PKI key pair of the token issuer>"
  - className: "com.hortonworks.registries.schemaregistry.webservice.RewriteUriFilter"
    params:
      # value format is [<targetpath>,<paths-should-be-redirected-to>,*|]*
      # below /subjects and /schemas/ids are forwarded to /api/v1/confluent
      forwardPaths: "/api/v1/confluent,/subjects/*,/schemas/ids/*"
      redirectPaths: "/ui/,/"

# HA configuration
#haConfig:
#  className: com.hortonworks.registries.ha.zk.ZKLeadershipParticipant
#  config:
#    # This url is a list of ZK servers separated by ,
#    connect.url: "localhost:2181"
#    # root node prefix in ZK for this instance
#    root: "/registry"
#    session.timeout.ms: 30000
#    connection.timeout.ms: 20000
#    retry.limit: 5
#    retry.base.sleep.time.ms: 1000
#    retry.max.sleep.time.ms: 5000

# Filesystem based jar storage
fileStorageConfiguration:
  className: "com.hortonworks.registries.common.util.LocalFileSystemStorage"
  properties:
    directory: "/tmp/schema-registry/jars"

# storage provider configuration
storageProviderConfiguration:
{{ if eq .Values.database "inmemory" }}
  providerClass: "com.hortonworks.registries.storage.impl.memory.InMemoryStorageManager"
{{ else if eq .Values.database "mysql" }}
  providerClass: "com.hortonworks.registries.storage.impl.jdbc.JdbcStorageManager"
  properties:
    db.type: "mysql"
    queryTimeoutInSecs: 30
    db.properties:
      dataSourceClassName: "com.mysql.jdbc.jdbc2.optional.MysqlDataSource"
      dataSource.url: "jdbc:mysql://db-mariadb:3306/registry"
      dataSource.user: "registry"
      dataSource.password: "registry"
{{ else if eq .Values.database "postgresql" }}
  providerClass: "com.hortonworks.registries.storage.impl.jdbc.JdbcStorageManager"
  properties:
    db.type: "postgresql"
    queryTimeoutInSecs: 30
    db.properties:
      dataSourceClassName: "org.postgresql.ds.PGSimpleDataSource"
      dataSource.url: "jdbc:postgresql://psql-postgresql/registry"
      dataSource.user: "registry"
      dataSource.password: "registry"
{{ end }}

#swagger configuration
swagger:
  resourcePackage: com.hortonworks.registries.schemaregistry.webservice

#enable CORS, may want to disable in production
enableCors: false

server:
{{ if eq .Values.security.type "kerberos" }}
  applicationConnectors:
    - type: https
      port: 8443
      keyStorePath: /tmp/registry.server.keystore.jks
      keyStorePassword: srv123
      validateCerts: false
      validatePeers: false
  adminConnectors:
    - type: https
      port: 8444
      keyStorePath: /tmp/registry.server.keystore.jks
      keyStorePassword: srv123
      validateCerts: false
      validatePeers: false
{{ else }}
  applicationConnectors:
    - type: http
      port: 9090
  adminConnectors:
    - type: http
      port: 9091
{{ end }}

# Logging settings.
logging:
  level: INFO
  loggers:
    com.hortonworks.registries: TRACE
{{- end }}

