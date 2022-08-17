# Atlas configuration only applies if enabled is true
atlas:
  enabled: ${config.atlasConfiguration.enabled?c}
  atlasUrls:
    # list of Atlas URLs
<#list config.atlasConfiguration.atlasUrls as url>
    - ${url}
</#list>
  basicAuth:
    username: "${config.atlasConfiguration.basicAuth.username}"
    password: "${config.atlasConfiguration.basicAuth.password}"
  waitBetweenAuditProcessing: ${config.atlasConfiguration.waitBetweenAuditProcessing?c}
<#if config.atlasConfiguration.customClasspathLoader?has_content>
  customClasspathLoader: "${config.atlasConfiguration.customClasspathLoader}"
  customClasspath: "${config.atlasConfiguration.customClasspath}"
</#if>

# registries configuration
schemaProviders:
  - providerClass: "com.hortonworks.registries.schemaregistry.avro.AvroSchemaProvider"
    defaultSerializerClass: "com.hortonworks.registries.schemaregistry.serdes.avro.AvroSnapshotSerializer"
    defaultDeserializerClass: "com.hortonworks.registries.schemaregistry.serdes.avro.AvroSnapshotDeserializer"
    hashFunction: "MD5"
  - providerClass: "com.hortonworks.registries.schemaregistry.json.JsonSchemaProvider"
    defaultSerializerClass: "com.hortonworks.registries.schemaregistry.serdes.json.JsonSnapshotSerializer"
    defaultDeserializerClass: "com.hortonworks.registries.schemaregistry.serdes.json.JsonSnapshotDeserializer"
    hashFunction: "MD5"
# schema reviewer configuration
customSchemaStateExecutor:
  className: "com.hortonworks.registries.schemaregistry.state.DefaultCustomSchemaStateExecutor"
  props:
# authorization properties
#authorization:
#  authorizationAgentClassName: "com.hortonworks.registries.schemaregistry.authorizer.agent.DefaultAuthorizationAgent"

servletFilters:
<#list config.servletFilters as filter>
  - className: "${filter.className}"
    params:
<#list filter.params as key, value>
      ${key}: "${value}"
</#list>
</#list>

## HA configuration
## When no configuration is set, then all the nodes in schema registry cluster are eligible to write to the backend storage.
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
    directory: "${config.fileStorageConfiguration.properties.directory}"


# MySQL based jdbc provider configuration is:
storageProviderConfiguration:
 providerClass: "com.hortonworks.registries.storage.impl.jdbc.JdbcStorageManager"
 properties:
   db.type: "${config.storageProviderConfiguration.properties.dbtype}"
   queryTimeoutInSecs: ${config.storageProviderConfiguration.properties.queryTimeoutInSecs}
   db.properties:
     dataSourceClassName: "${config.storageProviderConfiguration.properties.properties.dataSourceClassName}"
     dataSource.url: "${config.storageProviderConfiguration.properties.properties.dataSourceUrl}"
     dataSource.user: "${config.storageProviderConfiguration.properties.properties.dataSourceUser}"
     dataSource.password: "${config.storageProviderConfiguration.properties.properties.dataSourcePassword}"

#swagger configuration
swagger:
  resourcePackage: com.hortonworks.registries.schemaregistry.webservice,com.cloudera.dim.atlas

#enable CORS, may want to disable in production
enableCors: true
#Set below 3 properties if server needs a proxy to connect. Useful to download mysql jar
#httpProxyUrl: "http://proxyHost:port"
#httpProxyUsername: "username"
#httpProxyPassword: "password"

compatibility:
  avroCompatibility: "${config.compatibility.avroCompatibility}"
  jsonCompatibility: "${config.compatibility.jsonCompatibility}"
  validationLevel: "${config.compatibility.validationLevel}"

server:
  allowedMethods: 
    - GET
    - POST
    - PUT
    - DELETE
    - HEAD
    - OPTIONS
  applicationConnectors:
    - type: ${connectorType}
      port: ${port}
<#if tlsConfig?? && tlsConfig.needClientAuth>
      needClientAuth: ${tlsConfig.needClientAuth?string('true', 'false')}
      keyStorePath: "${tlsConfig.keyStorePath}"
      keyStorePassword: "${tlsConfig.keyStorePassword}"
      certAlias: "${tlsConfig.certAlias}"
      enableCRLDP: ${tlsConfig.enableCRLDP?string('true', 'false')}
      trustStorePath: "${tlsConfig.trustStorePath}"
      trustStorePassword: "${tlsConfig.trustStorePassword}"
</#if>
  adminConnectors: []

# Logging settings.
logging:
  level: INFO
  appenders:
    - type: console
      logFormat: "%d{HH:mm:ss.SSS} [%thread] %-5level %logger{36} - %msg%n"
  loggers:
    com.hortonworks.registries: DEBUG
    com.cloudera.dim: DEBUG
    org.apache.atlas.plugin.classloader: DEBUG

# Config for schema registry kerberos principle
#serviceAuthenticationConfiguration:
#  type: kerberos
#  properties:
#    principal: "schema-registry/hostname@GCE.CLOUDERA.COM"
#    keytab: "/tmp/schema-registry.keytab"

