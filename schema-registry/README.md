# Introduction
 
Each Schema is mainly represented with metadata like 
- name
  - name of the schema which is unique across the schema registry. 
- type
  - Represents the type of schema. For ex Avro, ProtoBuf, Json etc
- schemaGroup
  - group of schemas in which this schema belongs to. It can be like Kafka, Hive, Spark or system log etc
- compatibility
  - Compatibility between different versions of the schema.
- description
  - Description about the different versions of a schema.


Each of these schemas can evolve with multiple versions. Each version of the Schema can have 
- schemaText
  - Textual representation of schema
- description
  - Description about this version
    
### Compatibility
Compatibility of different versions of a schema can be configured with any of the below values
- Backward
  - It indicates that new version of a schema would be compatible with earlier version of that schema. That means the data written from earlier version of the schema, can be deserialized with a new version of the schema.
- Forward
  - It indicates that an existing schema is compatible with subsequent versions of the schema. That means the data written from new version of the schema can still be read with old version of the schema.
- Full
  - It indicates that a new version of the schema provides both backward and forward compatibilities. 
- None
  - There is no compatibility between different versions of a schema.

# SchemaRegistry configuration

Configuration file is located at conf/registry-dev.yaml. By default it uses inmemory storage manager. It can be changed to use MySQL as storage by providing configuration like below. You should configure respective dataSourceUrl

```
# MySQL based jdbc provider configuration is:
storageProviderConfiguration:
  providerClass: "com.hortonworks.registries.storage.impl.jdbc.JdbcStorageManager"
  properties:
    db.type: "mysql"
    queryTimeoutInSecs: 30
    db.properties:
      dataSourceClassName: "com.mysql.cj.jdbc.MysqlDataSource"
      dataSource.url: "jdbc:mysql://localhost:3307/schema_registry"

```

Before starting the SchemaRegistry server, below script should be run with configured database names.

```mysql
-- CREATE DATABASE IF NOT EXISTS schema_registry;
-- USE schema_registry;

-- THE NAMES OF THE TABLE COLUMNS MUST MATCH THE NAMES OF THE CORRESPONDING CLASS MODEL FIELDS
CREATE TABLE IF NOT EXISTS schema_metadata_info (
  id            BIGINT AUTO_INCREMENT NOT NULL,
  type          VARCHAR(256)          NOT NULL,
  schemaGroup   VARCHAR(256)          NOT NULL,
  name          VARCHAR(256)          NOT NULL,
  compatibility VARCHAR(256)          NOT NULL,
  description   TEXT,
  timestamp     BIGINT                NOT NULL,
  PRIMARY KEY (name),
  UNIQUE KEY (id)
);

CREATE TABLE IF NOT EXISTS schema_version_info (
  id               BIGINT AUTO_INCREMENT NOT NULL,
  description      TEXT,
  schemaText       TEXT                  NOT NULL,
  fingerprint      TEXT                  NOT NULL,
  version          INT                   NOT NULL,
  schemaMetadataId BIGINT                NOT NULL,
  timestamp        BIGINT                NOT NULL,
  name             VARCHAR(256)          NOT NULL,
  UNIQUE KEY (id),
  UNIQUE KEY `UK_METADATA_ID_VERSION_FK` (schemaMetadataId, version),
  PRIMARY KEY (name, version),
  FOREIGN KEY (schemaMetadataId, name) REFERENCES schema_metadata_info (id, name)
);

CREATE TABLE IF NOT EXISTS schema_field_info (
  id               BIGINT AUTO_INCREMENT NOT NULL,
  schemaInstanceId BIGINT                NOT NULL,
  timestamp        BIGINT                NOT NULL,
  name             VARCHAR(256)          NOT NULL,
  fieldNamespace   VARCHAR(256),
  type             VARCHAR(256)          NOT NULL,
  PRIMARY KEY (id),
  FOREIGN KEY (schemaInstanceId) REFERENCES schema_version_info (id)
);

CREATE TABLE IF NOT EXISTS schema_serdes_info (
  id           BIGINT AUTO_INCREMENT NOT NULL,
  description  TEXT,
  name         TEXT                  NOT NULL,
  fileId       TEXT                  NOT NULL,
  className    TEXT                  NOT NULL,
  isSerializer BOOLEAN               NOT NULL,
  timestamp    BIGINT                NOT NULL,
  PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS schema_serdes_mapping (
  schemaMetadataId BIGINT NOT NULL,
  serDesId         BIGINT NOT NULL,

  UNIQUE KEY `UK_IDS` (schemaMetadataId, serdesId)
);

```

# API examples
## Using schema related APIs

Below set of code snippets explain how SchemaRegistryClient can be used for
 - registering new versions of schemas
 - fetching registered schema versions
 - registering serializers/deserializers
 - fetching serializer/deserializer for a given schema

 
```java
SchemaMetadata schemaMetadata = createSchemaMetadata("com.hwx.schemas.sample-" + System.currentTimeMillis());

// registering a new schema
SchemaIdVersion v1 = schemaRegistryClient.addSchemaVersion(schemaMetadata, new SchemaVersion(schema1, "Initial version of the schema"));
LOG.info("Registered schema metadata [{}] and returned version [{}]", schema1, v1);

// adding a new version of the schema
String schema2 = getSchema("/device-next.avsc");
SchemaVersion schemaInfo2 = new SchemaVersion(schema2, "second version");
SchemaIdVersion v2 = schemaRegistryClient.addSchemaVersion(schemaMetadata, schemaInfo2);
LOG.info("Registered schema metadata [{}] and returned version [{}]", schema2, v2);

//adding same schema returns the earlier registered version
SchemaIdVersion version = schemaRegistryClient.addSchemaVersion(schemaMetadata, schemaInfo2);
LOG.info("Received version [{}] for schema metadata [{}]", version, schemaMetadata);

// get a specific version of the schema
String schemaName = schemaMetadata.getName();
SchemaVersionInfo schemaVersionInfo = schemaRegistryClient.getSchemaVersionInfo(new SchemaVersionKey(schemaName, v2.getVersion()));
LOG.info("Received schema version info [{}] for schema metadata [{}]", schemaVersionInfo, schemaMetadata);

// get latest version of the schema
SchemaVersionInfo latest = schemaRegistryClient.getLatestSchemaVersionInfo(schemaName);
LOG.info("Latest schema with schema key [{}] is : [{}]", schemaMetadata, latest);

// get all versions of the schema
Collection<SchemaVersionInfo> allVersions = schemaRegistryClient.getAllVersions(schemaName);
LOG.info("All versions of schema key [{}] is : [{}]", schemaMetadata, allVersions);

// finding schemas containing a specific field
SchemaFieldQuery md5FieldQuery = new SchemaFieldQuery.Builder().name("md5").build();
Collection<SchemaVersionKey> md5SchemaVersionKeys = schemaRegistryClient.findSchemasByFields(md5FieldQuery);
LOG.info("Schemas containing field query [{}] : [{}]", md5FieldQuery, md5SchemaVersionKeys);

SchemaFieldQuery txidFieldQuery = new SchemaFieldQuery.Builder().name("txid").build();
Collection<SchemaVersionKey> txidSchemaVersionKeys = schemaRegistryClient.findSchemasByFields(txidFieldQuery);
LOG.info("Schemas containing field query [{}] : [{}]", txidFieldQuery, txidSchemaVersionKeys);
```

### Default serializer and deserializer APIs.
Default serializer and deserializer for a given schema provider can be retrieved like below.
```java
// For avro,
AvroSnapshotSerializer serializer = schemaRegistryClient.getDefaultSerializer(AvroSchemaProvider.TYPE);
AvroSnapshotDeserializer deserializer = schemaRegistryClient.getDefaultDeserializer(AvroSchemaProvider.TYPE);
```

### Using serializer and deserializer related APIs
Registering serializer and deserializer is done with the below steps
 - Upload jar file which contains serializer and deserializer classes and its dependencies
 - Register serializer/deserializer
 - Map serializer/deserializer with a registered schema.
 - Fetch Serializer/Deserializer and use it to marshal/unmarshal payloads.
 
##### Uploading jar file

```java
String serdesJarName = "/serdes-examples.jar";
InputStream serdesJarInputStream = SampleSchemaRegistryApplication.class.getResourceAsStream(serdesJarName);
if (serdesJarInputStream == null) {
    throw new RuntimeException("Jar " + serdesJarName + " could not be loaded");
}

String fileId = schemaRegistryClient.uploadFile(serdesJarInputStream);
```

##### Register serializer and deserializer

```java
String simpleSerializerClassName = "org.apache.schemaregistry.samples.serdes.SimpleSerializer";
String simpleDeserializerClassName = "org.apache.schemaregistry.samples.serdes.SimpleDeserializer";

SerDesPair serializerInfo = new SerDesPair(
        "simple-serializer-deserializer",
        "simple serializer and deserializer",
        fileId,
        simpleSerializerClassName,
        simpleDeserializerClassName);
Long serDesId = schemaRegistryClient.addSerDes(serializerInfo);
```

##### Map serializer/deserializer with a schema

```java
// map serializer and deserializer with schema key
// for each schema, one serializer/deserializer is sufficient unless someone want to maintain multiple implementations of serializers/deserializers
String schemaName = ...
schemaRegistryClient.mapSchemaWithSerDes(schemaName, serializerId);
```

##### Marshal and unmarshal using the registered serializer and deserializer for a schema

```java
SnapshotSerializer<Object, byte[], SchemaMetadata> snapshotSerializer = getSnapshotSerializer(schemaMetadata);
String payload = "Random text: " + new Random().nextLong();
byte[] serializedBytes = snapshotSerializer.serialize(payload, schemaMetadata);

SnapshotDeserializer<byte[], Object, Integer> snapshotdeserializer = getSnapshotDeserializer(schemaMetadata);
Object deserializedObject = snapshotdeserializer.deserialize(serializedBytes, null);
```

## Using inbuilt Kafka Avro serializer and deserializer

Below Serializer and Deserializer can be used for avro records as respective Kafka avro serializer and deserializer respectively.
`com.hortonworks.registries.schemaregistry.avro.kafka.KafkaAvroSerializer`
`com.hortonworks.registries.schemaregistry.avro.kafka.KafkaAvroDeserializer`

Following properties can be configured for producer/consumer

```java
// producer configuration
props.put(SchemaRegistryClient.Options.SCHEMA_REGISTRY_URL, schemaRegistryUrl);
props.put(SchemaRegistryClient.Options.SCHEMA_CACHE_SIZE, 1000);
props.put(SchemaRegistryClient.Options.SCHEMA_CACHE_EXPIRY_INTERVAL_MILLISECS, 60*60*1000L);
props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());

// consumer configuration
props.put(SchemaRegistryClient.Options.SCHEMA_REGISTRY_URL, schemaRegistryUrl);
props.put(SchemaRegistryClient.Options.SCHEMA_CACHE_SIZE, 1000);
props.put(SchemaRegistryClient.Options.SCHEMA_CACHE_EXPIRY_INTERVAL_MILLISECS, 60*60*1000L);
props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
```