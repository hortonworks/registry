SchemaRegistry
==============

SchemaRegistry provides a central repository for a message’s metadata. A
schema specifies the message structure and type. Schema Registry will
allow us to store these schemas efficiently and provides a pluggable
serializerserializer interfaces and run-time provision of
serializer/deserializer implementations based on incoming messages.
Schema registry will also enable reuse/discovery/authoring/collaboration
related to schemas.

Each Schema is mainly represented with metadata like - name - name of
the schema which is unique across the schema registry. - type -
Represents the type of schema. For ex Avro, ProtoBuf, Json etc -
schemaGroup - group of schemas in which this schema belongs to. It can
be like Kafka, Hive, Spark or system log etc - compatibility -
Compatibility between different versions of the schema. - description -
Description about the different versions of a schema.

Each of these schemas can evolve with multiple versions. Each version of
the Schema can have - schemaText - Textual representation of schema -
description - Description about this version

Compatibility
~~~~~~~~~~~~~

Compatibility of different versions of a schema can be configured with
any of the below values - Backward - It indicates that new version of a
schema would be compatible with earlier version of that schema. That
means the data written from earlier version of the schema, can be
deserialized with a new version of the schema. - Forward - It indicates
that an existing schema is compatible with subsequent versions of the
schema. That means the data written from new version of the schema can
still be read with old version of the schema. - Full - It indicates that
a new version of the schema provides both backward and forward
compatibilities. - None - There is no compatibility between different
versions of a schema.

Quickstart
==========

Installation
------------

1. Download the latest release from here
   https://github.com/hortonworks/registry/releases
2. registry server can start with in-memory store or a persistent store
   like mysql. To setup with mysql please follow the instructions
   `here <https://github.com/hortonworks/registry/blob/master/SETUP.md>`__.
3. To start with in-memory store.

``cp $REGISTRY_HOME/conf/registry.yaml.inmemory.example $REGISTRY_HOME/conf/registry.yaml       # start the server in fore-ground       $REGISTRY_HOME/bin/registry-server-start conf/registry.yaml       # To start in daemon mode       sudo ./bin/registry start``

4. Access the UI at http://host.name:9090

.. figure:: https://raw.githubusercontent.com/hortonworks/registry/master/docs/images/registry-homepage.png
   :alt: SchemaRegistry UI

   SchemaRegistry UI

Running Kafka Example
---------------------

SchemaRegistry makes it very easy to integrate with Kafka, Storm and
Nifi and any other systems. We've an example code on how to integrate
with kafka [here]
(https://github.com/hortonworks/registry/blob/master/examples/schema-registry/avro/src/main/java/com/hortonworks/registries/schemaregistry/examples/avro/TruckEventsKafkaAvroSerDesApp.java)

To run this example, follow the steps below

Download and Start Apache Kafka
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

1. Download kafka 0.10.0.1 or higher http://kafka.apache.org/downloads
2. $KAFKA\_HOME/bin/zoookeeper-server-start.sh
   config/zookeeper.properties
3. $KAFKA\_HOME/bin/kafka-server-start.sh config/server.properties
4. $KAFKA\_HOME/bin/kafka-topics.sh --zookeeper localhost:2181 --topic
   truck\_events\_stream --partitions 1 --replication-factor 1 --create

Run producer to register schema and send data
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

1. cd $REGISTRY\_HOME/examples/schema-registry/avro
2. To send messages to topic "truck\_events\_stream"

::

    java -jar avro-examples-0.1.0-SNAPSHOT.jar -d data/truck_events.csv -p data/kafka-producer.props -sm -s data/truck_events.avsc

Kafka Produer Integration with SchemaRegistry
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

1. Any client integration code must make a dependency on
   schema-registry-serdes

``<dependency>         <groupId>com.hortonworks.registries</groupId>        <artifactId>schema-registry-serdes</artifactId>     </dependency>``
2. For KafkaProducer, user need to add the following config

\`\`\`java

::

     config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
     config.putAll(Collections.singletonMap(SchemaRegistryClient.Configuration.SCHEMA_REGISTRY_URL.name(), props.get(SCHEMA_REGISTRY_URL)));
     config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
     config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());

\`\`\` Important settings from above are

**schema.registry.url**, this should set to where the registry server is
running ex: http://localhost:9090/api/v1

**key.serializer & value.serializer**. In example above we are setting
value.serializer to
**com.hortonworks.registries.schemaregistry.serdes.avro.kafka.KafkaAvroSerializer**

::

    This serializer has integration with schema registry. It will take the producer config and retrieves schema.registry.url and the topic name to find out the schema. If there is no schema defined it will publish a first version of that schema.

Run consumer to retrieve schema and deserialze the messages
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

1. cd $REGISTRY\_HOME/examples/schema-registry/avro
2. To consume messages from topic "truck\_events\_stream"

::

    java -jar avro-examples-0.1.0-SNAPSHOT.jar -cm -c data/kafka-consumer.props
    press ctrl + c to stop

Kafka Consumer Integration with SchemaRegistry
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

1. For KafkaConsumer, user need to add the following to config

.. code:: java

     config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
     config.putAll(Collections.singletonMap(SchemaRegistryClient.Configuration.SCHEMA_REGISTRY_URL.name(), props.get(SCHEMA_REGISTRY_URL)));
     config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
     config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());

Important settings from above are

**schema.registry.url**, this should set to where the registry server is
running ex: http://localhost:9090/api/v1

**key.deserializer & value.deserializer**. In example above we are
setting value.deserializer to
**com.hortonworks.registries.schemaregistry.serdes.avro.kafka.KafkaAvroDeserializer**

This deserializer tries to find schema.id in the message paylod. If it
finds schema.id, makes a call to schemaregistry to fetch the avro
schema. If it doesn't find schema.id it falls back to using topic name
to fetch a schema.

API examples
============

Using schema related APIs
-------------------------

Below set of code snippets explain how SchemaRegistryClient can be used
for - registering new versions of schemas - fetching registered schema
versions - registering serializers/deserializers - fetching
serializer/deserializer for a given schema

.. code:: java


    Map<String, Object> config = new HashMap<>();
    config.put(SchemaRegistryClient.Options.SCHEMA_REGISTRY_URL, "http://localhost:8080/api/v1");
    config.put(SchemaRegistryClient.Options.CLASSLOADER_CACHE_SIZE, 10);
    config.put(SchemaRegistryClient.Options.CLASSLOADER_CACHE_EXPIRY_INTERVAL_MILLISECS, 5000L);

    schemaRegistryClient = new SchemaRegistryClient(config);

    String schemaFileName = "/device.avsc";
    String schema1 = getSchema(schemaFileName);
    SchemaMetadata schemaMetadata = createSchemaMetadata("com.hwx.schemas.sample-" + System.currentTimeMillis());

    // registering a new schema
    Integer v1 = schemaRegistryClient.addSchemaVersion(schemaMetadata, new SchemaVersion(schema1, "Initial version of the schema"));
    LOG.info("Registered schema [{}] and returned version [{}]", schema1, v1);

    // adding a new version of the schema
    String schema2 = getSchema("/device-next.avsc");
    SchemaVersion schemaInfo2 = new SchemaVersion(schema2, "second version");
    Integer v2 = schemaRegistryClient.addSchemaVersion(schemaMetadata, schemaInfo2);
    LOG.info("Registered schema [{}] and returned version [{}]", schema2, v2);

    //adding same schema returns the earlier registered version
    Integer version = schemaRegistryClient.addSchemaVersion(schemaMetadata, schemaInfo2);
    LOG.info("");

    // get a specific version of the schema
    String schemaName = schemaMetadata.getName();
    SchemaVersionInfo schemaVersionInfo = schemaRegistryClient.getSchemaVersionInfo(new SchemaVersionKey(schemaName, v2));

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

Default serializer and deserializer APIs.
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Default serializer and deserializer for a given schema provider can be
retrieved with the below APIs.

.. code:: java

    // for avro,
    AvroSnapshotSerializer serializer = schemaRegistryClient.getDefaultSerializer(AvroSchemaProvider.TYPE);
    AvroSnapshotDeserializer deserializer = schemaRegistryClient.getDefaultDeserializer(AvroSchemaProvider.TYPE);

Using serializer and deserializer related APIs
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Registering serializer and deserializer is done with the below steps -
Upload jar file which contains serializer and deserializer classes and
its dependencies - Register serializer/deserializer - Map
serializer/deserializer with a registered schema. - Fetch
Serializer/Deserializer and use it to marshal/unmarshal payloads.

Uploading jar file
''''''''''''''''''

.. code:: java


    String serdesJarName = "/serdes-examples.jar";
    InputStream serdesJarInputStream = SampleSchemaRegistryApplication.class.getResourceAsStream(serdesJarName);
    if (serdesJarInputStream == null) {
        throw new RuntimeException("Jar " + serdesJarName + " could not be loaded");
    }

    String fileId = schemaRegistryClient.uploadFile(serdesJarInputStream);

Register serializer and deserializer
''''''''''''''''''''''''''''''''''''

.. code:: java


    String simpleSerializerClassName = "com.hortonworks.schemaregistry.samples.serdes.SimpleSerializer";
    SerDesInfo serializerInfo = new SerDesInfo.Builder()
                                                .name("simple-serializer")
                                                .description("simple serializer")
                                                .fileId(fileId)
                                                .className(simpleSerializerClassName)
                                                .buildSerializerInfo();
    Long serializerId = schemaRegistryClient.addSerializer(serializerInfo);

    String simpleDeserializerClassName = "com.hortonworks.schemaregistry.samples.serdes.SimpleDeserializer";
    SerDesInfo deserializerInfo = new SerDesInfo.Builder()
            .name("simple-deserializer")
            .description("simple deserializer")
            .fileId(fileId)
            .className(simpleDeserializerClassName)
            .buildDeserializerInfo();
    Long deserializerId = schemaRegistryClient.addDeserializer(deserializerInfo);

Map serializer/deserializer with a schema
'''''''''''''''''''''''''''''''''''''''''

.. code:: java


    // map serializer and deserializer with schemakey
    // for each schema, one serializer/deserializer is sufficient unless someone want to maintain multiple implementations of serializers/deserializers
    String schemaName = ...
    schemaRegistryClient.mapSchemaWithSerDes(schemaName, serializerId);
    schemaRegistryClient.mapSchemaWithSerDes(schemaName, deserializerId);

Marshal and unmarshal using the registered serializer and deserializer for a schema
'''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''

.. code:: java

    SnapshotSerializer<Object, byte[], SchemaMetadata> snapshotSerializer = getSnapshotSerializer(schemaKey);
    String payload = "Random text: " + new Random().nextLong();
    byte[] serializedBytes = snapshotSerializer.serialize(payload, schemaInfo);

    SnapshotDeserializer<byte[], Object, SchemaMetadata, Integer> snapshotDeserializer = getSnapshotDeserializer(schemaKey);
    Object deserializedObject = snapshotDeserializer.deserialize(serializedBytes, schemaInfo, schemaInfo);

    LOG.info("Given payload and deserialized object are equal: "+ payload.equals(deserializedObject));
