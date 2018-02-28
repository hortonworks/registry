SchemaRegistry
==============

SchemaRegistry provides a central repository for a message’s metadata. A
schema specifies the message structure and type. Schema Registry will
allow us to store these schemas efficiently and provides a pluggable
serializer/deserializer interfaces and run-time provision of
serializer/deserializer implementations based on incoming messages.
Schema registry will also enable reuse/discovery/authoring/collaboration
related to schemas.

Each Schema is mainly represented with metadata like

    name - Name of the schema which is unique across the schema registry.

    type - Represents the type of schema. For ex Avro, ProtoBuf, Json etc

    schemaGroup - Group of schemas in which this schema belongs to. It can
    be like Kafka, Hive, Spark or system log etc

    compatibility - Compatibility between different versions of the schema.

    description - Description about the different versions of a schema.

Each of these schemas can evolve with multiple versions. Each version of
the Schema can have

    schemaText - Textual representation of schema

    description - Description about this version

Compatibility
~~~~~~~~~~~~~

Compatibility of different versions of a schema can be configured with
any of the below values

Backward - It indicates that new version of a
schema would be compatible with earlier version of that schema. That
means the data written from earlier version of the schema, can be
deserialized with a new version of the schema.

Forward - It indicates
that an existing schema is compatible with subsequent versions of the
schema. That means the data written from new version of the schema can
still be read with old version of the schema.

Full - It indicates that
a new version of the schema provides both backward and forward
compatibilities.

None - There is no compatibility between different
versions of a schema.

Quickstart
==========

Installation
~~~~~~~~~~~~

1. Download the latest release from `here <https://github.com/hortonworks/registry/releases>`_
2. Registry server can be started with in-memory store or a persistent store
   like mysql. To setup with mysql please follow the instructions `here <https://github.com/hortonworks/registry/blob/master/SETUP.md>`__.
3. To start with in-memory store.

::

   cp $REGISTRY_HOME/conf/registry.yaml.inmemory.example $REGISTRY_HOME/conf/registry.yaml
   # start the server in fore-ground
   $REGISTRY_HOME/bin/registry-server-start conf/registry.yaml
   # To start in daemon mode
   sudo ./bin/registry start

4. Access the UI at http://host.name:9090

.. figure:: https://raw.githubusercontent.com/hortonworks/registry/master/docs/images/registry-homepage.png
  :alt: SchemaRegistry UI

  SchemaRegistry UI

Running Kafka Example
~~~~~~~~~~~~~~~~~~~~~

SchemaRegistry makes it very easy to integrate with Kafka, Storm and
Nifi and any other systems. We've an example code on how to integrate
with kafka `here
<https://github.com/hortonworks/registry/blob/master/examples/schema-registry/avro/src/main/java/com/hortonworks/registries/schemaregistry/examples/avro/TruckEventsKafkaAvroSerDesApp.java>`_.

To run this example, follow the steps below

Download and Start Apache Kafka
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

1. Download kafka 0.10.0.1 or higher from `here <http://kafka.apache.org/downloads>`_.
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

Kafka Producer Integration with SchemaRegistry
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

1. Any client integration code must make a dependency on
   schema-registry-serdes
.. code::

  <dependency>
    <groupId>com.hortonworks.registries</groupId>
    <artifactId>schema-registry-serdes</artifactId>
  </dependency>

2. For KafkaProducer, user need to add the following config

.. code:: java

     config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
     config.put(SchemaRegistryClient.Configuration.SCHEMA_REGISTRY_URL.name(), props.get(SCHEMA_REGISTRY_URL));
     config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
     config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());

Important settings from the above are
**schema.registry.url**:
  This should be set to where the registry server is running ex: http://localhost:9090/api/v1

**key.serializer**:
  *StringSerializer* is used in the above example.

**value.serializer**:
  *com.hortonworks.registries.schemaregistry.serdes.avro.kafka.KafkaAvroSerializer* is used in the above example. This serializer has integration with schema registry. It will take the producer config and retrieves schema.registry.url and the topic name to find out the schema. If there is no schema defined it will publish a first version of that schema.

Run consumer to retrieve schema and deserialze the messages
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

1. cd $REGISTRY\_HOME/examples/schema-registry/avro
2. To consume messages from topic "truck\_events\_stream"

::

    java -jar avro-examples-0.5.0-SNAPSHOT.jar -cm -c data/kafka-consumer.props
    press ctrl + c to stop

Kafka Consumer Integration with SchemaRegistry
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

1. Any client integration code must make a dependency on
   schema-registry-serdes
.. code::

  <dependency>
    <groupId>com.hortonworks.registries</groupId>
    <artifactId>schema-registry-serdes</artifactId>
  </dependency>

2. For KafkaConsumer, user need to add the following to config

.. code:: java

     config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
     config.put(SchemaRegistryClient.Configuration.SCHEMA_REGISTRY_URL.name(), props.get(SCHEMA_REGISTRY_URL));
     config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
     config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());

Important settings from the above are

**schema.registry.url**:
  This should be set to where the registry server is running ex: http://localhost:9090/api/v1

**key.deserializer**:
  *StringDeserializer* is used in the above example.

**value.deserializer**:
  *com.hortonworks.registries.schemaregistry.serdes.avro.kafka.KafkaAvroDeserializer* is used in the above example.

This deserializer tries to find schema.id in the message payload. If it
finds schema.id, makes a call to schema registry to fetch the avro
schema. If it doesn't find schema.id it falls back to using topic name
to fetch a schema.

API examples
============

Using schema related APIs
~~~~~~~~~~~~~~~~~~~~~~~~~

Below set of code snippets explain how SchemaRegistryClient can be used
for - registering new versions of schemas - fetching registered schema
versions - registering serializers/deserializers - fetching
serializer/deserializer for a given schema

.. code:: java

   String schema1 = getSchema(""/device.avsc");
   SchemaMetadata schemaMetadata = createSchemaMetadata("com.hwx.schemas.sample-" + System.currentTimeMillis());

   // registering a new schema
   SchemaIdVersion v1 = schemaRegistryClient.addSchemaVersion(schemaMetadata, new SchemaVersion(schema1, "Initial version of the schema"));
   LOG.info("Registered schema [{}] and returned version [{}]", schema1, v1);

   // adding a new version of the schema
   String schema2 = getSchema("/device-next.avsc");
   SchemaVersion schemaInfo2 = new SchemaVersion(schema2, "second version");
   SchemaIdVersion v2 = schemaRegistryClient.addSchemaVersion(schemaMetadata, schemaInfo2);
   LOG.info("Registered schema [{}] and returned version [{}]", schema2, v2);

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

Registering serializer and deserializer is done with the below steps

- Upload jar file which contains serializer and deserializer classes and
its dependencies - Register serializer/deserializer.
- Map serializer/deserializer with a registered schema.
- Fetch Serializer/Deserializer and use it to marshal/unmarshal payloads.

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


   String simpleSerializerClassName = "org.apache.schemaregistry.samples.serdes.SimpleSerializer";
   String simpleDeserializerClassName = "org.apache.schemaregistry.samples.serdes.SimpleDeserializer";

   SerDesPair serializerInfo = new SerDesPair(
           "simple-serializer-deserializer",
           "simple serializer and deserializer",
           fileId,
           simpleSerializerClassName,
           simpleDeserializerClassName);
   Long serDesId = schemaRegistryClient.addSerDes(serializerInfo);

Map serializer/deserializer with a schema
'''''''''''''''''''''''''''''''''''''''''

.. code:: java


   // map serializer and deserializer with schema key
   // for each schema, one serializer/deserializer is sufficient unless someone want to maintain multiple implementations of serializers/deserializers
   String schemaName = ...
   schemaRegistryClient.mapSchemaWithSerDes(schemaName, serializerId);

Marshal and unmarshal using the registered serializer and deserializer for a schema
'''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''

.. code:: java

   SnapshotSerializer<Object, byte[], SchemaMetadata> snapshotSerializer = getSnapshotSerializer(schemaMetadata);
   String payload = "Random text: " + new Random().nextLong();
   byte[] serializedBytes = snapshotSerializer.serialize(payload, schemaMetadata);

   SnapshotDeserializer<byte[], Object, Integer> snapshotdeserializer = getSnapshotDeserializer(schemaMetadata);
   Object deserializedObject = snapshotdeserializer.deserialize(serializedBytes, null);
