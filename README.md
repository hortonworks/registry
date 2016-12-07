#SchemaRegistry

SchemaRegistry provides a central repository for a message’s metadata. A schema specifies the message structure and type. Schema Registry will allow us to store these schemas efficiently and provides a pluggable serializerserializer interfaces and run-time provision of serializer/deserializer implementations based on incoming messages. Schema registry will also enable reuse/discovery/authoring/collaboration related to schemas.


#Quickstart

## Installation
1. Download the latest release from here https://github.com/hortonworks/registry/releases
2. registry server can start with in-memory store or a persistent store like mysql. To setup with mysql please follow the instructions [here](https://github.com/hortonworks/registry/blob/master/SETUP.md).
3. To start with in-memory store.
  
  ```
      cp $REGISTRY_HOME/conf/registry.yaml.inmemory.example $REGISTRY_HOME/conf/registry.yaml
      # start the server in fore-ground
      $REGISTRY_HOME/bin/registry-server-start conf/registry.yaml
      # To start in daemon mode
      sudo ./bin/registry start
  ```
  
4. Access the UI at http://host.name:9090

![SchemaRegistry UI](https://github.com/hortonworks/registry/blob/master/docs/images/registry-homepage.png)

## Running Kafka Example
SchemaRegistry makes it very easy to integrate with Kafka, Storm and Nifi and any other systems.
We've an example code on how to integrate with kafka [here] (https://github.com/hortonworks/registry/blob/master/examples/schema-registry/avro/src/main/java/org/apache/registries/schemaregistry/examples/avro/TruckEventsKafkaAvroSerDesApp.java)

To run this example, follow the steps below

### Download and Start Apache Kafka

1. Download kafka 0.10.0.1 or higher http://kafka.apache.org/downloads
2. $KAFKA_HOME/bin/zoookeeper-server-start.sh config/zookeeper.properties
3. $KAFKA_HOME/bin/kafka-server-start.sh config/server.properties
4. $KAFKA_HOME/bin/kafka-topics.sh --zookeeper localhost:2181 --topic truck_events_stream --partitions 1 --replication-factor 1 --create

### Run producer to register schema and send data

1. cd $REGISTRY_HOME/examples/schema-registry/avro
2. To send messages to topic "truck_events_stream"

```
java -jar avro-examples-0.1.0-SNAPSHOT.jar -d data/truck_events.csv -p data/kafka-producer.props -sm -s data/truck_events.avsc
```

### Kafka Produer Integration with SchemaRegistry

1. Any client integration code must make a dependency on schema-registry-serdes

   ```
	<dependency>
   		<groupId>org.apache.registries</groupId>
	   <artifactId>schema-registry-serdes</artifactId>
	</dependency>
	```
2.  For KafkaProducer, user need to add the following config

   ```java
     
     config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
     config.putAll(Collections.singletonMap(SchemaRegistryClient.Configuration.SCHEMA_REGISTRY_URL.name(), props.get(SCHEMA_REGISTRY_URL)));
     config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
     config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
   ```
   Important settings from above are
   
   **schema.registry.url**, this should set to where the registry server is running ex: http://localhost:9090/api/v1
   
   **key.serializer & value.serializer**.  In example above we are setting value.serializer to **org.apache.registries.schemaregistry.serdes.avro.kafka.KafkaAvroSerializer**
   
    This serializer has integration with schema registry. It will take the producer config and retrieves schema.registry.url and the topic name to find out the schema. If there is no schema defined it will publish a first version of that schema.

### Run consumer to retrieve schema and deserialze the messages

1. cd $REGISTRY_HOME/examples/schema-registry/avro
2. To consume messages from topic "truck_events_stream"

```
java -jar avro-examples-0.1.0-SNAPSHOT.jar -cm -c data/kafka-consumer.props
press ctrl + c to stop
```

### Kafka Consumer Integration with SchemaRegistry

1. For KafkaConsumer, user need to add the following to config

```java
 config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
 config.putAll(Collections.singletonMap(SchemaRegistryClient.Configuration.SCHEMA_REGISTRY_URL.name(), props.get(SCHEMA_REGISTRY_URL)));
 config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
 config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
```

Important settings from above are
   
   **schema.registry.url**, this should set to where the registry server is running ex: http://localhost:9090/api/v1
   
   **key.deserializer & value.deserializer**.  In example above we are setting value.deserializer to **org.apache.registries.schemaregistry.serdes.avro.kafka.KafkaAvroDeserializer**
   
  This deserializer tries to find schema.id in the message paylod. If it finds schema.id, makes a call to schemaregistry  to fetch the avro schema. If it doesn't find schema.id it falls back to using topic name to fetch a schema.

   
