Examples
========

SchemaRegistry comes with examples of integration into Kafka. You can find the code
`here <https://github.com/hortonworks/registry/tree/master/examples/schema-registry/avro/src/main/java/com/hortonworks/registries/schemaregistry/examples>`_.


Running Kafka Producer with AvroSerializer
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

1. Login into one of the Kafka broker hosts
2. bin/kafka-topics.sh --zookeeper zk:2181 --topic truck_events_stream --partitions 2 --replication-factor 1 --create
3. On registry host; cd /usr/hdf/2.1.0.0-164/registry/examples/schema-registry/avro/
4. Edit data/kafka-producer.props, vi data/kafka-producer.props

.. code-block:: ruby

  topic=truck_events_stream
  bootstrap.servers=kafka_host1:6667,kafka_host2:6667
  schema.registry.url=http://regisry_host:9090/api/v1
  security.protocol=PLAINTEXT
  key.serializer=org.apache.kafka.common.serialization.StringSerializer
  value.serializer=com.hortonworks.registries.schemaregistry.serdes.avro.kafka.KafkaAvroSerializer

5. java -jar avro-examples-0.2.jar -d data/truck_events.csv -p data/kafka-producer.props -sm -s data/truck_events.avsc
6. The above command will register truck_events schema in data/truck_events.avsc into registry and ingests 200 messages into topic “truck_events_stream”


To run the producer in Secure cluster:
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

1. Issue ACLs on the topic you are trying to ingest

2. create kafka topic
   bin/kafka-acls.sh --authorizer kafka.security.auth.SimpleAclAuthorizer --authorizer-properties zookeeper.connect=pm-eng1-cluster2.field.hortonworks.com:2181 --add
       --allow-principal User:principal_name --allow-host "*" --operation All --topic truck_events_stream

   2.1 make sure you replace "principal_name" with the username you are trying to ingest

3.  edit data/kafka-producer.props , add security.protocol=SASL_PLAINTEXT

.. code-block:: ruby

  topic=truck_events_stream
  bootstrap.servers=kafka_host1:6667,kafka_host2:6667
  schema.registry.url=http://regisry_host:9090/api/v1
  security.protocol=SASL_PLAINTEXT
  key.serializer=org.apache.kafka.common.serialization.StringSerializer
  value.serializer=com.hortonworks.registries.schemaregistry.serdes.avro.kafka.KafkaAvroSerializer

4. Create following kafka_client_jaas.conf to pass to the Kafka Producer's JVM

.. code-block:: ruby

  KafkaClient {
    com.sun.security.auth.module.Krb5LoginModule required
    useTicketCache=true
    renewTicket=true
    serviceName="kafka";
  }
  In the above config we are expecting Kafka brokers running with principal "kafka".

5. kinit -kt your.keytab principal@EXAMPLE.com. Make sure you gave ACLs to the pricncipal refer to [2]

6. java -Djava.security.auth.login.config=/etc/kafka/conf/kafka_client_jaas.conf -jar avro-examples-0.4.0-SNAPSHOT.jar -d data/truck_events_json -p data/kafka-producer.props -sm -s data/truck_events.avsc



Running Kafka Consumer with AvroDeserializer
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

1. Edit data/kafka-consumer.props

.. code-block:: ruby

  topic=truck_events_stream
  bootstrap.servers=localhost:9092
  schema.registry.url=http://localhost:9090/api/v1
  key.deserializer=org.apache.kafka.common.serialization.StringDeserializer
  value.deserializer=com.hortonworks.registries.schemaregistry.serdes.avro.kafka.KafkaAvroDeserializer
  group.id=truck_group
  auto.offset.reset=earliest

2. java -jar avro-examples-0.2.jar -c data/kafka-consumer.props -cm -s data/truck_events.avsc



To run the consumer in Secure cluster:
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

1. Edit data/kafka-consumer.props

.. code-block:: ruby

 topic=truck_events
 bootstrap.servers=pm-eng1-cluster4.field.hortonworks.com:6667
 schema.registry.url=http://pm-eng1-cluster4.field.hortonworks.com:7788/api/v1
 security.protocol=SASL_PLAINTEXT
 key.deserializer=org.apache.kafka.common.serialization.StringDeserializer
 value.deserializer=com.hortonworks.registries.schemaregistry.serdes.avro.kafka.KafkaAvroDeserializer
 group.id=truck_group
 auto.offset.reset=earliest

2. Create following kafka_client_jaas.conf to pass to the Kafka Producer's JVM

.. code-block:: ruby

   KafkaClient {
     com.sun.security.auth.module.Krb5LoginModule required
     useTicketCache=true
     renewTicket=true
     serviceName="kafka";
   }
   In the above config we are expecting Kafka brokers running with principal "kafka".

3. kinit -kt your.keytab principal@EXAMPLE.com. Make sure you gave ACLs to the pricncipal refer to [2]

4. java -Djava.security.auth.login.config=/etc/kafka/conf/kafka_client_jaas.conf -jar avro-examples-0.4.0-SNAPSHOT.jar -c data/kafka-consumer.props -cm
