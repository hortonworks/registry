Examples
========

SchemaRegistry comes with examples of integration into Kafka. You can find the code
`here <https://github.com/hortonworks/registry/tree/master/examples/schema-registry/avro/src/main/java/com/hortonworks/registries/schemaregistry/examples>`_.


Running Kafka Producer with AvroSerializer
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

1. Login into one of the Kafka broker hosts

2.

.. code-block:: bash

  bin/kafka-topics.sh --create --bootstrap-server <kafka host>:9092 --replication-factor 1 --partitions 2 --topic truck_events_stream

3. On registry host;

.. code-block:: bash

  cd /opt/cloudera/parcels/CDH/lib/schemaregistry/examples/schema-registry/avro/

4. Edit data/kafka-producer.props

.. code-block:: ruby

  topic=truck_events_stream
  bootstrap.servers=<kafka_host1>:9092,<kafka_host2>:9092
  schema.registry.url=http://<regisry_host>:7788/api/v1
  security.protocol=PLAINTEXT
  key.serializer=org.apache.kafka.common.serialization.StringSerializer
  value.serializer=com.hortonworks.registries.schemaregistry.serdes.avro.kafka.KafkaAvroSerializer
  ignoreInvalidMessages=true

5. The following command will register truck_events schema in ``data/truck_events.avsc`` into registry and ingests 200 messages into topic “truck_events_stream”

.. code-block:: bash

  java -jar avro-examples-0.*.jar -d data/truck_events_json -p data/kafka-producer.props -sm -s data/truck_events.avsc

(java is installed in ``/usr/java/jdk1.8.0_232-cloudera/bin/java``)


To run the producer in Secure cluster:
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

1. Issue ACLs on the topic you are trying to ingest

2. create kafka topic:

  Make sure you replace ``principal_name`` with the username you are trying to ingest

.. code-block:: bash
   
  bin/kafka-acls.sh --authorizer kafka.security.auth.SimpleAclAuthorizer --authorizer-properties zookeeper.connect=<zookeeper_host>:2181 --add --allow-principal User:principal_name --allow-host "*" --operation All --topic truck_events_stream
   
3. On registry host;

.. code-block:: bash

  cd /opt/cloudera/parcels/CDH/lib/schemaregistry/examples/schema-registry/avro/

edit ``data/kafka-producer.props`` , add "security.protocol=SASL_PLAINTEXT"

.. code-block:: ruby

  topic=truck_events_stream
  bootstrap.servers=<kafka_host1>:9092,<kafka_host2>:9092
  schema.registry.url=http://<regisry_host>:7788/api/v1
  security.protocol=SASL_PLAINTEXT
  key.serializer=org.apache.kafka.common.serialization.StringSerializer
  value.serializer=com.hortonworks.registries.schemaregistry.serdes.avro.kafka.KafkaAvroSerializer

4. Create following ``/etc/kafka/conf/kafka_client_jaas.conf`` to pass to the Kafka Producer's JVM

.. code-block:: ruby

  KafkaClient {
    com.sun.security.auth.module.Krb5LoginModule required
    useTicketCache=true
    renewTicket=true
    serviceName="kafka";
  };
  
In the above config we are expecting Kafka brokers running with principal ``kafka``.

5.

.. code-block:: bash

  kinit -kt your.keytab principal@EXAMPLE.COM
     
Make sure you gave ACLs to the principal refer to [2]

6. java -Djava.security.auth.login.config=/etc/kafka/conf/kafka_client_jaas.conf -jar avro-examples-0.*.jar -d data/truck_events_json -p data/kafka-producer.props -sm -s data/truck_events.avsc


To run the producer in Secure cluster using dynamic JAAS configuration:
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

1. Issue ACLs on the topic you are trying to ingest

2. create kafka topic

  Make sure you replace "principal_name" with the username you are trying to ingest

.. code-block:: bash

  bin/kafka-acls.sh --authorizer kafka.security.auth.SimpleAclAuthorizer --authorizer-properties zookeeper.connect=<zookeeper_host>:2181 --add --allow-principal User:principal_name --allow-host "*" --operation All --topic truck_events_stream

3. On registry host;

.. code-block:: bash

  cd /opt/cloudera/parcels/CDH/lib/schemaregistry/examples/schema-registry/avro/
   
edit ``data/kafka-producer.props`` , add ``security.protocol=SASL_PLAINTEXT`` and ``sasl.jaas.config`` parameter

.. code-block:: ruby

  topic=truck_events_stream
  bootstrap.servers=<kafka_host1>:9092,<kafka_host2>:9092
  schema.registry.url=http://<regisry_host>:7788/api/v1
  security.protocol=SASL_PLAINTEXT
  key.serializer=org.apache.kafka.common.serialization.StringSerializer
  value.serializer=com.hortonworks.registries.schemaregistry.serdes.avro.kafka.KafkaAvroSerializer
  sasl.jaas.config=com.sun.security.auth.module.Krb5LoginModule required useTicketCache=true renewTicket=true serviceName="kafka";

4.

.. code-block:: bash

  kinit -kt your.keytab principal@EXAMPLE.COM 

Make sure you gave ACLs to the principal refer to [2]

5.

.. code-block:: bash

  java -jar avro-examples-0.*.jar -d data/truck_events_json -p data/kafka-producer.props -sm -s data/truck_events.avsc



Running Kafka Consumer with AvroDeserializer
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

1. On registry host;

.. code-block:: bash

  cd /opt/cloudera/parcels/CDH/lib/schemaregistry/examples/schema-registry/avro/

Edit ``data/kafka-consumer.props``

.. code-block:: ruby

  topic=truck_events_stream
  bootstrap.servers=<kafka_host1>:9092,<kafka_host2>:9092
  schema.registry.url=http://<regisry_host>:7788/api/v1
  security.protocol=PLAINTEXT
  key.deserializer=org.apache.kafka.common.serialization.StringDeserializer
  value.deserializer=com.hortonworks.registries.schemaregistry.serdes.avro.kafka.KafkaAvroDeserializer
  group.id=truck_group
  auto.offset.reset=earliest

2.

.. code-block:: bash

  java -jar avro-examples-0.*.jar -c data/kafka-consumer.props -cm -s data/truck_events.avsc



To run the consumer in Secure cluster:
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

1. On registry host;

.. code-block:: bash

  cd /opt/cloudera/parcels/CDH/lib/schemaregistry/examples/schema-registry/avro/

Edit ``data/kafka-consumer.props``

.. code-block:: ruby

 topic=truck_events_stream
 bootstrap.servers=<kafka_host1>:9092,<kafka_host2>:9092
 schema.registry.url=http://<regisry_host>:7788/api/v1
 security.protocol=SASL_PLAINTEXT
 key.deserializer=org.apache.kafka.common.serialization.StringDeserializer
 value.deserializer=com.hortonworks.registries.schemaregistry.serdes.avro.kafka.KafkaAvroDeserializer
 group.id=truck_group
 auto.offset.reset=earliest

2. Create following ``/etc/kafka/conf/kafka_client_jaas.conf`` to pass to the Kafka Producer's JVM

.. code-block:: ruby

  KafkaClient {
    com.sun.security.auth.module.Krb5LoginModule required
    useTicketCache=true
    renewTicket=true
    serviceName="kafka";
  };
   
In the above config we are expecting Kafka brokers running with principal "kafka".

3.

.. code-block:: bash

  kinit -kt your.keytab principal@EXAMPLE.COM
     
Make sure you gave ACLs to the pricncipal refer to [2]

4.

.. code-block:: bash

  java -Djava.security.auth.login.config=/etc/kafka/conf/kafka_client_jaas.conf -jar avro-examples-0.*.jar -c data/kafka-consumer.props -cm


To run the consumer in Secure cluster using dynamic JAAS configuration:
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

1. On registry host;

.. code-block:: bash

  cd /opt/cloudera/parcels/CDH/lib/schemaregistry/examples/schema-registry/avro/

Edit ``data/kafka-consumer.props``

.. code-block:: ruby

 topic=truck_events_stream
 bootstrap.servers=<kafka_host1>:9092,<kafka_host2>:9092
 schema.registry.url=http://<regisry_host>:7788/api/v1
 security.protocol=SASL_PLAINTEXT
 key.deserializer=org.apache.kafka.common.serialization.StringDeserializer
 value.deserializer=com.hortonworks.registries.schemaregistry.serdes.avro.kafka.KafkaAvroDeserializer
 group.id=truck_group
 auto.offset.reset=earliest
 sasl.jaas.config=com.sun.security.auth.module.Krb5LoginModule required useTicketCache=true renewTicket=true serviceName="kafka";

2.

.. code-block:: bash

  kinit -kt your.keytab principal@EXAMPLE.COM

Make sure you gave ACLs to the pricncipal refer to [2]

3.

.. code-block:: bash

  java -jar avro-examples-0.*.jar -c data/kafka-consumer.props -cm
