Competition
===========

Schema Registry is only one of the types of registries this project supports.
On the same foundation, other registries such as MLRegistry, Rules, ApplicationConfigs
etc. can be built. The foundation offers StorageManager, REST API structure and
a Versioned Entity store upon which other modules are built.

Specifically in SchemaRegistry, we are providing a solution that can capture
various formats of Schema and provide pluggable serializer/deserializers for schemas.

Any clients such as Storm, Spark, Nifi, Kafka and other projects can easily use
the schema-registry-client to query schema registry to serialize/deserialize the messages.

A frequently asked question is, how Schema Registry from this project is different
from Confluent Schema Registry. While we don't have a clear roadmap of Confluent
Schema Registry, here are the current capabilities of both. You may want to look at future
roadmap.

+-----------------------------+-------------------------+---------------------------+
| Features                    | Registry                | Confluent Schema Registry |
+=============================+=========================+===========================+
| | REST API for              ||  Yes                   | Yes                       |
| | Schema Management         |                         |                           |
+-----------------------------+-------------------------+---------------------------+
| | Supported Schema Types    | | AVRO (0.3), PROTOBUF, | AVRO only                 |
|                             | | JSON, extensible...   |                           |
+-----------------------------+-------------------------+---------------------------+
| | Pluggable                 | | Yes                   | No, uses Avro parser      |
| | Serializer/Deserialize    |                         |                           |
+-----------------------------+-------------------------+---------------------------+
| | Storage                   | | Pluggable Storage     | Uses Kafka as storage     |
|                             | | (Mysql, Postgres)     |                           |
+-----------------------------+-------------------------+---------------------------+
| | Registry Client to support| | Registry client to    | No. Developed for Kafka   |
| | multi platform(Storm,Kafka| | interact with schema  | producer/consumer APIs    |
| | , Nifi etc..)             | | registry              | only                      |
+-----------------------------+-------------------------+---------------------------+
| | HA                        | | No Master,Multi       | Single Master Architecture|
|                             | | Webserver deployment  | Depends on zookeeper      |
+-----------------------------+-------------------------+---------------------------+
| | UI                        | | Yes                   | | No                      |
+-----------------------------+-------------------------+---------------------------+
| | Collaboration             | | In future- See Roadmap| ?                         |
|                             |                         |                           |
+-----------------------------+-------------------------+---------------------------+
| | Search                    | | Yes                   | No                        |
+-----------------------------+-------------------------+---------------------------+
