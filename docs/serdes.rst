
Serialization/Deserialization protocol
======================================

Serializer and Deserializer are used to marshal and unmarshal messages according to a given schema by adding schema version
information either in the Message Header [or] along with the payload. There are different protocols and/or formats in
how schema version are added with the Message. This framework allows to build custom ser/des protocols by users.

Message Formats
```````````````

In Kafka v0.11.0.0, Record Header support is introduced. With this support, user can optionally save the schema
information in the header rather than prepending it in the body of the message. (See Message Format 2)

Message Format 1
""""""""""""""""
|header:| |body:<version_info><payload>|
This is the default format to maintain backward compatibility. Prepends the schema version information along with the
user payload in the body of the Message.

Message Format 2
""""""""""""""""
|header:<version_info>| |body:<payload>|
Adds the schema version information in the Message Header and keeps the user payload as it's in the body of the Message.

.. code-block:: java

    public byte[] serialize(String topic, Headers headers, Object data) {
        final MessageAndMetadata message = serializer.serialize(data, schemaMetadata);
        headers.add("value.schema.version.id", message.metadata());
        return message.payload();
    }


Confluent protocol
``````````````````
Protocol-id: 0

Serialization
"""""""""""""
version-info: version identifier as integer type, so it would be of 4 bytes.

payload:
  if schema type is byte array
    then write byte array as it is.
  else
    follow the below process to serialize the payload.

  if the given payload is of specific-record type
    then use specific datum writer along with binary encoder
  else
    use generic datum writer along with binary encoder.

Deserialization
"""""""""""""""
version-info: version identifier as integer type, so it would be of 4 bytes.

Get the respective avro schema for the given schema version id which will be writer schema.
User Deserializer API can take any schema version which can be treated as reader schema.

payload:
  if schema type is byte array
    then read byte array as it is.
  else
    follow the below process to deserialize the payload.

  if the given payload is of specific-record type
    then use specific datum writer
  else
    use generic datum writer along with binary encoder.

  if user api asks to read it as specific-record
    then use specific datum reader passing both writer and reader schemas
  else
    use generic datum reader passing both writer and reader schemas.

Java implementation is located at `serialization/deserialization  <https://github.com/hortonworks/registry/blob/master/schema-registry/serdes/src/main/java/com/hortonworks/registries/schemaregistry/serdes/avro/ConfluentAvroSerDesHandler.java>`_ and `protocol <https://github.com/hortonworks/registry/blob/master/schema-registry/serdes/src/main/java/com/hortonworks/registries/schemaregistry/serdes/avro/ConfluentProtocolHandler.java>`_.

Schema version id as long protocol
``````````````````````````````````
Protocol-id: 2

Serialization
"""""""""""""
version-info: long value which represents schema version id, viz 8 bytes

payload:
  if schema type is byte array
    then write byte array as it is.
  else
    follow the below process to serialize the payload.

  if the given payload is of specific-record type
    then use specific datum writer along with binary encoder.
  else
    use generic datum writer along with binary encoder.

Deserialization
"""""""""""""""
version-info: long value which represents schema version id, viz 8 bytes

Get the respective avro schema for the given schema version id which will be writer schema.
User Deserializer API can take any schema version which can be treated as reader schema.

payload:
  if schema type is byte array
    then read byte array as it is
  else if schema type is string
    then generate UTF-8 string from the remaining bytes
  else
    follow the below process to deserialize the payload

  if the given payload is of specific-record type
    then use specific datum writer along with binary encoder.
  else
    use generic datum writer along with binary encoder.

  if user api asks to read it as specific-record
    then use specific datum reader passing both writer and reader schemas.
  else
    use generic datum reader passing both writer and reader schemas.

Java implementation is located at `serialization/deserialization  <https://github.com/hortonworks/registry/blob/master/schema-registry/serdes/src/main/java/com/hortonworks/registries/schemaregistry/serdes/avro/DefaultAvroSerDesHandler.java>`_ and `protocol <https://github.com/hortonworks/registry/blob/master/schema-registry/serdes/src/main/java/com/hortonworks/registries/schemaregistry/serdes/avro/SchemaVersionIdAsLongProtocolHandler.java>`_.

Schema version id as int protocol
`````````````````````````````````
Protocol-id: 3

This protocol's serialization and deserialization of payload process is similar to Schema version id as long protocol except the schema version id is treated as int and it falls back to long when it is more than max integer value.

Java implementation is located at `serialization/deserialization  <https://github.com/hortonworks/registry/blob/master/schema-registry/serdes/src/main/java/com/hortonworks/registries/schemaregistry/serdes/avro/DefaultAvroSerDesHandler.java>`_ and `protocol <https://github.com/hortonworks/registry/blob/master/schema-registry/serdes/src/main/java/com/hortonworks/registries/schemaregistry/serdes/avro/SchemaVersionIdAsIntProtocolHandler.java>`_.


Schema metadata id and version protocol
```````````````````````````````````````
Protocol-id: 1

This protocol's serialization and deserialization of payload process is similar to Schema version id as long protocol except the version info contains both schema metadata od and version number.

version-info: <metadata-id><version>

metadata-id: long value which represents schema metadata id, viz 8 bytes

version: int value of version, viz 4 bytes

Java implementation is located at `serialization/deserialization  <https://github.com/hortonworks/registry/blob/master/schema-registry/serdes/src/main/java/com/hortonworks/registries/schemaregistry/serdes/avro/DefaultAvroSerDesHandler.java>`_ and `protocol <https://github.com/hortonworks/registry/blob/master/schema-registry/serdes/src/main/java/com/hortonworks/registries/schemaregistry/serdes/avro/SchemaMetadataIdProtocolHandler.java>`_.
