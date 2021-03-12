Migration from Confluent Schema Registry
----------------------------------------

We support a compatibility and a migration path from Confluent Schema Registry.

=========
Migration
=========

The following migration path is suggested.

1. Sync / Mirror Schemas

  - First to do is to sync/mirror the existing schema's and id's from Confluent's Schema Registry.
  - It is important the schema id, is the same, as it will need to reference the same schema even once migrated as data will exist in kafka.

We provide a REST API for migrating schemas from Confluent's schema registry to our own. **Please note** that this only works for Avro schemas, because other formats are not yet supported in this Schema Registry.

To migrate using the REST API, please follow these steps:
    1. Confluent SR keeps its schemas in a Kafka topic. You will need to dump this topic to a text file.

      .. code-block:: bash

        kafka-console-consumer --from-beginning --topic _schemas \
            --formatter kafka.tools.DefaultMessageFormatter \
            --property print.key=true --property print.value=true \
            --bootstrap-server `hostname`:9092

    2. The dump file will look something like this. If there are any additional lines, please remove them in order to not cause unexpected issues.

      ::

        {"keytype":"SCHEMA","subject":"Kafka-key","version":1,"magic":1}        {"subject":"Kafka-key","version":1,"id":1,"schema":"\"string\"","deleted":false}
        {"keytype":"CONFIG","subject":"MySchema","magic":0}  {"compatibilityLevel":"FULL"}

    3. Now you need to upload this file into Cloudera's Schema Registry. The service needs to be running. It is recommended that you provide an empty database, to avoid conflicts.
    4. You may use Schema Registry's Swagger API to import the dump file, or you can use `curl`

      .. code-block:: bash

        curl -u : --negotiate --form file='@schemadump.json' http://`hostname`:7788/api/v1/schemaregistry/import?format=1&failOnError=true

    5. If everything was successful, you should get a response like this:

      ::

        {"successCount":45,"failedCount":0,"failedIds":[]}

    6. In case of issues, please take a look at the error message. The most likely cause of errors is that the dump file was not properly sanitized. Please remove any empty lines and anything which doesn't contain valid information about the schema.


2. Redirect Confluent Serdes.

  - Registry provides a Confluent compatible API for the Confluent Serdes.
  - As such simply re-directing the url path to Registry for existing applications and services will migrate these. You can do that by enabling a filter in registry configuration like below.
    ::

      servletFilters:
        - className: "com.hortonworks.registries.schemaregistry.webservice.RewriteUriFilter"
          params:
            # value format is [<targetpath>,<paths-should-be-redirected-to>,*|]*
            # below /subjects and /schemas/ids are forwarded to /api/v1/confluent
            forwardPaths: "/api/v1/confluent,/subjects/*,/schemas/ids/*"

3. Serdes Change.

  - Next which will be a slower process will be to change code bases to use the Registry Serdes instead of Confluents.
    
  - For Producer wire compatibility where you still have consumers using confluent serdes, then set the "protocol.version" to 0x0.
    
  - For Consumer wire compatibility the consumer will auto-detect the confluent protocol vs the registry and auto handle this.
    
  - Once all consumers are migrated on a flow you can start using the registry protocol.


==================
Third Party Tools.
================== 

We are aware there are a lot of third party tools currently supporting Confluent's protocol.

As such the compatibility both at the Serdes REST API level and the wire protocol will be supported until this situation improves somewhat.
In cases where these flows exist, you will simply need to:

- ensure as mentioned above Producers set the wire protocol to 0x0
- configure the third party to point at Registy's confluent compatible api.
