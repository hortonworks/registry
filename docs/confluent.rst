Migration from Confluent Schema Registry
----------------------------------------

We support a comparability and a migration path from Confluent Schema Registry.

=========
Migration
=========

The following migration path is suggested.

1. Sync / Mirror Schemas

  - First to do is to sync/mirror the existing shema's and id's from confluent's schema registry.
  - It is important the schema id, is the same, as it will need to refernece the same schema even once migrated as data will exist in kafka.


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

We are aware there are alot of third party tools only supporting currently confluent's protocol.

As such the compatibility both at the Sedes REST API level and the wire protocol will be supported until this situation improves somewhat.
In cases where these flows exist, you will simply need to:

- ensure as mentioned above Producers set the wire protocol to 0x0
- configure the third party to point at Registy's confluent compatible api.
