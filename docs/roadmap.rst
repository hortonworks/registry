
Roadmap
=======


We are working on making Registry a true Central Metadata Repository that
can serve Schemas, ML Docs, Service Discovery and Application Configs.
Here is our roadmap that we are working for next versions. As always, as the
community needs evolve, this list may be reprioritized and changes may be made.
We greatly appreciate the community feedback on this roadmap.


Schema Registry
---------------


Schema Improvements
~~~~~~~~~~~~~~~~~~~

1. Rich Data types
  With Avro and other formats, Users get standard types including primitives and in Avro's
  case Record etc. With Rich Data types we are looking to add Rich-type decorator document on top
  of Schema. This Decorator doc will define how to semantically interpret the schema fields.
  Rich-types can be both system types and user-defined types and come from a hierarchical
  taxonomy such as String -> Date -> Specific Date format.

  For example if you have Latitude, Longitude fields in the schema, decorator doc can have
  a field saying Location = [Latitude, Longitude], similarly if the schema has date field,
  decorator can have a field saying Date = ["DD/MM/YYYY"]. This will give
  much better integration of schemas in application and a important context around each field.
  The context can be used for data discovery, and also to maintain tools related to
  such data types such as validation tools, conversion etc.


Collaboration
~~~~~~~~~~~~~

1. Notifications
  Allow users to subscribe to a schema of interest for any version changes.
  The changes to a schema will be updated via Email.

2. Schema Review & Management
  Currently, we allow users to programmatically register a schema and it will be
  immediately visible to all the clients. With this option, we will allow schema
  to be staged & reviewed. Once approved will be available to production users.

3. Audit Log
  Record all the usages of a schema and add Heartbeat mechanism on client side.
  This will allow us to show case all the clients that are using a particular Schema.
  With this option, users will be able to see how many clients are currently using the schema
  and which version they depend upon.

4. Schema Life Cycle Management
  Schemas are immutable, once created they can be extended but cannot be deleted.
  But there are often cases where the clients are not any more using schemas and having
  archiving option will allow users to keep the schema not available and still be in the
  system, if in case they need to refer or put back in production.

5. Improved UI
  We will continue to improve and provide a better collaboration and management
  of Schemas through UI. This helps in authoring, discovering, monitoring and managing
  the schemas.

Security
~~~~~~~~

1. Authentication
  As part of 0.3 release , we added SPNEGO auth to authenticate users. As part of
  authentication improvements we will add SSL and OAUTH 2.0 .

2. Authorization
  Implement Schema & Sub-Schema level authorization. This will allow users to
  fine-tune the control of Schema on who is allowed to access/edit etc..


Integration
~~~~~~~~~~~

1. Multi-lang Client support for Registry
 SchemaRegistryClient can only be used in Java based applications. We are working
 on adding support of Python and in future extend that to other languages.
2. Pluggable Listeners
 Allows users to write a simple plugin to fetch metadata from other stores
 such as Hive Metastore, Confluent Schema Registry etc..
3. Converters
 SchemaRegistry uses Avro as the format with an option of extending it to other formats
 such as Protobuf etc. . With converters we provide an option for users to convert
 their csv, xml, json to Registry format such as Avro, Protobuf etc..


Operations
~~~~~~~~~~

1. Cross-Colo Mirroring
  Mirroring will allow users to have a independent registry cluster per data center
  and allow syncing of schemas. This will allow users to keep the registry servers
  closer to their clients and keep the schemas in-sync across the clusters.
