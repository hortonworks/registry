Feature: Importing and exporting schemas

  Background: Schema Registry is running
    Given that Schema Registry is running

  Scenario: Exporting schemas
    Given the schema meta "Car" exists with the following parameters:
      | Name            | Value          |
      | type            | avro           |
      | schemaGroup     | Kafka          |
      | compatibility   | BACKWARD       |
      | validationLevel | ALL            |
      | evolve          | true           |
      | description     | this is a test |
    And we create a new version for schema "Car" with the following schema:
    """
    {
      "type": "record",
      "namespace": "com.cloudera",
      "name": "Car",
      "fields": [{ "name": "model", "type":  "string" }]
    }
    """
    When we export all schemas
    Then we should see the "Car" schema with id 1 and version 1 on the "MASTER" branch in the export with the same schema text

  Scenario: importing schemas from a Confluent registry
    Given an item exported from a Confluent registry:
    """
    {"keytype":"SCHEMA","subject":"Hippopotamus","version":1,"magic":1}      {"subject":"Hippopotamus","version":1,"id":25,"schema":"{\"type\":\"record\",\"name\":\"Hippopotamus\",\"namespace\":\"com.cloudera\",\"fields\":[{\"name\":\"model\",\"type\":\"string\"}]}","deleted":false}
    {"keytype":"SCHEMA","subject":"Hippopotamus","version":2,"magic":1}      {"subject":"Hippopotamus","version":2,"id":35,"schema":"{\"type\":\"record\",\"name\":\"Hippopotamus\",\"namespace\":\"com.cloudera\",\"fields\":[{\"name\":\"model\",\"type\":\"string\"},{\"name\":\"color\",\"type\":\"string\",\"default\":\"blue\"},{\"name\":\"price\",\"type\":\"string\",\"default\":\"0\"}]}","deleted":false}
    """
    When we import that into the Schema Registry as a "confluent" format schema
    Then we should see 2 successfully imported versions in the response and 0 should have failed
    And there should be a version 1 with id 25 of the schema "Hippopotamus" on the "MASTER" branch
    And there should be a version 2 with id 35 of the schema "Hippopotamus" on the "MASTER" branch

  Scenario: importing schemas from a Confluent registry with fingerprints/texts matching
    Given an item exported from a Confluent registry:
    """
    {"keytype":"SCHEMA","subject":"Hippopotamus","version":1,"magic":1}      {"subject":"Hippopotamus","version":1,"id":25,"schema":"{\"type\":\"record\",\"name\":\"Hippopotamus\",\"namespace\":\"com.cloudera\",\"fields\":[{\"name\":\"model\",\"type\":\"string\"}]}","deleted":false}
    {"keytype":"SCHEMA","subject":"Hippopotamus","version":2,"magic":1}      {"subject":"Hippopotamus","version":2,"id":35,"schema":"{\"type\":\"record\",\"name\":\"Hippopotamus\",\"namespace\":\"com.cloudera\",\"fields\":[{\"name\":\"model\",\"type\":{\"type\":\"string\", \"avro.java.string\":\"String\"}}]}","deleted":false}
    {"keytype":"SCHEMA","subject":"Hippopotamus","version":3,"magic":1}      {"subject":"Hippopotamus","version":3,"id":42,"schema":"{\"type\":\"record\",\"name\":\"Hippopotamus\",\"namespace\":\"com.cloudera\",\"fields\":[{\"name\":\"model\",\"type\":{\"type\":\"string\", \"avro.java.string\":\"String\"}}]}","deleted":false}
    """
    When we import that into the Schema Registry as a "confluent" format schema
    Then we should see 3 successfully imported versions in the response and 0 should have failed
    And there should be a version 1 with id 25 of the schema "Hippopotamus" on the "MASTER" branch
    And there should be a version 2 with id 35 of the schema "Hippopotamus" on the "MASTER" branch
    And there should be a version 3 with id 42 of the schema "Hippopotamus" on the "MASTER" branch

  Scenario: importing new schemas
    Given an export containing the schema "Car" and version 1 on the "MASTER" branch and schema text:
    """
    {
      "type": "record",
      "namespace": "com.cloudera",
      "name": "Car",
      "fields": [{ "name": "model", "type":  "string" }]
    }
    """
    When we import that into the Schema Registry as a "cloudera" format schema
    Then we should see 1 successfully imported versions in the response and 0 should have failed
    And there should be a version 1 with id 1 of the schema "Car" on the "MASTER" branch in the Registry with the same schema text

  Scenario: importing new schemas with fingerprints/texts matching
    Given an export containing the schema "Car" and version 1 on the "MASTER" branch and schema text:
    """
    {
      "type": "record",
      "namespace": "com.cloudera",
      "name": "Car",
      "fields": [{ "name": "model", "type":  "string" }]
    }
    """
    And in the export there is a version 2 with id 2 on the "MASTER" branch with schema text:
    """
    {
      "type": "record",
      "namespace": "com.cloudera",
      "name": "Car",
      "fields": [{ "name": "model", "type":  { "type": "string", "avro.java.string": "String" }}]
    }
    """
    And in the export there is a version 3 with id 5 on the "MASTER" branch with schema text:
    """
    {
      "type": "record",
      "namespace": "com.cloudera",
      "name": "Car",
      "fields": [{ "name": "model", "type":  { "type": "string", "avro.java.string": "String" }}]
    }
    """
    When we import that into the Schema Registry as a "cloudera" format schema
    Then we should see 3 successfully imported versions in the response and 0 should have failed
    And there should be a version 1 with id 1 of the schema "Car" on the "MASTER" branch
    And there should be a version 2 with id 2 of the schema "Car" on the "MASTER" branch
    And there should be a version 3 with id 5 of the schema "Car" on the "MASTER" branch

  Scenario: importing new schemas with branches
    Given an export containing the schema "Car" and version 1 on the "MASTER" branch and schema text:
    """
    {
      "type": "record",
      "namespace": "com.cloudera",
      "name": "Car",
      "fields": [{ "name": "model", "type":  "string" }]
    }
    """
    And in the export there is a version 1 with id 1 on the "newbranch" branch forked from version 1 with the same schema text as the already existing one
    And in the export there is a version 2 with id 2 on the "newbranch" branch with schema text:
    """
    {
      "type": "record",
      "namespace": "com.cloudera",
      "name": "Car",
      "fields": [
                { "name": "model", "type":  "string" },
                { "name": "color", "type":  "string", "default": "blue" }
      ]
    }
    """
    When we import that into the Schema Registry as a "cloudera" format schema
    Then we should see 2 successfully imported versions in the response and 0 should have failed
    And there should be a version 1 with id 1 of the schema "Car" on the "MASTER" branch
    And there should be a version 1 with id 1 of the schema "Car" on the "newbranch" branch
    And there should be a version 2 with id 2 of the schema "Car" on the "newbranch" branch

  Scenario: importing new schemas with more branches
    Given an import file "complex-export.json"
    When we import that into the Schema Registry as a "cloudera" format schema
    Then we should see 5 successfully imported versions in the response and 0 should have failed
    And there should be a version 5 with id 10007 of the schema "Car" on the "MASTER" branch
    And there should be a version 3 with id 10005 of the schema "Car" on the "MASTER" branch
    And there should be a version 1 with id 10003 of the schema "Car" on the "MASTER" branch
    And there should be a version 2 with id 10004 of the schema "Car" on the "newfield" branch
    And there should be a version 1 with id 10003 of the schema "Car" on the "newfield" branch
    And there should be a version 4 with id 10006 of the schema "Car" on the "pricebranch" branch
    And there should be a version 3 with id 10005 of the schema "Car" on the "pricebranch" branch

  Scenario: importing new versions when there is a preexisting one
    Given the schema meta "Car" exists with the following parameters:
      | Name            | Value          |
      | type            | avro           |
      | schemaGroup     | Kafka          |
    And we create a new version for schema "Car" with the following schema:
    """
    {
      "type": "record",
      "namespace": "com.cloudera",
      "name": "Car",
      "fields": [{ "name": "model", "type":  "string" }]
    }
    """
    And assuming it was created with the id 1 and version 1
    And we got an export containing the schema "Car"
    And in the export there is a version 1 with id 1 on the "MASTER" branch with the same schema text as the already existing one
    And in the export there is a version 2 with id 2 on the "MASTER" branch with schema text:
    """
    {
      "type": "record",
      "namespace": "com.cloudera",
      "name": "Car",
      "fields": [
                { "name": "model", "type":  "string" },
                { "name": "color", "type":  "string", "default": "blue" }
      ]
    }
    """
    When we import that into the Schema Registry as a "cloudera" format schema
    Then we should see 1 successfully imported versions in the response and 0 should have failed

  Scenario: importing the same version with different schema text should fail
    Given the schema meta "Car" exists with the following parameters:
      | Name            | Value          |
      | type            | avro           |
      | schemaGroup     | Kafka          |
    And we create a new version for schema "Car" with the following schema:
    """
    {
      "type": "record",
      "namespace": "com.cloudera",
      "name": "Car",
      "fields": [{ "name": "model", "type":  "string" }]
    }
    """
    And assuming it was created with the id 1 and version 1
    And we got an export containing the schema "Car"
    And in the export there is a version 1 with id 1 on the "MASTER" branch with schema text:
    """
    {
      "type": "record",
      "namespace": "com.cloudera",
      "name": "Car",
      "fields": [
                { "name": "model", "type":  "string" },
                { "name": "color", "type":  "string", "default": "blue" }
      ]
    }
    """
    When we import that into the Schema Registry as a "cloudera" format schema
    Then we should see 0 successfully imported versions in the response and 1 should have failed showing the id 1 as failing

  Scenario: importing with a different schema type fails
    Given the schema meta "Car" exists with the following parameters:
      | Name            | Value          |
      | type            | json           |
      | schemaGroup     | Kafka          |
    And we create a new version for schema "Car" with the following schema:
    """
    {
      "description": "desc",
      "schemaText": "{\"type\": \"object\",\"additionalProperties\": false,\"title\": \"Car\",\"$schema\": \"http: //json-schema.org/draft-07/schema#\",\"properties\": {\"name\": {\"oneOf\": [{\"type\": \"null\",\"title\": \"Not included\"},{\"type\": \"string\"}]},\"version\": {\"oneOf\": [{\"type\": \"null\",\"title\": \"Not included\"},{\"type\": \"integer\"}]}}}"
    }
    """
    And we got an export containing an "avro" typed "Car" schema
    And in the export there is a version 1 with id 1 on the "MASTER" branch with schema text:
    """
    {
      "type": "record",
      "namespace": "com.cloudera",
      "name": "Car",
      "fields": [{ "name": "model", "type":  "string" }]
    }
    """
    When we import that into the Schema Registry as a "cloudera" format schema
    Then we should see 0 successfully imported versions in the response and 1 should have failed showing the id 1 as failing

  Scenario: when there's an existing meta with the same id but different name, import should fail
    Given the schema meta "Car" exists with the following parameters:
      | Name            | Value          |
      | type            | avro           |
      | schemaGroup     | Kafka          |
    And we create a new version for schema "Car" with the following schema:
    """
    {
      "type": "record",
      "namespace": "com.cloudera",
      "name": "Car",
      "fields": [{ "name": "model", "type":  "string" }]
    }
    """
    And assuming it was created with the id 1 and version 1
    And we got an export containing an "avro" typed "IncompatibleCar" schema with id 1
    But in the export there is a version 1 with id 2 on the "MASTER" branch with schema text:
    """
    {
      "type": "record",
      "namespace": "com.cloudera",
      "name": "IncompatibleCar",
      "fields": [
                { "name": "model2", "type":  "string" }
      ]
    }
    """
    When we import that into the Schema Registry as a "cloudera" format schema
    Then we should see 0 successfully imported versions in the response and 1 should have failed showing the id 2 as failing

    Scenario: there is an imported metadata in the offset range and another metadata is created that would get the same ID as the imported one
      Given we create a new schema meta "FullName" with the following parameters:
        | Name            | Value          |
        | type            | avro           |
        | schemaGroup     | Kafka          |
        | compatibility   | BACKWARD       |
        | validationLevel | ALL            |
        | evolve          | true           |
        | description     | this is a test |
      Given an import file "offset-import.json"
      And we import that into the Schema Registry as a "cloudera" format schema
      Given we create a new schema meta "FullerName" with the following parameters:
        | Name            | Value          |
        | type            | avro           |
        | schemaGroup     | Kafka          |
        | compatibility   | BACKWARD       |
        | validationLevel | ALL            |
        | evolve          | true           |
        | description     | description    |
      And we export all schemas
      Then we should see the "FullName" schema with id 1 in the export
      And we should see the "offset-import" schema with id 2 in the export
      And we should see the "FullerName" schema with id 3 in the export
      