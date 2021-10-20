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

  Scenario: importing schemas from a Confluent registry should produce mismatching ids until fix for CDPD-31003 is merged
    Given an item exported from a Confluent registry:
    """
    {"keytype":"SCHEMA","subject":"Car","version":1,"magic":1}      {"subject":"Car","version":1,"id":2,"schema":"{\"type\":\"record\",\"name\":\"Car\",\"namespace\":\"com.cloudera\",\"fields\":[{\"name\":\"model\",\"type\":\"string\"}]}","deleted":false}
    """
    When we import that into the Schema Registry as a "confluent" format schema
    Then we should see 1 successfully imported versions in the response and 0 should have failed
    #And there should be a version with id 2 of the schema "Car" on the "MASTER" branch in the Registry with the same schema text

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
    And there should be a version with id 1 of the schema "Car" on the "MASTER" branch in the Registry with the same schema text

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
    And there should be a version with id 1 of the schema "Car" on the "MASTER" branch
    And there should be a version with id 1 of the schema "Car" on the "newbranch" branch
    And there should be a version with id 2 of the schema "Car" on the "newbranch" branch

  Scenario: importing new schemas with more branches
    Given an import file "complex-export.json"
    When we import that into the Schema Registry as a "cloudera" format schema
    Then we should see 5 successfully imported versions in the response and 0 should have failed
    And there should be a version with id 10007 of the schema "Car" on the "MASTER" branch
    And there should be a version with id 10005 of the schema "Car" on the "MASTER" branch
    And there should be a version with id 10003 of the schema "Car" on the "MASTER" branch
    And there should be a version with id 10004 of the schema "Car" on the "newfield" branch
    And there should be a version with id 10003 of the schema "Car" on the "newfield" branch
    And there should be a version with id 10006 of the schema "Car" on the "pricebranch" branch
    And there should be a version with id 10005 of the schema "Car" on the "pricebranch" branch

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
