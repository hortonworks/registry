Feature: Schema Registry Client

  Background: Schema Registry is running
    Given that Schema Registry is running

  Scenario: Create a new schema
    When we send an HTTP POST request to "/api/v1/schemaregistry/schemas" with the following parameters:
      | Name            | Value    |
      | name            | Car      |
      | schemaGroup     | Kafka    |
      | type            | avro     |
      | description     | desc     |
      | compatibility   | BACKWARD |
      | validationLevel | ALL      |
    Then the response code is 201

    When we send an HTTP GET request to "/api/v1/schemaregistry/schemas" with the following query parameters:
      | Name           | Value       |
      | name           | Car         |
      | _orderByFields | timestamp,d |
    Then the response code is 200
    And the response will contain 1 entity

    When we send an HTTP POST request to "/api/v1/schemaregistry/schemas/Car/versions?branch=MASTER&disableCanonicalCheck=false" with the following payload:
    """
    {
      "description": "desc",
      "schemaText": "{\"type\": \"record\",  \"namespace\": \"com.cloudera\", \"name\": \"Car\",  \"fields\": [ { \"name\": \"model\", \"type\":  \"string\" },  { \"name\": \"color\", \"type\":  \"string\", \"default\": \"blue\" } ] }"
    }
    """
    Then the response code is 201

    When we send an HTTP POST request to "/api/v1/schemaregistry/schemas/Car/versions?branch=MASTER&disableCanonicalCheck=false" with the following payload:
    """
    {
      "description": "desc",
      "schemaText": "{\"type\": \"record\",  \"namespace\": \"com.cloudera\", \"name\": \"Car\",  \"fields\": [ { \"name\": \"model\", \"type\":  \"string\" },  { \"name\": \"color\", \"type\":  \"string\", \"default\": \"blue\" },  { \"name\": \"year\", \"type\":  \"int\", \"default\": 1999 } ] }"
    }
    """
    Then the response code is 201

    When we send an HTTP GET request to "/api/v1/schemaregistry/schemas" with the following query parameters:
      | Name           | Value       |
      | name           | Car         |
      | _orderByFields | timestamp,d |
    Then the response code is 200
    And the response will contain 1 entity

    When we send an HTTP GET request to "/api/v1/schemaregistry/schemas/Car/versions?branch=MASTER"
    Then the response code is 200
    And the response will contain 2 entities