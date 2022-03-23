Feature: Schema Registry Client

  Scenario: We set the default compatibility to FORWARD

    Given that Schema Registry is running with default avro compatibility set to "FORWARD"
    When we create a new schema meta "Car" with the following parameters:
    | Name            | Value          |
    | type            | avro           |
    | schemaGroup     | Kafka          |
    | validationLevel | ALL            |
    | evolve          | true           |
    | description     | this is a test |
    Then the schema is successfully created

    When we create a new version for schema "Car" with the following schema:
    """
    {
      "type": "record",
      "namespace": "com.cloudera",
      "name": "Car",
      "fields": [
                { "name": "model", "type":  "string" },
                { "name": "color", "type":  "string", "default": "blue" },
                { "name": "price", "type":  "string", "default": "0" }
      ]
    }
    """
    Then the version is successfully created

    When we send an HTTP GET request to "/api/v1/schemaregistry/schemas" with the following query parameters:
      | Name           | Value       |
      | name           | Car         |
      | _orderByFields | timestamp,d |
    Then the response code is 200
    And the response will contain 1 entity
    And the response metadata will have the following properties:
      | Name            | Value          |
      | name            | Car            |
      | compatibility   | FORWARD        |
      | description     | this is a test |
      | type            | avro           |
      | schemaGroup     | Kafka          |
      | validationLevel | ALL            |


  Scenario: We explicitly set the compatibility of the schema

    Given that Schema Registry is running with default avro compatibility set to "FORWARD"
    When we create a new schema meta "Car2" with the following parameters:
      | Name            | Value          |
      | type            | avro           |
      | schemaGroup     | Kafka          |
      | compatibility   | BACKWARD       |
      | validationLevel | ALL            |
      | evolve          | true           |
      | description     | this is a test |
    Then the schema is successfully created

    When we create a new version for schema "Car2" with the following schema:
    """
    {
      "type": "record",
      "namespace": "com.cloudera",
      "name": "Car2",
      "fields": [
                { "name": "model", "type":  "string" },
                { "name": "color", "type":  "string", "default": "blue" },
                { "name": "price", "type":  "string", "default": "0" }
      ]
    }
    """
    Then the version is successfully created

    When we send an HTTP GET request to "/api/v1/schemaregistry/schemas" with the following query parameters:
      | Name           | Value       |
      | name           | Car2        |
      | _orderByFields | timestamp,d |
    Then the response code is 200
    And the response will contain 1 entity
    And the response metadata will have the following properties:
      | Name            | Value          |
      | name            | Car2           |
      | compatibility   | BACKWARD       |
      | description     | this is a test |
      | type            | avro           |
      | schemaGroup     | Kafka          |
      | validationLevel | ALL            |


  Scenario: We submit a schema through Confluent's API

    Given that Schema Registry is running with default avro compatibility set to "FORWARD"
    When we send an HTTP POST request to "/api/v1/confluent/subjects/Truck/versions" with the following payload:
    """
      {"schema": "{\"type\": \"record\",\"namespace\": \"com.piripocs\",\"name\": \"Truck\",\"fields\": [{ \"name\": \"model\", \"type\":  \"string\" },{ \"name\": \"color\", \"type\":  \"string\", \"default\": \"blue\" },{ \"name\": \"price\", \"type\":  \"string\", \"default\": \"0\" },{ \"name\": \"year\", \"type\": [\"null\", \"string\"], \"default\": null }]}"}
    """
    Then the response code is 200

    When we send an HTTP GET request to "/api/v1/schemaregistry/schemas" with the following query parameters:
      | Name           | Value       |
      | name           | Truck       |
      | _orderByFields | timestamp,d |
    Then the response code is 200
    And the response will contain 1 entity
    And the response metadata will have the following properties:
      | Name            | Value          |
      | name            | Truck          |
      | compatibility   | FORWARD        |
      | type            | avro           |
      | schemaGroup     | Kafka          |
